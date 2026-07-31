use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde_json::{Value, json};

use super::relay::{WorkerRelayClient, WorkerRelayError};

#[derive(Debug, thiserror::Error)]
pub enum LoadoutError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
}

#[derive(Clone)]
pub struct LoadoutTracker {
    database: Database,
    relay: WorkerRelayClient,
}

impl LoadoutTracker {
    pub fn new(database: Database, config: &BackendConfig) -> Result<Self, WorkerRelayError> {
        Ok(Self {
            database,
            relay: WorkerRelayClient::new(config)?,
        })
    }

    pub async fn fetch_player_loadouts(&self, player_id: i64) -> Result<Vec<Value>, LoadoutError> {
        let value = self
            .relay
            .call_value(
                "getPlayerLoadouts",
                vec![json!(player_id)],
                "loadout_tracker",
            )
            .await?;
        let rows = value.as_array().cloned().unwrap_or_default();
        if !rows.is_empty() {
            self.relay
                .call_value(
                    "dumpRawPayloads",
                    vec![json!([{
                        "endpoint":"getplayerloadouts",
                        "entity_type":"loadout",
                        "entity_id":player_id,
                        "raw_data":rows,
                    }])],
                    "loadout_tracker",
                )
                .await?;
        }
        Ok(rows)
    }

    pub async fn compute_card_win_rates(&self, player_id: i64) -> Result<i32, DatabaseError> {
        let row = self.database.one_json(
            "WITH aggregate AS(\
               SELECT mp.player_id,mp.champion_id,mpc.card_id,mpc.card_level,count(*)::INT times_used,\
               count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::INT wins,\
               count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::INT losses \
               FROM match_player_cards mpc JOIN match_players mp ON mp.match_id=mpc.match_id AND mp.player_id=mpc.player_id \
               JOIN matches m ON m.match_id=mpc.match_id AND m.entry_datetime=mp.entry_datetime \
               WHERE mp.player_id=$1 AND m.queue_id=486 GROUP BY mp.player_id,mp.champion_id,mpc.card_id,mpc.card_level\
             ),upserted AS(\
               INSERT INTO player_loadout_cards(player_id,champion_id,card_id,card_level,times_used,wins,losses,win_rate,updated_at)\
               SELECT player_id,champion_id,card_id,card_level,times_used,wins,losses,\
                 COALESCE(round(wins::NUMERIC/NULLIF(wins+losses,0)*100,2),0),now() FROM aggregate \
               ON CONFLICT(player_id,champion_id,card_id) DO UPDATE SET card_level=EXCLUDED.card_level,times_used=EXCLUDED.times_used,\
                 wins=EXCLUDED.wins,losses=EXCLUDED.losses,win_rate=EXCLUDED.win_rate,updated_at=now() RETURNING card_id\
             ) SELECT count(*)::INT AS count FROM upserted",
            &[&player_id],
        ).await?;
        Ok(row
            .as_ref()
            .and_then(|row| integer(row, "count"))
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default())
    }

    pub async fn top_builds(
        &self,
        player_id: i64,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<Value>, DatabaseError> {
        self.database
            .query_json(
                "SELECT * FROM player_loadout_cards WHERE player_id=$1 AND champion_id=$2 \
             ORDER BY win_rate DESC,times_used DESC LIMIT $3",
                &[&player_id, &champion_id, &limit],
            )
            .await
    }

    pub async fn recompute_all_card_win_rates(&self) -> Result<i32, DatabaseError> {
        let players = self.database.query_json(
            "SELECT DISTINCT mpc.player_id FROM match_player_cards mpc \
             JOIN match_players mp ON mp.match_id=mpc.match_id AND mp.player_id=mpc.player_id \
             JOIN matches m ON m.match_id=mpc.match_id AND m.entry_datetime=mp.entry_datetime WHERE m.queue_id=486",
            &[],
        ).await?;
        let mut total = 0_i32;
        for player in players {
            if let Some(id) = integer(&player, "player_id") {
                total = total.saturating_add(self.compute_card_win_rates(id).await?);
            }
        }
        Ok(total)
    }
}

fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
