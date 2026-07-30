use paladinscat_core::database::{Database, DatabaseError};
use serde_json::Value;

use super::match_lifecycle::MatchPopulation;

const NONRANKED_CORE_STATS_STAGE: &str = "nonranked_core_stats";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NonrankedScope {
    Casual,
    Bot,
    TeamDeathmatch,
    Arcade,
    WaveDefense,
    Experiment,
    Newcomer,
    Custom,
    Other,
}

impl NonrankedScope {
    pub fn as_database(self) -> &'static str {
        match self {
            Self::Casual => "casual",
            Self::Bot => "bot",
            Self::TeamDeathmatch => "team_deathmatch",
            Self::Arcade => "arcade",
            Self::WaveDefense => "wave_defense",
            Self::Experiment => "experiment",
            Self::Newcomer => "newcomer",
            Self::Custom => "custom",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonrankedItemFact {
    pub roster_slot: i16,
    pub player_id: i64,
    pub slot: i16,
    pub item_id: i32,
    pub item_level: i16,
    pub item_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonrankedTalentFact {
    pub roster_slot: i16,
    pub player_id: i64,
    pub champion_id: i32,
    pub talent_id: i32,
    pub talent_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonrankedCardFact {
    pub roster_slot: i16,
    pub player_id: i64,
    pub champion_id: i32,
    pub talent_id: i32,
    pub card_id: i32,
    pub card_level: i16,
    pub card_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonrankedMechanicsFacts {
    pub match_id: i64,
    pub queue_id: i32,
    pub population: MatchPopulation,
    pub scope: NonrankedScope,
    pub items: Vec<NonrankedItemFact>,
    pub talents: Vec<NonrankedTalentFact>,
    pub cards: Vec<NonrankedCardFact>,
}

#[derive(Debug, thiserror::Error)]
pub enum NonrankedMechanicsError {
    #[error("ranked match {match_id} cannot enter non-ranked mechanics facts")]
    RankedPopulation { match_id: i64 },
    #[error("match {match_id} has no classified non-ranked population")]
    UnknownPopulation { match_id: i64 },
    #[error("match {match_id} is classified as {actual}; refusing non-ranked write as {requested}")]
    PopulationMismatch {
        match_id: i64,
        actual: String,
        requested: &'static str,
    },
    #[error("match {match_id} population {population} cannot use non-ranked scope {scope}")]
    ScopeMismatch {
        match_id: i64,
        population: &'static str,
        scope: &'static str,
    },
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

impl From<tokio_postgres::Error> for NonrankedMechanicsError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Database(DatabaseError::Query(error))
    }
}

impl NonrankedMechanicsFacts {
    pub fn from_players(
        match_id: i64,
        queue_id: i32,
        population: MatchPopulation,
        scope: NonrankedScope,
        players: &[Value],
    ) -> Result<Self, NonrankedMechanicsError> {
        match population {
            MatchPopulation::Ranked => {
                return Err(NonrankedMechanicsError::RankedPopulation { match_id });
            }
            MatchPopulation::Unknown => {
                return Err(NonrankedMechanicsError::UnknownPopulation { match_id });
            }
            MatchPopulation::Casual | MatchPopulation::Special => {}
        }
        if matches!(
            (population, scope),
            (MatchPopulation::Casual, value) if value != NonrankedScope::Casual
        ) || matches!(
            (population, scope),
            (MatchPopulation::Special, NonrankedScope::Casual)
        ) {
            return Err(NonrankedMechanicsError::ScopeMismatch {
                match_id,
                population: population.as_database(),
                scope: scope.as_database(),
            });
        }

        let mut facts = Self {
            match_id,
            queue_id,
            population,
            scope,
            items: Vec::new(),
            talents: Vec::new(),
            cards: Vec::new(),
        };
        for (index, player) in players.iter().enumerate() {
            let roster_slot = i16::try_from(index + 1).unwrap_or(i16::MAX);
            let player_id = number(player, &["player_id", "playerId", "PlayerId"]);
            let champion_id = integer(player, &["champion_id", "ChampionId"]);
            let talent_id = integer(player, &["item_id_6", "ItemId6"]);

            for slot in 1..=4 {
                let item_id = integer(
                    player,
                    &[&format!("active_id_{slot}"), &format!("ActiveId{slot}")],
                );
                if item_id <= 0 {
                    continue;
                }
                let raw_level = integer(
                    player,
                    &[
                        &format!("active_level_{slot}"),
                        &format!("ActiveLevel{slot}"),
                    ],
                );
                facts.items.push(NonrankedItemFact {
                    roster_slot,
                    player_id,
                    slot: i16::try_from(slot).expect("item slot"),
                    item_id,
                    item_level: normalize_active_level(raw_level),
                    item_name: text(
                        player,
                        &[
                            &format!("item_active_{slot}"),
                            &format!("Item_Active_{slot}"),
                        ],
                    ),
                });
            }

            if champion_id > 0 && talent_id > 0 {
                facts.talents.push(NonrankedTalentFact {
                    roster_slot,
                    player_id,
                    champion_id,
                    talent_id,
                    talent_name: text(player, &["item_purch_6", "Item_Purch_6"]),
                });
            }

            for slot in 1..=5 {
                let card_id = integer(
                    player,
                    &[&format!("item_id_{slot}"), &format!("ItemId{slot}")],
                );
                if champion_id <= 0 || card_id <= 0 {
                    continue;
                }
                facts.cards.push(NonrankedCardFact {
                    roster_slot,
                    player_id,
                    champion_id,
                    talent_id: talent_id.max(0),
                    card_id,
                    card_level: integer(
                        player,
                        &[&format!("item_level_{slot}"), &format!("ItemLevel{slot}")],
                    )
                    .clamp(0, 5) as i16,
                    card_name: text(
                        player,
                        &[&format!("item_purch_{slot}"), &format!("Item_Purch_{slot}")],
                    ),
                });
            }
        }
        Ok(facts)
    }
}

fn normalize_active_level(level: i32) -> i16 {
    let normalized = if level > 2 { level / 4 } else { level };
    normalized.clamp(0, 3) as i16
}

fn integer(value: &Value, keys: &[&str]) -> i32 {
    keys.iter()
        .find_map(|key| {
            let value = value.get(*key)?;
            value
                .as_i64()
                .and_then(|number| i32::try_from(number).ok())
                .or_else(|| value.as_u64().and_then(|number| i32::try_from(number).ok()))
                .or_else(|| value.as_str().and_then(|text| text.parse::<i32>().ok()))
        })
        .unwrap_or_default()
}

fn number(value: &Value, keys: &[&str]) -> i64 {
    keys.iter()
        .find_map(|key| {
            let value = value.get(*key)?;
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
                .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        })
        .unwrap_or_default()
}

fn text(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
        .trim()
        .to_owned()
}

#[derive(Clone)]
pub struct NonrankedMechanicsRepository {
    database: Database,
}

impl NonrankedMechanicsRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn replace_match_facts(
        &self,
        facts: &NonrankedMechanicsFacts,
        owner: &str,
    ) -> Result<bool, NonrankedMechanicsError> {
        match facts.population {
            MatchPopulation::Ranked => {
                return Err(NonrankedMechanicsError::RankedPopulation {
                    match_id: facts.match_id,
                });
            }
            MatchPopulation::Unknown => {
                return Err(NonrankedMechanicsError::UnknownPopulation {
                    match_id: facts.match_id,
                });
            }
            MatchPopulation::Casual | MatchPopulation::Special => {}
        }

        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let status = transaction
            .query_opt(
                r#"
                SELECT population
                FROM match_ingest_status
                WHERE match_id = $1
                  AND lease_owner = $2
                  AND lease_until > now()
                FOR UPDATE
                "#,
                &[&facts.match_id, &owner],
            )
            .await?;
        let Some(status) = status else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let actual_population = status.get::<_, String>("population");
        if actual_population != facts.population.as_database() {
            return Err(NonrankedMechanicsError::PopulationMismatch {
                match_id: facts.match_id,
                actual: actual_population,
                requested: facts.population.as_database(),
            });
        }
        for table in [
            "nonranked_match_items",
            "nonranked_match_talents",
            "nonranked_match_cards",
        ] {
            transaction
                .execute(
                    &format!("DELETE FROM {table} WHERE match_id = $1"),
                    &[&facts.match_id],
                )
                .await?;
        }

        for item in &facts.items {
            transaction
                .execute(
                    r#"
                    INSERT INTO items (item_id, item_name)
                    VALUES ($1, $2)
                    ON CONFLICT (item_id) DO UPDATE SET
                      item_name = CASE
                        WHEN COALESCE(items.item_name, '') = ''
                          OR items.item_name LIKE 'Item %'
                        THEN EXCLUDED.item_name
                        ELSE items.item_name
                      END
                    "#,
                    &[
                        &item.item_id,
                        &reference_name(&item.item_name, "Item", item.item_id),
                    ],
                )
                .await?;
            transaction
                .execute(
                    r#"
                    INSERT INTO nonranked_match_items (
                      match_id, population, stats_scope, queue_id, roster_slot,
                      player_id, slot, item_id, item_level
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    "#,
                    &[
                        &facts.match_id,
                        &facts.population.as_database(),
                        &facts.scope.as_database(),
                        &facts.queue_id,
                        &item.roster_slot,
                        &item.player_id,
                        &item.slot,
                        &item.item_id,
                        &item.item_level,
                    ],
                )
                .await?;
        }

        for talent in &facts.talents {
            transaction
                .execute(
                    r#"
                    INSERT INTO talents (talent_id, talent_name, champion_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (talent_id) DO UPDATE SET
                      talent_name = CASE
                        WHEN talents.talent_name LIKE 'Talent %'
                        THEN EXCLUDED.talent_name
                        ELSE talents.talent_name
                      END,
                      champion_id = COALESCE(talents.champion_id, EXCLUDED.champion_id)
                    "#,
                    &[
                        &talent.talent_id,
                        &reference_name(&talent.talent_name, "Talent", talent.talent_id),
                        &talent.champion_id,
                    ],
                )
                .await?;
            transaction
                .execute(
                    r#"
                    INSERT INTO nonranked_match_talents (
                      match_id, population, stats_scope, queue_id, roster_slot,
                      player_id, champion_id, talent_id
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    "#,
                    &[
                        &facts.match_id,
                        &facts.population.as_database(),
                        &facts.scope.as_database(),
                        &facts.queue_id,
                        &talent.roster_slot,
                        &talent.player_id,
                        &talent.champion_id,
                        &talent.talent_id,
                    ],
                )
                .await?;
        }

        for card in &facts.cards {
            transaction
                .execute(
                    r#"
                    INSERT INTO cards (card_id, card_name, champion_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (card_id) DO UPDATE SET
                      card_name = CASE
                        WHEN COALESCE(cards.card_name, '') = ''
                          OR cards.card_name LIKE 'Card %'
                        THEN EXCLUDED.card_name
                        ELSE cards.card_name
                      END,
                      champion_id = COALESCE(cards.champion_id, EXCLUDED.champion_id)
                    "#,
                    &[
                        &card.card_id,
                        &reference_name(&card.card_name, "Card", card.card_id),
                        &card.champion_id,
                    ],
                )
                .await?;
            transaction
                .execute(
                    r#"
                    INSERT INTO nonranked_match_cards (
                      match_id, population, stats_scope, queue_id, roster_slot,
                      player_id, champion_id, talent_id, card_id, card_level
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    "#,
                    &[
                        &facts.match_id,
                        &facts.population.as_database(),
                        &facts.scope.as_database(),
                        &facts.queue_id,
                        &card.roster_slot,
                        &card.player_id,
                        &card.champion_id,
                        &card.talent_id,
                        &card.card_id,
                        &card.card_level,
                    ],
                )
                .await?;
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn project_match(
        &self,
        match_id: i64,
        population: MatchPopulation,
        owner: &str,
    ) -> Result<bool, NonrankedMechanicsError> {
        let (match_table, player_table, scope_expression, stage) = match population {
            MatchPopulation::Ranked => {
                return Err(NonrankedMechanicsError::RankedPopulation { match_id });
            }
            MatchPopulation::Unknown => {
                return Err(NonrankedMechanicsError::UnknownPopulation { match_id });
            }
            MatchPopulation::Casual => (
                "casual_matches",
                "casual_match_players",
                "'casual'::varchar",
                "casual_mechanics_stats",
            ),
            MatchPopulation::Special => (
                "special_matches",
                "special_match_players",
                "m.stats_scope",
                "special_mechanics_stats",
            ),
        };

        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let status = transaction
            .query_opt(
                r#"
                SELECT completed_stages, population
                FROM match_ingest_status
                WHERE match_id = $1
                  AND lease_owner = $2
                  AND lease_until > now()
                FOR UPDATE
                "#,
                &[&match_id, &owner],
            )
            .await?;
        let Some(status) = status else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let actual_population = status.get::<_, String>("population");
        if actual_population != population.as_database() {
            return Err(NonrankedMechanicsError::PopulationMismatch {
                match_id,
                actual: actual_population,
                requested: population.as_database(),
            });
        }
        let completed_stages = status.get::<_, Vec<String>>("completed_stages");
        if completed_stages.iter().any(|completed| completed == stage) {
            transaction.rollback().await?;
            return Ok(false);
        }
        let core_stats_complete = completed_stages
            .iter()
            .any(|completed| completed == NONRANKED_CORE_STATS_STAGE);

        let context = format!(
            r#"
            SELECT
              m.match_id,
              m.entry_datetime::date AS stats_date,
              {scope_expression} AS stats_scope,
              m.queue_id,
              m.lobby_tier,
              COALESCE(NULLIF(m.region, ''), 'Unknown') AS region,
              COALESCE(NULLIF(m.map, ''), 'Unknown') AS map,
              m.duration_seconds
            FROM {match_table} m
            WHERE m.match_id = $1
              AND m.stats_eligible
            "#
        );

        if !core_stats_complete {
            transaction
                .execute(
                    &format!(
                        r#"
                    WITH context AS ({context})
                    INSERT INTO nonranked_map_stats_daily (
                      stats_date, stats_scope, queue_id, region, map,
                      matches, duration_sum
                    )
                    SELECT
                      stats_date, stats_scope, queue_id, region, map,
                      1, duration_seconds
                    FROM context
                    ON CONFLICT (
                      stats_date, stats_scope, queue_id, region, map
                    ) DO UPDATE SET
                      matches = nonranked_map_stats_daily.matches + 1,
                      duration_sum = nonranked_map_stats_daily.duration_sum
                        + EXCLUDED.duration_sum,
                      updated_at = now()
                    "#
                    ),
                    &[&match_id],
                )
                .await?;

            transaction
                .execute(
                    &format!(
                    r#"
                    WITH context AS ({context})
                    INSERT INTO nonranked_champion_stats_daily (
                      stats_date, stats_scope, queue_id, region, map, champion_id,
                      plays, wins, losses, kills_sum, deaths_sum, assists_sum,
                      damage_sum, healing_sum, mitigation_sum, credits_sum,
                      duration_sum
                    )
                    SELECT
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.region, context.map, player.champion_id,
                      COUNT(*)::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('winner', 'win')
                      )::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('loser', 'loss')
                      )::bigint,
                      SUM(player.kills)::bigint,
                      SUM(player.deaths)::bigint,
                      SUM(player.assists)::bigint,
                      SUM(player.damage)::bigint,
                      SUM(player.healing)::bigint,
                      SUM(player.mitigation)::bigint,
                      SUM(player.credits)::bigint,
                      SUM(context.duration_seconds)::bigint
                    FROM context
                    JOIN {player_table} player
                      ON player.match_id = context.match_id
                    WHERE player.stats_eligible
                      AND player.champion_id > 0
                    GROUP BY
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.region, context.map, player.champion_id
                    ON CONFLICT (
                      stats_date, stats_scope, queue_id, region, map, champion_id
                    ) DO UPDATE SET
                      plays = nonranked_champion_stats_daily.plays + EXCLUDED.plays,
                      wins = nonranked_champion_stats_daily.wins + EXCLUDED.wins,
                      losses = nonranked_champion_stats_daily.losses + EXCLUDED.losses,
                      kills_sum = nonranked_champion_stats_daily.kills_sum + EXCLUDED.kills_sum,
                      deaths_sum = nonranked_champion_stats_daily.deaths_sum + EXCLUDED.deaths_sum,
                      assists_sum = nonranked_champion_stats_daily.assists_sum + EXCLUDED.assists_sum,
                      damage_sum = nonranked_champion_stats_daily.damage_sum + EXCLUDED.damage_sum,
                      healing_sum = nonranked_champion_stats_daily.healing_sum + EXCLUDED.healing_sum,
                      mitigation_sum = nonranked_champion_stats_daily.mitigation_sum + EXCLUDED.mitigation_sum,
                      credits_sum = nonranked_champion_stats_daily.credits_sum + EXCLUDED.credits_sum,
                      duration_sum = nonranked_champion_stats_daily.duration_sum + EXCLUDED.duration_sum,
                      updated_at = now()
                    "#
                    ),
                    &[&match_id],
                )
                .await?;
        }

        transaction
            .execute(
                &format!(
                    r#"
                    WITH context AS ({context})
                    INSERT INTO casual_item_stats_daily (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      item_id, slot, item_level, plays, wins, losses
                    )
                    SELECT
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.lobby_tier, context.region, context.map,
                      item.item_id, item.slot, item.item_level,
                      COUNT(*)::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('winner', 'win')
                      )::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('loser', 'loss')
                      )::bigint
                    FROM context
                    JOIN nonranked_match_items item
                      ON item.match_id = context.match_id
                    JOIN {player_table} player
                      ON player.match_id = item.match_id
                     AND player.roster_slot = item.roster_slot
                    WHERE player.stats_eligible
                    GROUP BY
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.lobby_tier, context.region, context.map,
                      item.item_id, item.slot, item.item_level
                    ON CONFLICT (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      item_id, slot, item_level
                    ) DO UPDATE SET
                      plays = casual_item_stats_daily.plays + EXCLUDED.plays,
                      wins = casual_item_stats_daily.wins + EXCLUDED.wins,
                      losses = casual_item_stats_daily.losses + EXCLUDED.losses,
                      updated_at = now()
                    "#
                ),
                &[&match_id],
            )
            .await?;

        transaction
            .execute(
                &format!(
                    r#"
                    WITH context AS ({context})
                    INSERT INTO casual_talent_stats_daily (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      champion_id, talent_id, plays, wins, losses
                    )
                    SELECT
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.lobby_tier, context.region, context.map,
                      talent.champion_id, talent.talent_id,
                      COUNT(*)::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('winner', 'win')
                      )::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('loser', 'loss')
                      )::bigint
                    FROM context
                    JOIN nonranked_match_talents talent
                      ON talent.match_id = context.match_id
                    JOIN {player_table} player
                      ON player.match_id = talent.match_id
                     AND player.roster_slot = talent.roster_slot
                    WHERE player.stats_eligible
                    GROUP BY
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.lobby_tier, context.region, context.map,
                      talent.champion_id, talent.talent_id
                    ON CONFLICT (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      champion_id, talent_id
                    ) DO UPDATE SET
                      plays = casual_talent_stats_daily.plays + EXCLUDED.plays,
                      wins = casual_talent_stats_daily.wins + EXCLUDED.wins,
                      losses = casual_talent_stats_daily.losses + EXCLUDED.losses,
                      updated_at = now()
                    "#
                ),
                &[&match_id],
            )
            .await?;

        transaction
            .execute(
                &format!(
                    r#"
                    WITH context AS ({context})
                    INSERT INTO casual_card_stats_daily (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      champion_id, talent_id, card_id, card_level,
                      plays, wins, losses
                    )
                    SELECT
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.lobby_tier, context.region, context.map,
                      card.champion_id, card.talent_id, card.card_id, card.card_level,
                      COUNT(*)::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('winner', 'win')
                      )::bigint,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(player.win_status, '')) IN ('loser', 'loss')
                      )::bigint
                    FROM context
                    JOIN nonranked_match_cards card
                      ON card.match_id = context.match_id
                    JOIN {player_table} player
                      ON player.match_id = card.match_id
                     AND player.roster_slot = card.roster_slot
                    WHERE player.stats_eligible
                    GROUP BY
                      context.stats_date, context.stats_scope, context.queue_id,
                      context.lobby_tier, context.region, context.map,
                      card.champion_id, card.talent_id, card.card_id, card.card_level
                    ON CONFLICT (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      champion_id, talent_id, card_id, card_level
                    ) DO UPDATE SET
                      plays = casual_card_stats_daily.plays + EXCLUDED.plays,
                      wins = casual_card_stats_daily.wins + EXCLUDED.wins,
                      losses = casual_card_stats_daily.losses + EXCLUDED.losses,
                      updated_at = now()
                    "#
                ),
                &[&match_id],
            )
            .await?;

        transaction
            .execute(
                &format!(
                    r#"
                    WITH context AS ({context}),
                    team_compositions AS (
                      SELECT
                        context.stats_date, context.stats_scope, context.queue_id,
                        context.lobby_tier, context.region, context.map,
                        player.task_force,
                        COUNT(*) FILTER (
                          WHERE LOWER(COALESCE(champion.roles, '')) LIKE '%front%'
                        )::smallint AS frontline,
                        COUNT(*) FILTER (
                          WHERE LOWER(COALESCE(champion.roles, '')) LIKE '%damage%'
                        )::smallint AS damage,
                        COUNT(*) FILTER (
                          WHERE LOWER(COALESCE(champion.roles, '')) LIKE '%flank%'
                        )::smallint AS flank,
                        COUNT(*) FILTER (
                          WHERE LOWER(COALESCE(champion.roles, '')) LIKE '%support%'
                        )::smallint AS support,
                        BOOL_OR(
                          LOWER(COALESCE(player.win_status, '')) IN ('winner', 'win')
                        ) AS won,
                        BOOL_OR(
                          LOWER(COALESCE(player.win_status, '')) IN ('loser', 'loss')
                        ) AS lost
                      FROM context
                      JOIN {player_table} player
                        ON player.match_id = context.match_id
                      JOIN champions champion
                        ON champion.id = player.champion_id
                      WHERE player.stats_eligible
                        AND player.task_force IN (1, 2)
                      GROUP BY
                        context.stats_date, context.stats_scope, context.queue_id,
                        context.lobby_tier, context.region, context.map,
                        player.task_force
                      HAVING COUNT(*) = 5
                    )
                    INSERT INTO casual_composition_stats_daily (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      frontline, damage, flank, support, plays, wins, losses
                    )
                    SELECT
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      frontline, damage, flank, support,
                      1, won::int, lost::int
                    FROM team_compositions
                    ON CONFLICT (
                      stats_date, stats_scope, queue_id, lobby_tier, region, map,
                      frontline, damage, flank, support
                    ) DO UPDATE SET
                      plays = casual_composition_stats_daily.plays + 1,
                      wins = casual_composition_stats_daily.wins + EXCLUDED.wins,
                      losses = casual_composition_stats_daily.losses + EXCLUDED.losses,
                      updated_at = now()
                    "#
                ),
                &[&match_id],
            )
            .await?;

        transaction
            .execute(
                r#"
                UPDATE match_ingest_status
                SET completed_stages = (
                      SELECT ARRAY(
                        SELECT DISTINCT stage_name
                        FROM unnest(
                          completed_stages || ARRAY[$2, $3]::text[]
                        ) stage_name
                      )
                    ),
                    acquisition_state = 'complete',
                    updated_at = now()
                WHERE match_id = $1
                "#,
                &[&match_id, &stage, &NONRANKED_CORE_STATS_STAGE],
            )
            .await?;
        transaction.commit().await?;
        Ok(true)
    }
}

fn reference_name(value: &str, prefix: &str, id: i32) -> String {
    let value = value.trim();
    if value.is_empty() {
        format!("{prefix} {id}")
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use paladinscat_core::config::BackendConfig;
    use serde_json::json;

    use super::*;

    fn player_fixture() -> Value {
        json!({
            "player_id": 123,
            "champion_id": 7,
            "active_id_1": 1001,
            "active_level_1": 12,
            "item_active_1": "Chronos",
            "item_id_1": 2001,
            "item_level_1": 5,
            "item_purch_1": "Loadout Card",
            "item_id_6": 3001,
            "item_purch_6": "Chosen Talent"
        })
    }

    #[test]
    fn extracts_items_cards_and_talent_from_one_shared_normalizer() {
        let facts = NonrankedMechanicsFacts::from_players(
            42,
            424,
            MatchPopulation::Casual,
            NonrankedScope::Casual,
            &[player_fixture()],
        )
        .expect("facts");
        assert_eq!(facts.items.len(), 1);
        assert_eq!(facts.items[0].item_level, 3);
        assert_eq!(facts.cards.len(), 1);
        assert_eq!(facts.cards[0].card_level, 5);
        assert_eq!(facts.cards[0].talent_id, 3001);
        assert_eq!(facts.talents.len(), 1);
        assert_eq!(facts.talents[0].talent_name, "Chosen Talent");
    }

    #[test]
    fn ranked_population_is_rejected_before_any_database_write() {
        let error = NonrankedMechanicsFacts::from_players(
            42,
            486,
            MatchPopulation::Ranked,
            NonrankedScope::Other,
            &[player_fixture()],
        )
        .expect_err("ranked rejection");
        assert!(matches!(
            error,
            NonrankedMechanicsError::RankedPopulation { match_id: 42 }
        ));
    }

    #[test]
    fn special_and_custom_use_nonranked_mechanics_with_classified_scope() {
        let facts = NonrankedMechanicsFacts::from_players(
            42,
            9999,
            MatchPopulation::Special,
            NonrankedScope::Custom,
            &[player_fixture()],
        )
        .expect("special facts");
        assert_eq!(facts.population, MatchPopulation::Special);
        assert_eq!(facts.scope.as_database(), "custom");
        assert_eq!(facts.items.len(), 1);
        assert_eq!(facts.cards.len(), 1);
        assert_eq!(facts.talents.len(), 1);
    }

    #[test]
    fn casual_and_special_scopes_cannot_cross_classification() {
        assert!(matches!(
            NonrankedMechanicsFacts::from_players(
                42,
                424,
                MatchPopulation::Casual,
                NonrankedScope::Custom,
                &[player_fixture()],
            ),
            Err(NonrankedMechanicsError::ScopeMismatch { .. })
        ));
        assert!(matches!(
            NonrankedMechanicsFacts::from_players(
                43,
                9999,
                MatchPopulation::Special,
                NonrankedScope::Casual,
                &[player_fixture()],
            ),
            Err(NonrankedMechanicsError::ScopeMismatch { .. })
        ));
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with migration 109"]
    async fn live_repository_persists_normalized_nonranked_facts_only() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "nonranked-mechanics-integration").expect("database");
        let repository = NonrankedMechanicsRepository::new(database.clone());
        let match_id = 9_882_000_001_i64;
        let client = database.connection().await.expect("connection");
        client
            .execute(
                r#"
                INSERT INTO champions (id, name, health, speed, roles)
                VALUES (7, 'Integration Champion', 1, 1, 'Damage')
                ON CONFLICT (id) DO UPDATE SET roles = EXCLUDED.roles
                "#,
                &[],
            )
            .await
            .expect("champion fixture");
        client
            .execute(
                r#"
                INSERT INTO match_ingest_status (
                  match_id, status, completed_stages, queue_id, population,
                  acquisition_state, lease_owner, lease_until
                )
                VALUES (
                  $1, 'processing', ARRAY[
                    'player_facts', 'match_bans', 'nonranked_core_stats'
                  ],
                  424, 'casual', 'facts_ready',
                  'nonranked-mechanics-test', now() + interval '5 minutes'
                )
                ON CONFLICT (match_id) DO UPDATE SET
                  status = EXCLUDED.status,
                  completed_stages = EXCLUDED.completed_stages,
                  queue_id = EXCLUDED.queue_id,
                  population = EXCLUDED.population,
                  acquisition_state = EXCLUDED.acquisition_state,
                  lease_owner = EXCLUDED.lease_owner,
                  lease_until = EXCLUDED.lease_until
                "#,
                &[&match_id],
            )
            .await
            .expect("lifecycle fixture");
        client
            .execute(
                r#"
                INSERT INTO casual_matches (
                  match_id, queue_id, entry_datetime, region, map,
                  duration_seconds, quality, stats_eligible, player_count,
                  source, lobby_tier
                )
                VALUES (
                  $1, 424, now(), 'North America', 'Integration Map',
                  600, 'complete', true, 1, 'direct', 20
                )
                ON CONFLICT (match_id) DO UPDATE SET
                  stats_eligible = true,
                  lobby_tier = 20
                "#,
                &[&match_id],
            )
            .await
            .expect("match fixture");
        client
            .execute(
                r#"
                INSERT INTO casual_match_players (
                  match_id, roster_slot, player_id, champion_id,
                  task_force, win_status, participant_kind, source,
                  stats_eligible, kills, deaths, assists, damage,
                  healing, mitigation, credits
                )
                VALUES (
                  $1, 1, 123, 7, 1, 'Winner', 'human', 'direct',
                  true, 10, 2, 8, 40000, 1000, 500, 3000
                )
                ON CONFLICT (match_id, roster_slot) DO UPDATE SET
                  stats_eligible = true,
                  win_status = 'Winner'
                "#,
                &[&match_id],
            )
            .await
            .expect("player fixture");
        client
            .execute(
                r#"
                INSERT INTO nonranked_map_stats_daily (
                  stats_date, stats_scope, queue_id, region, map,
                  matches, duration_sum
                )
                VALUES (
                  CURRENT_DATE, 'casual', 424, 'North America',
                  'Integration Map', 1, 600
                )
                ON CONFLICT (
                  stats_date, stats_scope, queue_id, region, map
                ) DO UPDATE SET matches = 1, duration_sum = 600
                "#,
                &[],
            )
            .await
            .expect("adopted map projection fixture");
        client
            .execute(
                r#"
                INSERT INTO nonranked_champion_stats_daily (
                  stats_date, stats_scope, queue_id, region, map, champion_id,
                  plays, wins, losses, kills_sum, deaths_sum, assists_sum,
                  damage_sum, healing_sum, mitigation_sum, credits_sum,
                  duration_sum
                )
                VALUES (
                  CURRENT_DATE, 'casual', 424, 'North America',
                  'Integration Map', 7,
                  1, 1, 0, 10, 2, 8, 40000, 1000, 500, 3000, 600
                )
                ON CONFLICT (
                  stats_date, stats_scope, queue_id, region, map, champion_id
                ) DO UPDATE SET plays = 1, wins = 1, duration_sum = 600
                "#,
                &[],
            )
            .await
            .expect("adopted champion projection fixture");
        drop(client);

        let facts = NonrankedMechanicsFacts::from_players(
            match_id,
            424,
            MatchPopulation::Casual,
            NonrankedScope::Casual,
            &[player_fixture()],
        )
        .expect("facts");
        assert!(
            repository
                .replace_match_facts(&facts, "nonranked-mechanics-test")
                .await
                .expect("persist facts")
        );
        assert!(
            repository
                .project_match(
                    match_id,
                    MatchPopulation::Casual,
                    "nonranked-mechanics-test",
                )
                .await
                .expect("project")
        );
        assert!(
            !repository
                .project_match(
                    match_id,
                    MatchPopulation::Casual,
                    "nonranked-mechanics-test",
                )
                .await
                .expect("idempotent project")
        );

        let client = database.connection().await.expect("connection");
        for (table, expected) in [
            ("nonranked_match_items", 1_i64),
            ("nonranked_match_talents", 1_i64),
            ("nonranked_match_cards", 1_i64),
        ] {
            let row = client
                .query_one(
                    &format!("SELECT COUNT(*)::bigint AS count FROM {table} WHERE match_id = $1"),
                    &[&match_id],
                )
                .await
                .expect("fact count");
            assert_eq!(row.get::<_, i64>("count"), expected);
        }
        for table in [
            "match_player_items",
            "match_player_talents",
            "match_player_cards",
        ] {
            let row = client
                .query_one(
                    &format!("SELECT COUNT(*)::bigint AS count FROM {table} WHERE match_id = $1"),
                    &[&match_id],
                )
                .await
                .expect("ranked fact count");
            assert_eq!(row.get::<_, i64>("count"), 0);
        }
        let casual_item = client
            .query_one(
                r#"
                SELECT plays, wins, losses
                FROM casual_item_stats_daily
                WHERE item_id = 1001
                  AND map = 'Integration Map'
                  AND lobby_tier = 20
                "#,
                &[],
            )
            .await
            .expect("casual aggregate");
        assert_eq!(casual_item.get::<_, i64>("plays"), 1);
        assert_eq!(casual_item.get::<_, i64>("wins"), 1);
        assert_eq!(casual_item.get::<_, i64>("losses"), 0);
        let adopted_map = client
            .query_one(
                r#"
                SELECT matches, duration_sum
                FROM nonranked_map_stats_daily
                WHERE stats_scope = 'casual'
                  AND queue_id = 424
                  AND map = 'Integration Map'
                "#,
                &[],
            )
            .await
            .expect("adopted map aggregate");
        assert_eq!(adopted_map.get::<_, i64>("matches"), 1);
        assert_eq!(adopted_map.get::<_, i64>("duration_sum"), 600);
        let adopted_champion = client
            .query_one(
                r#"
                SELECT plays, wins, duration_sum
                FROM nonranked_champion_stats_daily
                WHERE stats_scope = 'casual'
                  AND queue_id = 424
                  AND map = 'Integration Map'
                  AND champion_id = 7
                "#,
                &[],
            )
            .await
            .expect("adopted champion aggregate");
        assert_eq!(adopted_champion.get::<_, i64>("plays"), 1);
        assert_eq!(adopted_champion.get::<_, i64>("wins"), 1);
        assert_eq!(adopted_champion.get::<_, i64>("duration_sum"), 600);
        let ranked_item = client
            .query_one(
                "SELECT COUNT(*)::bigint AS count FROM item_counts_ranked WHERE item_id = 1001",
                &[],
            )
            .await
            .expect("ranked aggregate");
        assert_eq!(ranked_item.get::<_, i64>("count"), 0);
        let ranked_scope_write = client
            .execute(
                r#"
                INSERT INTO casual_item_stats_daily (
                  stats_date, stats_scope, queue_id, lobby_tier, region, map,
                  item_id, slot, item_level, plays, wins, losses
                )
                VALUES (
                  CURRENT_DATE, 'ranked', 486, 20, 'North America',
                  'Ranked Pollution', 1001, 1, 1, 1, 1, 0
                )
                "#,
                &[],
            )
            .await;
        assert!(
            ranked_scope_write.is_err(),
            "the database must reject ranked scope in a non-ranked aggregate"
        );
        let stages = client
            .query_one(
                "SELECT completed_stages FROM match_ingest_status WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("stages")
            .get::<_, Vec<String>>("completed_stages");
        assert!(stages.contains(&"casual_mechanics_stats".to_owned()));
        assert!(!stages.contains(&"ranked_stats".to_owned()));

        client
            .execute(
                "DELETE FROM casual_item_stats_daily WHERE item_id = 1001 AND map = 'Integration Map'",
                &[],
            )
            .await
            .expect("item aggregate cleanup");
        client
            .execute(
                "DELETE FROM casual_talent_stats_daily WHERE talent_id = 3001 AND map = 'Integration Map'",
                &[],
            )
            .await
            .expect("talent aggregate cleanup");
        client
            .execute(
                "DELETE FROM casual_card_stats_daily WHERE card_id = 2001 AND map = 'Integration Map'",
                &[],
            )
            .await
            .expect("card aggregate cleanup");
        client
            .execute(
                "DELETE FROM nonranked_champion_stats_daily WHERE champion_id = 7 AND map = 'Integration Map'",
                &[],
            )
            .await
            .expect("champion aggregate cleanup");
        client
            .execute(
                "DELETE FROM nonranked_map_stats_daily WHERE map = 'Integration Map'",
                &[],
            )
            .await
            .expect("map aggregate cleanup");
        for table in [
            "nonranked_match_items",
            "nonranked_match_talents",
            "nonranked_match_cards",
        ] {
            client
                .execute(
                    &format!("DELETE FROM {table} WHERE match_id = $1"),
                    &[&match_id],
                )
                .await
                .expect("cleanup facts");
        }
        client
            .execute(
                "DELETE FROM casual_matches WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("match cleanup");
        client
            .execute(
                "DELETE FROM match_ingest_status WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("lifecycle cleanup");
    }
}
