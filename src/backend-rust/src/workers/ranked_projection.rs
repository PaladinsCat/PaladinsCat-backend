use paladinscat_core::database::{Database, DatabaseError};

use super::projections::{project_performance, project_scalable};
use super::rating::{RatingApplicationResult, RatingRepository};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RankedProjectionResult {
    Projected,
    AlreadyProjected,
}

#[derive(Debug, thiserror::Error)]
pub enum RankedProjectionError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
    #[error("ranked projection rejected non-ranked or incomplete match {0}")]
    Rejected(i64),
    #[error("ranked rating projection failed: {0}")]
    Rating(String),
}

#[derive(Clone)]
pub struct RankedProjectionRepository {
    database: Database,
}

impl RankedProjectionRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn project_match(
        &self,
        match_id: i64,
    ) -> Result<RankedProjectionResult, RankedProjectionError> {
        let population = self
            .database
            .one_json(
                "SELECT mis.population FROM match_ingest_status mis \
                 JOIN matches m ON m.match_id=mis.match_id \
                 WHERE mis.match_id=$1 AND mis.population='ranked' AND m.queue_id=486 \
                   AND COALESCE(m.limited,FALSE)=FALSE",
                &[&match_id],
            )
            .await?;
        if population.is_none() {
            return Err(RankedProjectionError::Rejected(match_id));
        }
        match RatingRepository::new(self.database.clone())
            .apply_match(match_id, false)
            .await
            .map_err(|error| RankedProjectionError::Rating(format!("{error:?}")))?
        {
            RatingApplicationResult::Busy | RatingApplicationResult::Deferred => {
                return Err(RankedProjectionError::Rating(
                    "rating stream is not ready".to_owned(),
                ));
            }
            RatingApplicationResult::Applied | RatingApplicationResult::Skipped => {}
        }
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let row = transaction
            .query_opt(
                "SELECT completed_stages,population FROM match_ingest_status \
                 WHERE match_id=$1 FOR UPDATE",
                &[&match_id],
            )
            .await?
            .ok_or(RankedProjectionError::Rejected(match_id))?;
        let population = row.get::<_, String>("population");
        if population != "ranked" {
            return Err(RankedProjectionError::Rejected(match_id));
        }
        let stages = row.get::<_, Vec<String>>("completed_stages");
        if stages.iter().any(|stage| stage == "ranked_stats") {
            transaction.commit().await?;
            return Ok(RankedProjectionResult::AlreadyProjected);
        }
        let eligible = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM matches m WHERE m.match_id=$1 AND m.queue_id=486) \
                 AND (SELECT count(*) FROM match_players p WHERE p.match_id=$1 \
                   AND p.is_ranked AND p.champion_id>0 \
                   AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss'))>=10 eligible",
                &[&match_id],
            )
            .await?
            .get::<_, bool>("eligible");
        if !eligible {
            return Err(RankedProjectionError::Rejected(match_id));
        }

        transaction
            .execute(
                "INSERT INTO match_lobby_tiers(match_id,entry_datetime,lobby_tier,known_players,updated_at)\
                 SELECT m.match_id,m.entry_datetime,\
                   COALESCE(round(avg(p.league_tier) FILTER(WHERE p.league_tier BETWEEN 1 AND 26)),0)::smallint,\
                   count(*) FILTER(WHERE p.league_tier BETWEEN 1 AND 26)::smallint,now() \
                 FROM matches m LEFT JOIN match_players p ON p.match_id=m.match_id AND p.entry_datetime=m.entry_datetime \
                 WHERE m.match_id=$1 AND m.queue_id=486 GROUP BY m.match_id,m.entry_datetime \
                 ON CONFLICT(match_id,entry_datetime) DO UPDATE SET lobby_tier=EXCLUDED.lobby_tier,\
                   known_players=EXCLUDED.known_players,updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO item_counts_ranked(item_id,item_name,slot,item_level,count,wins,losses,winrate,updated_at)\
                 SELECT f.item_id,i.item_name,f.slot,COALESCE(f.item_level,0),count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,\
                   round(count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::numeric/count(*)*100,2),now() \
                 FROM match_player_items f JOIN match_players p ON p.match_id=f.match_id AND p.player_id=f.player_id \
                 LEFT JOIN items i ON i.item_id=f.item_id WHERE f.match_id=$1 \
                 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') \
                 GROUP BY f.item_id,i.item_name,f.slot,COALESCE(f.item_level,0) \
                 ON CONFLICT(item_id,slot,item_level) DO UPDATE SET item_name=EXCLUDED.item_name,\
                   count=item_counts_ranked.count+EXCLUDED.count,wins=item_counts_ranked.wins+EXCLUDED.wins,\
                   losses=item_counts_ranked.losses+EXCLUDED.losses,\
                   winrate=round((item_counts_ranked.wins+EXCLUDED.wins)::numeric/NULLIF(item_counts_ranked.count+EXCLUDED.count,0)*100,2),updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "WITH claimed AS(INSERT INTO map_item_counts_ranked_matches(match_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING match_id)\
                 INSERT INTO map_item_counts_ranked(map_name,lobby_tier,item_id,count,wins,losses,updated_at)\
                 SELECT m.map,COALESCE(t.lobby_tier,0),f.item_id,count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,now() \
                 FROM claimed JOIN matches m ON m.match_id=claimed.match_id \
                 JOIN match_players p ON p.match_id=m.match_id AND p.entry_datetime=m.entry_datetime \
                 JOIN match_player_items f ON f.match_id=p.match_id AND f.player_id=p.player_id \
                 LEFT JOIN match_lobby_tiers t ON t.match_id=m.match_id AND t.entry_datetime=m.entry_datetime \
                 WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') \
                 GROUP BY m.map,COALESCE(t.lobby_tier,0),f.item_id \
                 ON CONFLICT(map_name,lobby_tier,item_id) DO UPDATE SET count=map_item_counts_ranked.count+EXCLUDED.count,\
                   wins=map_item_counts_ranked.wins+EXCLUDED.wins,losses=map_item_counts_ranked.losses+EXCLUDED.losses,updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO talent_counts_ranked(talent_id,champion_name,talent_name,count,wins,losses,winrate,updated_at)\
                 SELECT f.talent_id,c.name,t.talent_name,count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,\
                   round(count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::numeric/count(*)*100,2),now() \
                 FROM match_player_talents f JOIN match_players p ON p.match_id=f.match_id AND p.player_id=f.player_id \
                 LEFT JOIN talents t ON t.talent_id=f.talent_id LEFT JOIN champions c ON c.id=COALESCE(t.champion_id,p.champion_id) \
                 WHERE f.match_id=$1 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') \
                 GROUP BY f.talent_id,c.name,t.talent_name \
                 ON CONFLICT(talent_id) DO UPDATE SET champion_name=EXCLUDED.champion_name,talent_name=EXCLUDED.talent_name,\
                   count=talent_counts_ranked.count+EXCLUDED.count,wins=talent_counts_ranked.wins+EXCLUDED.wins,\
                   losses=talent_counts_ranked.losses+EXCLUDED.losses,\
                   winrate=round((talent_counts_ranked.wins+EXCLUDED.wins)::numeric/NULLIF(talent_counts_ranked.count+EXCLUDED.count,0)*100,2),updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO card_counts_ranked(card_id,champion_name,card_name,card_level,count,wins,losses,winrate,updated_at)\
                 SELECT f.card_id,c.name,card.card_name,COALESCE(f.card_level,0),count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,\
                   round(count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::numeric/count(*)*100,2),now() \
                 FROM match_player_cards f JOIN match_players p ON p.match_id=f.match_id AND p.player_id=f.player_id \
                 LEFT JOIN cards card ON card.card_id=f.card_id LEFT JOIN champions c ON c.id=COALESCE(card.champion_id,p.champion_id) \
                 WHERE f.match_id=$1 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') \
                 GROUP BY f.card_id,c.name,card.card_name,COALESCE(f.card_level,0) \
                 ON CONFLICT(card_id,card_level) DO UPDATE SET champion_name=EXCLUDED.champion_name,card_name=EXCLUDED.card_name,\
                   count=card_counts_ranked.count+EXCLUDED.count,wins=card_counts_ranked.wins+EXCLUDED.wins,\
                   losses=card_counts_ranked.losses+EXCLUDED.losses,\
                   winrate=round((card_counts_ranked.wins+EXCLUDED.wins)::numeric/NULLIF(card_counts_ranked.count+EXCLUDED.count,0)*100,2),updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO talent_card_counts_ranked(talent_id,card_id,card_level,count,wins,losses,updated_at)\
                 SELECT talent.talent_id,card.card_id,COALESCE(card.card_level,0),count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,now() \
                 FROM match_player_talents talent JOIN match_player_cards card \
                   ON card.match_id=talent.match_id AND card.player_id=talent.player_id \
                 JOIN match_players p ON p.match_id=talent.match_id AND p.player_id=talent.player_id \
                 WHERE talent.match_id=$1 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') \
                 GROUP BY talent.talent_id,card.card_id,COALESCE(card.card_level,0) \
                 ON CONFLICT(talent_id,card_id,card_level) DO UPDATE SET count=talent_card_counts_ranked.count+EXCLUDED.count,\
                   wins=talent_card_counts_ranked.wins+EXCLUDED.wins,losses=talent_card_counts_ranked.losses+EXCLUDED.losses,updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO skin_counts_ranked(champion_id,skin_id,league_tier,skin_name,count,wins,losses,updated_at)\
                 SELECT p.champion_id,COALESCE(p.skin_id,0),COALESCE(p.league_tier,0)::smallint,\
                   COALESCE(NULLIF(p.skin_name,''),'Default'),count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,now() \
                 FROM match_players p WHERE p.match_id=$1 AND p.champion_id>0 \
                 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') \
                 GROUP BY p.champion_id,COALESCE(p.skin_id,0),COALESCE(p.league_tier,0),COALESCE(NULLIF(p.skin_name,''),'Default') \
                 ON CONFLICT(champion_id,skin_id,league_tier) DO UPDATE SET skin_name=EXCLUDED.skin_name,\
                   count=skin_counts_ranked.count+EXCLUDED.count,wins=skin_counts_ranked.wins+EXCLUDED.wins,\
                   losses=skin_counts_ranked.losses+EXCLUDED.losses,updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO bans_ranked(champion_id,champion_name,ban_total,slot1,slot2,slot3,slot4,slot5,slot6,slot7,slot8,updated_at)\
                 SELECT b.champion_id,COALESCE(c.name,'Unknown'),count(*)::int,\
                   count(*) FILTER(WHERE b.ban_slot=1)::int,count(*) FILTER(WHERE b.ban_slot=2)::int,\
                   count(*) FILTER(WHERE b.ban_slot=3)::int,count(*) FILTER(WHERE b.ban_slot=4)::int,\
                   count(*) FILTER(WHERE b.ban_slot=5)::int,count(*) FILTER(WHERE b.ban_slot=6)::int,\
                   count(*) FILTER(WHERE b.ban_slot=7)::int,count(*) FILTER(WHERE b.ban_slot=8)::int,now() \
                 FROM match_bans b LEFT JOIN champions c ON c.id=b.champion_id WHERE b.match_id=$1 AND b.champion_id>0 \
                 GROUP BY b.champion_id,c.name ON CONFLICT(champion_id) DO UPDATE SET champion_name=EXCLUDED.champion_name,\
                   ban_total=bans_ranked.ban_total+EXCLUDED.ban_total,slot1=bans_ranked.slot1+EXCLUDED.slot1,\
                   slot2=bans_ranked.slot2+EXCLUDED.slot2,slot3=bans_ranked.slot3+EXCLUDED.slot3,\
                   slot4=bans_ranked.slot4+EXCLUDED.slot4,slot5=bans_ranked.slot5+EXCLUDED.slot5,\
                   slot6=bans_ranked.slot6+EXCLUDED.slot6,slot7=bans_ranked.slot7+EXCLUDED.slot7,\
                   slot8=bans_ranked.slot8+EXCLUDED.slot8,updated_at=now()",
                &[&match_id],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO champion_stats_ranked(champion_id,champion_name,total_matches,wins,losses,sum_kills,sum_deaths,\
                  sum_assists,sum_damage,sum_gold,sum_heal,sum_mitigation,sum_league_tier,league_tier_count,updated_at)\
                 SELECT p.champion_id,COALESCE(c.name,p.champion_id::text),count(*)::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('winner','win'))::int,\
                   count(*) FILTER(WHERE lower(p.win_status) IN('loser','loss'))::int,\
                   sum(COALESCE(p.kills,0))::int,sum(COALESCE(p.deaths,0))::int,sum(COALESCE(p.assists,0))::int,\
                   sum(COALESCE(p.damage_done_physical,0))::int,sum(COALESCE(p.gold_earned,0))::int,\
                   sum(COALESCE(p.healing,0))::int,sum(COALESCE(p.damage_mitigated,0))::int,\
                   COALESCE(sum(p.league_tier) FILTER(WHERE p.league_tier BETWEEN 1 AND 26),0)::int,\
                   count(*) FILTER(WHERE p.league_tier BETWEEN 1 AND 26)::int,now() \
                 FROM match_players p LEFT JOIN champions c ON c.id=p.champion_id WHERE p.match_id=$1 AND p.champion_id>0 \
                 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss') GROUP BY p.champion_id,c.name \
                 ON CONFLICT(champion_id) DO UPDATE SET champion_name=EXCLUDED.champion_name,\
                   total_matches=champion_stats_ranked.total_matches+EXCLUDED.total_matches,\
                   wins=champion_stats_ranked.wins+EXCLUDED.wins,losses=champion_stats_ranked.losses+EXCLUDED.losses,\
                   sum_kills=champion_stats_ranked.sum_kills+EXCLUDED.sum_kills,\
                   sum_deaths=champion_stats_ranked.sum_deaths+EXCLUDED.sum_deaths,\
                   sum_assists=champion_stats_ranked.sum_assists+EXCLUDED.sum_assists,\
                   sum_damage=champion_stats_ranked.sum_damage+EXCLUDED.sum_damage,\
                   sum_gold=champion_stats_ranked.sum_gold+EXCLUDED.sum_gold,sum_heal=champion_stats_ranked.sum_heal+EXCLUDED.sum_heal,\
                   sum_mitigation=champion_stats_ranked.sum_mitigation+EXCLUDED.sum_mitigation,\
                   sum_league_tier=champion_stats_ranked.sum_league_tier+EXCLUDED.sum_league_tier,\
                   league_tier_count=champion_stats_ranked.league_tier_count+EXCLUDED.league_tier_count,updated_at=now()",
                &[&match_id],
            )
            .await?;
        project_compositions(&transaction, match_id).await?;
        project_relationships(&transaction, match_id).await?;
        project_performance(&transaction, match_id).await?;
        project_scalable(&transaction, match_id).await?;
        transaction
            .execute(
                "UPDATE match_ingest_status SET completed_stages=(SELECT array_agg(DISTINCT stage ORDER BY stage) \
                   FROM unnest(completed_stages||ARRAY['ranked_stats','performance_projections','scalable_stats']::TEXT[]) stage),\
                 status='complete',acquisition_state='complete',completed_at=COALESCE(completed_at,now()),\
                 lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE match_id=$1",
                &[&match_id],
            )
            .await?;
        transaction.commit().await?;
        Ok(RankedProjectionResult::Projected)
    }
}

async fn project_compositions(
    transaction: &tokio_postgres::Transaction<'_>,
    match_id: i64,
) -> Result<(), tokio_postgres::Error> {
    transaction.execute(
        "WITH teams AS(SELECT p.task_force,\
           count(*) FILTER(WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%')::smallint frontline,\
           count(*) FILTER(WHERE c.roles ILIKE '%Damage%')::smallint damage,\
           count(*) FILTER(WHERE c.roles ILIKE '%Flank%')::smallint flank,\
           count(*) FILTER(WHERE c.roles ILIKE '%Support%')::smallint support,\
           bool_or(lower(p.win_status) IN('winner','win')) won \
         FROM match_players p JOIN champions c ON c.id=p.champion_id WHERE p.match_id=$1 AND p.task_force IN(1,2) \
         GROUP BY p.task_force HAVING count(*)=5),rows AS(SELECT frontline||'-'||damage||'-'||flank||'-'||support comp_id,*,\
           COALESCE((SELECT lobby_tier FROM match_lobby_tiers WHERE match_id=$1 ORDER BY entry_datetime DESC LIMIT 1),0) lobby_tier FROM teams),\
         collapsed AS(SELECT comp_id,lobby_tier,frontline,damage,flank,support,count(*)::int count,\
           count(*) FILTER(WHERE won)::int wins,count(*) FILTER(WHERE NOT won)::int losses FROM rows \
           GROUP BY comp_id,lobby_tier,frontline,damage,flank,support)\
         INSERT INTO match_compositions_ranked(comp_id,lobby_tier,frontline,damage,flank,support,count,wins,losses,updated_at)\
         SELECT comp_id,lobby_tier,frontline,damage,flank,support,count,wins,losses,now() FROM collapsed \
         ON CONFLICT(comp_id,lobby_tier) DO UPDATE SET count=match_compositions_ranked.count+EXCLUDED.count,\
           wins=match_compositions_ranked.wins+EXCLUDED.wins,losses=match_compositions_ranked.losses+EXCLUDED.losses,updated_at=now()",
        &[&match_id],
    ).await?;
    Ok(())
}

async fn project_relationships(
    transaction: &tokio_postgres::Transaction<'_>,
    match_id: i64,
) -> Result<(), tokio_postgres::Error> {
    transaction.execute(
        "INSERT INTO player_relationships(source_player_id,target_player_id,same_team,same_party,count,first_seen,last_seen)\
         SELECT LEAST(a.player_id,b.player_id),GREATEST(a.player_id,b.player_id),a.task_force=b.task_force,\
           a.party>0 AND a.party=b.party,1,now(),now() FROM match_players a JOIN match_players b \
           ON b.match_id=a.match_id AND b.entry_datetime=a.entry_datetime AND b.player_id>a.player_id \
         WHERE a.match_id=$1 AND a.player_id>0 AND b.player_id>0 \
         ON CONFLICT(source_player_id,target_player_id,same_team) DO UPDATE SET \
           same_party=player_relationships.same_party OR EXCLUDED.same_party,count=player_relationships.count+1,last_seen=now()",
        &[&match_id],
    ).await?;
    transaction.execute(
        "INSERT INTO match_opponents(player_id,player_champion_id,opponent_champion_id,wins,losses)\
         SELECT p.player_id,p.champion_id,o.champion_id,\
           (lower(p.win_status) IN('winner','win'))::int,(lower(p.win_status) IN('loser','loss'))::int \
         FROM match_players p JOIN match_players o ON o.match_id=p.match_id AND o.entry_datetime=p.entry_datetime \
           AND o.task_force<>p.task_force AND o.champion_id>0 \
         WHERE p.match_id=$1 AND p.player_id>0 AND p.champion_id>0 \
         ON CONFLICT(player_id,player_champion_id,opponent_champion_id) DO UPDATE SET \
           wins=match_opponents.wins+EXCLUDED.wins,losses=match_opponents.losses+EXCLUDED.losses",
        &[&match_id],
    ).await?;
    Ok(())
}
