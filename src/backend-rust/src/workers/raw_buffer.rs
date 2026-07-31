use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

use super::{
    casual_mechanics::CasualMechanicsRepository,
    match_facts::{CanonicalMatchPayload, MatchFactError, MatchFactRepository},
    ranked_projection::RankedProjectionRepository,
};

const MAX_RETRIES: i32 = 3;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RawBufferBatchResult {
    pub processed: i32,
    pub failed: i32,
    pub deferred: i32,
}

#[derive(Debug, thiserror::Error)]
pub enum RawBufferError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
    #[error(transparent)]
    MatchFacts(#[from] MatchFactError),
    #[error("raw buffer row {row_id} has unsupported entity type {entity_type} ({endpoint})")]
    Unsupported {
        row_id: i64,
        entity_type: String,
        endpoint: String,
    },
    #[error("raw buffer row {row_id} has invalid payload: {message}")]
    Invalid { row_id: i64, message: String },
}

#[derive(Clone, Debug)]
struct ClaimedRow {
    id: i64,
    raw_data: Value,
    endpoint: String,
    entity_type: String,
    entity_id: String,
    retry_count: i32,
}

pub async fn process_raw_buffer_batch(
    database: &Database,
    batch_size: usize,
) -> Result<RawBufferBatchResult, RawBufferError> {
    recover_stale_leases(database).await?;
    let rows = claim_rows(database, batch_size).await?;
    let mut result = RawBufferBatchResult::default();
    for row in rows {
        match process_row(database, &row).await {
            Ok(()) => {
                mark_processed(database, row.id).await?;
                result.processed = result.processed.saturating_add(1);
            }
            Err(error) => {
                let retry = row.retry_count.saturating_add(1);
                let terminal =
                    retry >= MAX_RETRIES || matches!(error, RawBufferError::Unsupported { .. });
                mark_failed_or_deferred(database, row.id, retry, terminal, &error.to_string())
                    .await?;
                if terminal {
                    result.failed = result.failed.saturating_add(1);
                } else {
                    result.deferred = result.deferred.saturating_add(1);
                }
            }
        }
    }
    Ok(result)
}

async fn recover_stale_leases(database: &Database) -> Result<(), DatabaseError> {
    database
        .query_json(
            "UPDATE raw_ingest_buffer SET \
               status=CASE WHEN retry_count+1 >= $1 THEN 'failed' ELSE 'pending' END,\
               retry_count=retry_count+1,\
               error_message=concat_ws(' | ',nullif(error_message,''),'Rust worker stale lease recovery'),\
               processed_at=CASE WHEN retry_count+1 >= $1 THEN now() ELSE NULL END,\
               available_at=CASE WHEN retry_count+1 >= $1 THEN available_at ELSE now()+INTERVAL '30 seconds' END \
             WHERE status='processing' \
               AND COALESCE(processed_at,created_at)<now()-INTERVAL '15 minutes'",
            &[&MAX_RETRIES],
        )
        .await?;
    Ok(())
}

async fn claim_rows(
    database: &Database,
    batch_size: usize,
) -> Result<Vec<ClaimedRow>, RawBufferError> {
    let limit = i64::try_from(batch_size.clamp(1, 500)).unwrap_or(50);
    let rows = database
        .query_json(
            "WITH selected AS(\
               SELECT id FROM raw_ingest_buffer \
               WHERE status='pending' AND available_at<=now() \
               ORDER BY CASE \
                 WHEN entity_type='match' THEN 0 \
                 WHEN entity_type IN('match_history','prefetch_match') THEN 1 \
                 ELSE 2 END,created_at,id \
               LIMIT $1 FOR UPDATE SKIP LOCKED\
             ),claimed AS(\
               UPDATE raw_ingest_buffer rib SET status='processing',processed_at=now(),error_message=NULL \
               FROM selected WHERE rib.id=selected.id \
               RETURNING rib.id,rib.raw_data,rib.endpoint,rib.entity_type,rib.entity_id,rib.retry_count\
             ) SELECT * FROM claimed ORDER BY id",
            &[&limit],
        )
        .await?;
    rows.into_iter().map(decode_claimed_row).collect()
}

fn decode_claimed_row(value: Value) -> Result<ClaimedRow, RawBufferError> {
    let id = integer(&value, "id").unwrap_or_default();
    let raw_data = value.get("raw_data").cloned().unwrap_or(Value::Null);
    if id <= 0 || raw_data.is_null() {
        return Err(RawBufferError::Invalid {
            row_id: id,
            message: "claim did not return id/raw_data".to_owned(),
        });
    }
    Ok(ClaimedRow {
        id,
        raw_data,
        endpoint: text(&value, "endpoint"),
        entity_type: text(&value, "entity_type"),
        entity_id: text(&value, "entity_id"),
        retry_count: integer(&value, "retry_count")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
    })
}

async fn process_row(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    let endpoint = row.endpoint.to_ascii_lowercase();
    let entity_type = row.entity_type.to_ascii_lowercase();
    if is_history_contract(&endpoint, &entity_type, &row.raw_data) {
        return persist_match_history(database, row).await;
    }
    match entity_type.as_str() {
        "match" => persist_match(database, row).await,
        "loadout" => persist_loadouts(database, row).await,
        "player" => persist_players(database, row).await,
        "champion" => persist_champions(database, row).await,
        "item" => persist_items(database, row).await,
        "player_status" => persist_player_status(database, row).await,
        "player_champions" => persist_player_champions(database, row).await,
        "player_achievements" => persist_player_achievements(database, row).await,
        "champion_skins" => persist_champion_skins(database, row).await,
        "live_match" => persist_live_match(database, row).await,
        "leaderboard" | "league_leaderboard" => persist_leaderboard(database, row).await,
        "esports" | "esports_team" => persist_esports(database, row).await,
        "bounty_items" => persist_bounty_items(database, row).await,
        _ => Err(RawBufferError::Unsupported {
            row_id: row.id,
            entity_type: row.entity_type.clone(),
            endpoint: row.endpoint.clone(),
        }),
    }
}

fn is_history_contract(endpoint: &str, entity_type: &str, raw: &Value) -> bool {
    matches!(entity_type, "match_history" | "prefetch_match")
        || matches!(
            endpoint,
            "getmatchhistory" | "getplayermatchhistory" | "getplayermatchhistoryafterdatetime"
        )
        || raw.as_array().is_some_and(|rows| {
            !rows.is_empty()
                && rows.iter().all(|row| {
                    matches!(
                        text(row, "source").to_ascii_lowercase().as_str(),
                        "prefetch" | "match_history" | "history_observation" | "legacy_prefetch"
                    )
                })
        })
}

async fn persist_match(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    let payload = CanonicalMatchPayload::from_buffer_rows(row.raw_data.clone())?;
    if !row.entity_id.is_empty() && row.entity_id.parse::<i64>().ok() != Some(payload.match_id) {
        return Err(RawBufferError::Invalid {
            row_id: row.id,
            message: "entity_id does not match payload match_id".to_owned(),
        });
    }
    let finalized = MatchFactRepository::new(database.clone())
        .finalize(&payload, "rust_raw_buffer")
        .await?;
    if finalized.population == super::match_lifecycle::MatchPopulation::Ranked {
        RankedProjectionRepository::new(database.clone())
            .project_match(payload.match_id)
            .await
            .map_err(|error| RawBufferError::Invalid {
                row_id: row.id,
                message: error.to_string(),
            })?;
    } else {
        CasualMechanicsRepository::new(database.clone())
            .project_all_for_match(payload.match_id)
            .await
            .map_err(|error| RawBufferError::Invalid {
                row_id: row.id,
                message: error.to_string(),
            })?;
    }
    Ok(())
}

async fn persist_match_history(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    let raw = clean_json_text(&row.raw_data)?;
    database
        .query_json(
            r#"
            WITH payload AS (
              SELECT value AS raw
              FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof($1::text::jsonb)='array'
                  THEN $1::text::jsonb ELSE jsonb_build_array($1::text::jsonb) END
              )
              WHERE btrim(COALESCE(value->>'ret_msg',''))=''
            ), normalized AS (
              SELECT
                COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint) match_id,
                COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint,
                         NULLIF(raw->>'playerIdActive','')::bigint) player_id,
                COALESCE(NULLIF(raw->>'entry_datetime',''),NULLIF(raw->>'Match_Time',''),
                         NULLIF(raw->>'Entry_Datetime','')) entry_datetime,
                COALESCE(NULLIF(raw->>'queue_id','')::int,NULLIF(raw->>'match_queue_id','')::int,
                         NULLIF(raw->>'Match_Queue_Id','')::int) queue_id,
                COALESCE(NULLIF(raw->>'region',''),NULLIF(raw->>'Region','')) region,
                COALESCE(NULLIF(raw->>'map',''),NULLIF(raw->>'Map_Game','')) map,
                COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int) champion_id,
                COALESCE(NULLIF(raw->>'champion_name',''),NULLIF(raw->>'Champion','')) champion_name,
                COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'SkinId','')::int) skin_id,
                COALESCE(NULLIF(raw->>'skin_name',''),NULLIF(raw->>'Skin','')) skin_name,
                COALESCE(NULLIF(raw->>'win_status',''),NULLIF(raw->>'Win_Status','')) win_status,
                COALESCE(NULLIF(raw->>'kills','')::int,NULLIF(raw->>'Kills','')::int,0) kills,
                COALESCE(NULLIF(raw->>'deaths','')::int,NULLIF(raw->>'Deaths','')::int,0) deaths,
                COALESCE(NULLIF(raw->>'assists','')::int,NULLIF(raw->>'Assists','')::int,0) assists,
                COALESCE(NULLIF(raw->>'damage','')::int,NULLIF(raw->>'damage_done_physical','')::int,
                         NULLIF(raw->>'Damage','')::int,0) damage,
                COALESCE(NULLIF(raw->>'healing','')::int,NULLIF(raw->>'Healing','')::int,0) healing,
                COALESCE(NULLIF(raw->>'gold_earned','')::int,NULLIF(raw->>'Gold_Earned','')::int,0) gold_earned,
                COALESCE(NULLIF(raw->>'time_in_match','')::int,NULLIF(raw->>'match_duration','')::int,
                         NULLIF(raw->>'Time_In_Match_Seconds','')::int,0) time_in_match,
                COALESCE(NULLIF(raw->>'task_force','')::smallint,NULLIF(raw->>'TaskForce','')::smallint,0) task_force,
                COALESCE(NULLIF(raw->>'league_tier','')::smallint,NULLIF(raw->>'League_Tier','')::smallint,0) league_tier,
                raw
              FROM payload
            )
            INSERT INTO player_match_history_entries(
              match_id,player_id,fetched_player_id,entry_datetime,queue_id,region,map,
              champion_id,champion_name,skin_id,skin_name,win_status,kills,deaths,assists,
              damage,healing,gold_earned,time_in_match,task_force,league_tier,source,
              raw_data,normalized_data,observed_at,expires_at
            )
            SELECT match_id,player_id,player_id,NULLIF(entry_datetime,'')::timestamptz,queue_id,
              region,map,champion_id,champion_name,skin_id,skin_name,win_status,kills,deaths,
              assists,damage,healing,gold_earned,time_in_match,task_force,league_tier,
              COALESCE(NULLIF($2,''),'getmatchhistory'),raw,raw||jsonb_build_object('source','match_history'),
              now(),now()+INTERVAL '6 hours'
            FROM normalized WHERE match_id>0 AND player_id>0
            ON CONFLICT(match_id,player_id) DO UPDATE SET
              fetched_player_id=COALESCE(EXCLUDED.fetched_player_id,player_match_history_entries.fetched_player_id),
              entry_datetime=COALESCE(EXCLUDED.entry_datetime,player_match_history_entries.entry_datetime),
              queue_id=COALESCE(EXCLUDED.queue_id,player_match_history_entries.queue_id),
              region=COALESCE(EXCLUDED.region,player_match_history_entries.region),
              map=COALESCE(NULLIF(EXCLUDED.map,''),player_match_history_entries.map),
              champion_id=COALESCE(EXCLUDED.champion_id,player_match_history_entries.champion_id),
              champion_name=COALESCE(NULLIF(EXCLUDED.champion_name,''),player_match_history_entries.champion_name),
              skin_id=COALESCE(EXCLUDED.skin_id,player_match_history_entries.skin_id),
              skin_name=COALESCE(NULLIF(EXCLUDED.skin_name,''),player_match_history_entries.skin_name),
              win_status=COALESCE(NULLIF(EXCLUDED.win_status,''),player_match_history_entries.win_status),
              kills=EXCLUDED.kills,deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,
              damage=EXCLUDED.damage,healing=EXCLUDED.healing,gold_earned=EXCLUDED.gold_earned,
              time_in_match=EXCLUDED.time_in_match,task_force=EXCLUDED.task_force,
              league_tier=EXCLUDED.league_tier,source=EXCLUDED.source,raw_data=EXCLUDED.raw_data,
              normalized_data=EXCLUDED.normalized_data,observed_at=now(),expires_at=EXCLUDED.expires_at
            "#,
            &[&raw, &row.endpoint],
        )
        .await?;
    Ok(())
}

async fn persist_loadouts(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    let raw = clean_json_text(&row.raw_data)?;
    database
        .query_json(
            r#"
            WITH rows AS(
              SELECT value raw FROM jsonb_array_elements($1::text::jsonb)
              WHERE btrim(COALESCE(value->>'ret_msg',''))=''
            ), decks AS(
              SELECT
                COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint) player_id,
                COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int) champion_id,
                COALESCE(NULLIF(raw->>'deck_id','')::bigint,NULLIF(raw->>'DeckId','')::bigint) deck_id,
                COALESCE(NULLIF(raw->>'deck_name',''),NULLIF(raw->>'DeckName',''),
                         NULLIF(raw->>'loadout_name','')) deck_name,
                raw
              FROM rows
            ), upserted AS(
              INSERT INTO player_loadouts(
                player_id,champion_id,deck_id,deck_key,loadout_name,card_ids,card_levels,talent_id,fetched_at,updated_at
              )
              SELECT player_id,champion_id,deck_id,
                CASE WHEN COALESCE(deck_id,0)>0 THEN 'id:'||deck_id::text
                  ELSE 'legacy:'||champion_id::text||':'||left(lower(regexp_replace(deck_name,'\s+',' ','g')),80) END,
                deck_name,
                ARRAY(SELECT COALESCE(NULLIF(card->>'item_id','')::int,NULLIF(card->>'ItemId','')::int)
                      FROM jsonb_array_elements(COALESCE(raw->'cards',raw->'LoadoutItems','[]'::jsonb)) card),
                ARRAY(SELECT COALESCE(NULLIF(card->>'points','')::int,NULLIF(card->>'Points','')::int,0)
                      FROM jsonb_array_elements(COALESCE(raw->'cards',raw->'LoadoutItems','[]'::jsonb)) card),
                NULL,now(),now()
              FROM decks WHERE player_id>0 AND champion_id>0 AND COALESCE(deck_name,'')<>''
              ON CONFLICT(player_id,deck_key) DO UPDATE SET champion_id=EXCLUDED.champion_id,
                deck_id=EXCLUDED.deck_id,loadout_name=EXCLUDED.loadout_name,
                card_ids=EXCLUDED.card_ids,card_levels=EXCLUDED.card_levels,fetched_at=now(),updated_at=now()
              RETURNING player_id
            )
            INSERT INTO player_loadout_fetches(player_id,fetched_at)
            SELECT DISTINCT player_id,now() FROM upserted
            ON CONFLICT(player_id) DO UPDATE SET fetched_at=now()
            "#,
            &[&raw],
        )
        .await?;
    Ok(())
}

async fn persist_players(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements(CASE WHEN jsonb_typeof($1::text::jsonb)='array' THEN $1::text::jsonb ELSE jsonb_build_array($1::text::jsonb) END))
        INSERT INTO players(id,name,level,region,platform,portal_id,portal_user_id,kbm_tier,kbm_points,first_seen,last_seen,last_updated,name_source)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'Id','')::bigint),
          COALESCE(NULLIF(raw->>'player_name',''),NULLIF(raw->>'Name',''),'Unknown'),
          COALESCE(NULLIF(raw->>'account_level','')::int,NULLIF(raw->>'Level','')::int,0),
          COALESCE(NULLIF(raw->>'region',''),NULLIF(raw->>'Region',''),'Unknown'),
          COALESCE(NULLIF(raw->>'platform',''),NULLIF(raw->>'Platform',''),'Unknown'),
          COALESCE(NULLIF(raw->>'portal_id','')::int,NULLIF(raw->>'PortalId','')::int),
          COALESCE(NULLIF(raw->>'portal_user_id',''),NULLIF(raw->>'PortalUserId','')),
          COALESCE(NULLIF(raw->>'kbm_tier','')::int,NULLIF(raw->>'RankedKBM_Tier','')::int,0),
          COALESCE(NULLIF(raw->>'kbm_points','')::int,NULLIF(raw->>'RankedKBM_Points','')::int,0),
          now(),now(),now(),'hirez'
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'Id','')::bigint)>0
        ON CONFLICT(id) DO UPDATE SET name=CASE WHEN EXCLUDED.name<>'Unknown' THEN EXCLUDED.name ELSE players.name END,
          level=GREATEST(players.level,EXCLUDED.level),region=COALESCE(NULLIF(EXCLUDED.region,'Unknown'),players.region),
          platform=COALESCE(NULLIF(EXCLUDED.platform,'Unknown'),players.platform),portal_id=COALESCE(EXCLUDED.portal_id,players.portal_id),
          portal_user_id=COALESCE(EXCLUDED.portal_user_id,players.portal_user_id),kbm_tier=EXCLUDED.kbm_tier,
          kbm_points=EXCLUDED.kbm_points,last_seen=now(),last_updated=now()"#,
    )
    .await
}

async fn persist_champions(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO champions(id,name,title,roles,lore,health,speed,icon_url,last_updated)
        SELECT COALESCE(NULLIF(raw->>'id','')::int,NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int),
          COALESCE(NULLIF(raw->>'name',''),NULLIF(raw->>'Name','')),
          COALESCE(raw->>'title',raw->>'Title'),COALESCE(raw->>'roles',raw->>'Roles'),
          COALESCE(raw->>'lore',raw->>'Lore'),
          COALESCE(NULLIF(raw->>'health','')::int,NULLIF(raw->>'Health','')::int,0),
          COALESCE(NULLIF(raw->>'speed','')::int,NULLIF(raw->>'Speed','')::int,0),
          COALESCE(raw->>'icon_url',raw->>'ChampionIcon_URL'),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'id','')::int,NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int)>0
        ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,title=EXCLUDED.title,roles=EXCLUDED.roles,
          lore=EXCLUDED.lore,health=EXCLUDED.health,speed=EXCLUDED.speed,icon_url=EXCLUDED.icon_url,last_updated=now()"#,
    )
    .await
}

async fn persist_items(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO items(item_id,item_name,description,price,icon_url,item_type,last_updated)
        SELECT COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'ItemId','')::int),
          COALESCE(NULLIF(raw->>'item_name',''),NULLIF(raw->>'DeviceName','')),
          COALESCE(raw->>'description',raw->>'Description'),
          COALESCE(NULLIF(raw->>'price','')::int,NULLIF(raw->>'Price','')::int,0),
          COALESCE(raw->>'icon_url',raw->>'itemIcon_URL'),
          COALESCE(raw->>'item_type',raw->>'Type'),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'ItemId','')::int)>0
        ON CONFLICT(item_id) DO UPDATE SET item_name=EXCLUDED.item_name,description=EXCLUDED.description,
          price=EXCLUDED.price,icon_url=EXCLUDED.icon_url,item_type=EXCLUDED.item_type,last_updated=now()"#,
    )
    .await
}

async fn persist_player_status(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO player_status(player_id,status,status_string,match_id,queue_id,last_updated)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint),
          COALESCE(NULLIF(raw->>'status','')::int,NULLIF(raw->>'status_id','')::int,0),
          COALESCE(raw->>'status_string',raw->>'status'),
          COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint),
          COALESCE(NULLIF(raw->>'queue_id','')::int,NULLIF(raw->>'match_queue_id','')::int),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint)>0
        ON CONFLICT(player_id) DO UPDATE SET status=EXCLUDED.status,status_string=EXCLUDED.status_string,
          match_id=EXCLUDED.match_id,queue_id=EXCLUDED.queue_id,last_updated=now()"#,
    )
    .await
}

async fn persist_player_champions(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO player_champions(player_id,champion_id,champion_name,xp,ownership_type,wins,losses,kills,deaths,assists,minutes_played,stats_populated,last_updated)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::int,NULLIF(raw->>'playerId','')::int),
          COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'champion_id','')::int),
          COALESCE(raw->>'champion_name',raw->>'champion'),
          COALESCE(NULLIF(raw->>'xp','')::bigint,NULLIF(raw->>'Worshippers','')::bigint,0),
          COALESCE(raw->>'ownership_type',raw->>'Ownership'),COALESCE(NULLIF(raw->>'wins','')::int,0),
          COALESCE(NULLIF(raw->>'losses','')::int,0),COALESCE(NULLIF(raw->>'kills','')::int,0),
          COALESCE(NULLIF(raw->>'deaths','')::int,0),COALESCE(NULLIF(raw->>'assists','')::int,0),
          COALESCE(NULLIF(raw->>'minutes_played','')::int,0),true,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::int,NULLIF(raw->>'playerId','')::int)>0
        ON CONFLICT(player_id,champion_id) DO UPDATE SET champion_name=EXCLUDED.champion_name,xp=EXCLUDED.xp,
          ownership_type=EXCLUDED.ownership_type,wins=EXCLUDED.wins,losses=EXCLUDED.losses,kills=EXCLUDED.kills,
          deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,minutes_played=EXCLUDED.minutes_played,
          stats_populated=true,last_updated=now()"#,
    )
    .await
}

async fn persist_player_achievements(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO player_achievements(player_id,achievements,updated_at)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF($2,'')::bigint),raw,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF($2,'')::bigint)>0
        ON CONFLICT(player_id) DO UPDATE SET achievements=EXCLUDED.achievements,updated_at=now()"#,
    )
    .await
}

async fn persist_champion_skins(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO skins(skin_id,skin_name,champion_id,champion_name,rarity,icon_url,last_updated)
        SELECT COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'skin_id1','')::int),
          COALESCE(raw->>'skin_name',raw->>'skin_name1'),
          COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'champion_id1','')::int),
          COALESCE(raw->>'champion_name',raw->>'champion_name1'),COALESCE(raw->>'rarity',raw->>'rarity'),
          COALESCE(raw->>'icon_url',raw->>'skinIcon_URL'),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'skin_id1','')::int)>0
        ON CONFLICT(skin_id) DO UPDATE SET skin_name=EXCLUDED.skin_name,champion_id=EXCLUDED.champion_id,
          champion_name=EXCLUDED.champion_name,rarity=EXCLUDED.rarity,icon_url=EXCLUDED.icon_url,last_updated=now()"#,
    )
    .await
}

async fn persist_live_match(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO live_match_players(match_id,player_id,player_name,champion_id,champion_name,team,queue_id,region,observed_at,raw_data)
        SELECT COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint),
          COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint),
          COALESCE(raw->>'player_name',raw->>'playerName'),
          COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int),
          COALESCE(raw->>'champion_name',raw->>'ChampionName'),
          COALESCE(NULLIF(raw->>'team','')::smallint,NULLIF(raw->>'task_force','')::smallint,0),
          COALESCE(NULLIF(raw->>'queue_id','')::int,NULLIF(raw->>'match_queue_id','')::int),
          COALESCE(raw->>'region',raw->>'Region'),now(),raw
        FROM rows WHERE COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint)>0
        ON CONFLICT(match_id,player_id) DO UPDATE SET player_name=EXCLUDED.player_name,
          champion_id=EXCLUDED.champion_id,champion_name=EXCLUDED.champion_name,team=EXCLUDED.team,
          queue_id=EXCLUDED.queue_id,region=EXCLUDED.region,observed_at=now(),raw_data=EXCLUDED.raw_data"#,
    )
    .await
}

async fn persist_leaderboard(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO leaderboard_entries(player_id,player_name,rank,ranked_points,wins,losses,queue_id,updated_at)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint),
          COALESCE(raw->>'player_name',raw->>'Name'),
          COALESCE(NULLIF(raw->>'rank','')::int,NULLIF(raw->>'Rank','')::int,0),
          COALESCE(NULLIF(raw->>'player_ranking','')::int,NULLIF(raw->>'Rank_Stat','')::int,0),
          COALESCE(NULLIF(raw->>'wins','')::int,NULLIF(raw->>'Wins','')::int,0),
          COALESCE(NULLIF(raw->>'losses','')::int,NULLIF(raw->>'Losses','')::int,0),
          COALESCE(NULLIF(raw->>'queue_id','')::int,486),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint)>0
        ON CONFLICT(player_id,queue_id) DO UPDATE SET player_name=EXCLUDED.player_name,rank=EXCLUDED.rank,
          ranked_points=EXCLUDED.ranked_points,wins=EXCLUDED.wins,losses=EXCLUDED.losses,updated_at=now()"#,
    )
    .await
}

async fn persist_esports(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO esports_leagues(league_id,league_name,league_description,league_image_url,league_start_date,league_end_date,updated_at)
        SELECT COALESCE(NULLIF(raw->>'league_id','')::int,NULLIF(raw->>'LeagueId','')::int),
          COALESCE(raw->>'league_name',raw->>'Name'),COALESCE(raw->>'league_description',raw->>'Description'),
          COALESCE(raw->>'league_image_url',raw->>'Image_URL'),
          NULLIF(COALESCE(raw->>'league_start_date',raw->>'StartDate'),'')::timestamptz,
          NULLIF(COALESCE(raw->>'league_end_date',raw->>'EndDate'),'')::timestamptz,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'league_id','')::int,NULLIF(raw->>'LeagueId','')::int)>0
        ON CONFLICT(league_id) DO UPDATE SET league_name=EXCLUDED.league_name,
          league_description=EXCLUDED.league_description,league_image_url=EXCLUDED.league_image_url,
          league_start_date=EXCLUDED.league_start_date,league_end_date=EXCLUDED.league_end_date,updated_at=now()"#,
    )
    .await
}

async fn persist_bounty_items(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO bounty_items(item_id,item_name,champion_name,price,active,raw_data,updated_at)
        SELECT COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'bounty_item_id','')::int),
          COALESCE(raw->>'item_name',raw->>'Name'),COALESCE(raw->>'champion_name',raw->>'ChampionName'),
          COALESCE(NULLIF(raw->>'price','')::int,0),COALESCE(NULLIF(raw->>'active','')::boolean,true),raw,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'bounty_item_id','')::int)>0
        ON CONFLICT(item_id) DO UPDATE SET item_name=EXCLUDED.item_name,champion_name=EXCLUDED.champion_name,
          price=EXCLUDED.price,active=EXCLUDED.active,raw_data=EXCLUDED.raw_data,updated_at=now()"#,
    )
    .await
}

async fn run_json_upsert(
    database: &Database,
    row: &ClaimedRow,
    sql: &str,
) -> Result<(), RawBufferError> {
    let raw = clean_json_text(&row.raw_data)?;
    database.query_json(sql, &[&raw, &row.entity_id]).await?;
    Ok(())
}

fn clean_json_text(value: &Value) -> Result<String, RawBufferError> {
    serde_json::to_string(value)
        .map(|value| value.replace('\0', ""))
        .map_err(|error| RawBufferError::Invalid {
            row_id: 0,
            message: error.to_string(),
        })
}

async fn mark_processed(database: &Database, row_id: i64) -> Result<(), DatabaseError> {
    database
        .query_json(
            "UPDATE raw_ingest_buffer SET status='processed',processed_at=now(),error_message=NULL WHERE id=$1",
            &[&row_id],
        )
        .await?;
    Ok(())
}

async fn mark_failed_or_deferred(
    database: &Database,
    row_id: i64,
    retry: i32,
    terminal: bool,
    message: &str,
) -> Result<(), DatabaseError> {
    database
        .query_json(
            "UPDATE raw_ingest_buffer SET status=CASE WHEN $3 THEN 'failed' ELSE 'pending' END,\
               retry_count=$2,error_message=left($4,4000),processed_at=CASE WHEN $3 THEN now() ELSE NULL END,\
               available_at=CASE WHEN $3 THEN available_at ELSE now()+make_interval(secs=>LEAST(900,30*(1<<LEAST($2,5)))) END \
             WHERE id=$1",
            &[&row_id, &retry, &terminal, &message],
        )
        .await?;
    Ok(())
}

fn integer(value: &Value, key: &str) -> Option<i64> {
    value
        .get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}
