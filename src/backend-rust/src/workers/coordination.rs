use std::time::Duration;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;

pub const SCHEDULER_KEYS: [&str; 6] = [
    "ranked_tracker",
    "auto_ingester",
    "baseline_tracker",
    "derived_projection_tracker",
    "hourly_gap_checker",
    "tier_stats",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchedulerOwnership {
    pub scheduler_key: String,
    pub owner_id: String,
    pub engine: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchedulerAssignment {
    pub scheduler_key: String,
    pub desired_engine: String,
    pub generation: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobLease {
    pub job_key: String,
    pub scheduler_key: String,
    pub owner_id: String,
}

#[derive(Clone)]
pub struct WorkerCoordinationRepository {
    database: Database,
}

impl WorkerCoordinationRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn acquire_scheduler_owner(
        &self,
        scheduler_key: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, DatabaseError> {
        if !SCHEDULER_KEYS.contains(&scheduler_key) {
            return Ok(false);
        }
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let client = self.database.connection().await?;
        Ok(client
            .query_opt(
                r#"
                INSERT INTO worker_scheduler_ownership (
                  scheduler_key, owner_id, engine, lease_until,
                  acquired_at, heartbeat_at, updated_at
                )
                SELECT
                  $1::varchar(64), $2::varchar(120), 'rust',
                  now() + ($3::int * interval '1 second'),
                  now(), now(), now()
                WHERE EXISTS (
                  SELECT 1
                  FROM worker_scheduler_assignments assignment
                  WHERE assignment.scheduler_key = $1::varchar(64)
                    AND assignment.desired_engine = 'rust'
                )
                ON CONFLICT (scheduler_key) DO UPDATE SET
                  owner_id = EXCLUDED.owner_id,
                  engine = EXCLUDED.engine,
                  lease_until = EXCLUDED.lease_until,
                  acquired_at = CASE
                    WHEN worker_scheduler_ownership.owner_id = EXCLUDED.owner_id
                      THEN worker_scheduler_ownership.acquired_at
                    ELSE now()
                  END,
                  heartbeat_at = now(),
                  updated_at = now()
                WHERE (
                    worker_scheduler_ownership.lease_until <= now()
                    OR worker_scheduler_ownership.owner_id = EXCLUDED.owner_id
                  )
                  AND EXISTS (
                    SELECT 1
                    FROM worker_scheduler_assignments assignment
                    WHERE assignment.scheduler_key = EXCLUDED.scheduler_key
                      AND assignment.desired_engine = 'rust'
                  )
                RETURNING scheduler_key
                "#,
                &[&scheduler_key, &owner_id, &lease_seconds],
            )
            .await?
            .is_some())
    }

    pub async fn heartbeat_scheduler_owner(
        &self,
        scheduler_key: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, DatabaseError> {
        if !SCHEDULER_KEYS.contains(&scheduler_key) {
            return Ok(false);
        }
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let client = self.database.connection().await?;
        Ok(client
            .execute(
                r#"
                UPDATE worker_scheduler_ownership
                SET lease_until = now() + ($3::int * interval '1 second'),
                    heartbeat_at = now(),
                    updated_at = now()
                WHERE scheduler_key = $1
                  AND owner_id = $2::varchar(120)
                  AND engine = 'rust'
                  AND lease_until > now()
                  AND EXISTS (
                    SELECT 1
                    FROM worker_scheduler_assignments assignment
                    WHERE assignment.scheduler_key = $1::varchar(64)
                      AND assignment.desired_engine = 'rust'
                  )
                "#,
                &[&scheduler_key, &owner_id, &lease_seconds],
            )
            .await?
            == 1)
    }

    pub async fn release_scheduler_owner(
        &self,
        scheduler_key: &str,
        owner_id: &str,
    ) -> Result<bool, DatabaseError> {
        if !SCHEDULER_KEYS.contains(&scheduler_key) {
            return Ok(false);
        }
        let client = self.database.connection().await?;
        Ok(client
            .execute(
                r#"
                DELETE FROM worker_scheduler_ownership
                WHERE scheduler_key = $1
                  AND owner_id = $2
                  AND engine = 'rust'
                "#,
                &[&scheduler_key, &owner_id],
            )
            .await?
            == 1)
    }

    /// Atomic final-wave helper. Staged rollout uses
    /// `acquire_scheduler_owner` for one independently verified domain.
    pub async fn acquire_all_scheduler_owners(
        &self,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, DatabaseError> {
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let mut acquired = 0_usize;
        for scheduler_key in SCHEDULER_KEYS {
            let row = transaction
                .query_opt(
                    r#"
                    INSERT INTO worker_scheduler_ownership (
                      scheduler_key, owner_id, engine, lease_until,
                      acquired_at, heartbeat_at, updated_at
                    )
                    SELECT
                      $1::varchar(64), $2::varchar(120), 'rust',
                      now() + ($3::int * interval '1 second'),
                      now(), now(), now()
                    WHERE EXISTS (
                      SELECT 1
                      FROM worker_scheduler_assignments assignment
                      WHERE assignment.scheduler_key = $1::varchar(64)
                        AND assignment.desired_engine = 'rust'
                    )
                    ON CONFLICT (scheduler_key) DO UPDATE SET
                      owner_id = EXCLUDED.owner_id,
                      engine = EXCLUDED.engine,
                      lease_until = EXCLUDED.lease_until,
                      acquired_at = CASE
                        WHEN worker_scheduler_ownership.owner_id = EXCLUDED.owner_id
                          THEN worker_scheduler_ownership.acquired_at
                        ELSE now()
                      END,
                      heartbeat_at = now(),
                      updated_at = now()
                    WHERE (
                        worker_scheduler_ownership.lease_until <= now()
                        OR worker_scheduler_ownership.owner_id = EXCLUDED.owner_id
                      )
                      AND EXISTS (
                        SELECT 1
                        FROM worker_scheduler_assignments assignment
                        WHERE assignment.scheduler_key = EXCLUDED.scheduler_key
                          AND assignment.desired_engine = 'rust'
                      )
                    RETURNING scheduler_key
                    "#,
                    &[&scheduler_key, &owner_id, &lease_seconds],
                )
                .await?;
            acquired += usize::from(row.is_some());
        }
        if acquired != SCHEDULER_KEYS.len() {
            transaction.rollback().await?;
            return Ok(false);
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn heartbeat_scheduler_owners(
        &self,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, DatabaseError> {
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let client = self.database.connection().await?;
        let updated = client
            .execute(
                r#"
                UPDATE worker_scheduler_ownership
                SET lease_until = now() + ($2::int * interval '1 second'),
                    heartbeat_at = now(),
                    updated_at = now()
                WHERE owner_id = $1
                  AND engine = 'rust'
                  AND scheduler_key = ANY($3::text[])
                  AND lease_until > now()
                  AND EXISTS (
                    SELECT 1
                    FROM worker_scheduler_assignments assignment
                    WHERE assignment.scheduler_key =
                      worker_scheduler_ownership.scheduler_key
                      AND assignment.desired_engine = 'rust'
                  )
                "#,
                &[&owner_id, &lease_seconds, &SCHEDULER_KEYS.as_slice()],
            )
            .await?;
        Ok(updated == SCHEDULER_KEYS.len() as u64)
    }

    pub async fn release_scheduler_owners(&self, owner_id: &str) -> Result<u64, DatabaseError> {
        let client = self.database.connection().await?;
        client
            .execute(
                r#"
                DELETE FROM worker_scheduler_ownership
                WHERE owner_id = $1
                  AND engine = 'rust'
                  AND scheduler_key = ANY($2::text[])
                "#,
                &[&owner_id, &SCHEDULER_KEYS.as_slice()],
            )
            .await
            .map_err(DatabaseError::from)
    }

    pub async fn scheduler_ownership(&self) -> Result<Vec<SchedulerOwnership>, DatabaseError> {
        let client = self.database.connection().await?;
        let rows = client
            .query(
                r#"
                SELECT scheduler_key, owner_id, engine
                FROM worker_scheduler_ownership
                WHERE lease_until > now()
                ORDER BY scheduler_key
                "#,
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| SchedulerOwnership {
                scheduler_key: row.get("scheduler_key"),
                owner_id: row.get("owner_id"),
                engine: row.get("engine"),
            })
            .collect())
    }

    pub async fn scheduler_assignments(&self) -> Result<Vec<SchedulerAssignment>, DatabaseError> {
        let client = self.database.connection().await?;
        let rows = client
            .query(
                r#"
                SELECT scheduler_key, desired_engine, generation
                FROM worker_scheduler_assignments
                ORDER BY scheduler_key
                "#,
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| SchedulerAssignment {
                scheduler_key: row.get("scheduler_key"),
                desired_engine: row.get("desired_engine"),
                generation: row.get("generation"),
            })
            .collect())
    }

    pub async fn acquire_job(
        &self,
        job_key: &str,
        scheduler_key: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<Option<JobLease>, DatabaseError> {
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let client = self.database.connection().await?;
        let row = client
            .query_opt(
                r#"
                INSERT INTO worker_job_leases (
                  job_key, scheduler_key, owner_id, lease_until,
                  started_at, heartbeat_at, attempt, updated_at
                )
                SELECT
                  $1::varchar(100), $2::varchar(64), $3::varchar(120),
                  now() + ($4::int * interval '1 second'),
                  now(), now(), 1, now()
                WHERE EXISTS (
                  SELECT 1
                  FROM worker_scheduler_ownership ownership
                  WHERE ownership.scheduler_key = $2::varchar(64)
                    AND ownership.owner_id = $3::varchar(120)
                    AND ownership.engine = 'rust'
                    AND ownership.lease_until > now()
                )
                ON CONFLICT (job_key) DO UPDATE SET
                  scheduler_key = EXCLUDED.scheduler_key,
                  owner_id = EXCLUDED.owner_id,
                  lease_until = EXCLUDED.lease_until,
                  started_at = now(),
                  heartbeat_at = now(),
                  attempt = worker_job_leases.attempt + 1,
                  updated_at = now()
                WHERE worker_job_leases.lease_until <= now()
                  AND EXISTS (
                    SELECT 1
                    FROM worker_scheduler_ownership ownership
                    WHERE ownership.scheduler_key = EXCLUDED.scheduler_key
                      AND ownership.owner_id = EXCLUDED.owner_id
                      AND ownership.engine = 'rust'
                      AND ownership.lease_until > now()
                  )
                RETURNING job_key
                "#,
                &[&job_key, &scheduler_key, &owner_id, &lease_seconds],
            )
            .await?;
        Ok(row.map(|_| JobLease {
            job_key: job_key.to_owned(),
            scheduler_key: scheduler_key.to_owned(),
            owner_id: owner_id.to_owned(),
        }))
    }

    pub async fn release_job(&self, lease: &JobLease) -> Result<bool, DatabaseError> {
        let client = self.database.connection().await?;
        Ok(client
            .execute(
                r#"
                DELETE FROM worker_job_leases
                WHERE job_key = $1
                  AND scheduler_key = $2
                  AND owner_id = $3
                "#,
                &[&lease.job_key, &lease.scheduler_key, &lease.owner_id],
            )
            .await?
            == 1)
    }

    pub async fn start_run(&self, lease: &JobLease, trigger: &str) -> Result<i64, DatabaseError> {
        let client = self.database.connection().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO worker_job_run_log (
                  job_key, scheduler_key, owner_id, trigger, status
                )
                VALUES ($1, $2, $3, $4, 'running')
                RETURNING run_id
                "#,
                &[
                    &lease.job_key,
                    &lease.scheduler_key,
                    &lease.owner_id,
                    &trigger,
                ],
            )
            .await?;
        Ok(row.get("run_id"))
    }

    pub async fn finish_run(
        &self,
        run_id: i64,
        status: &str,
        result: Option<&serde_json::Value>,
        error_message: Option<&str>,
    ) -> Result<bool, DatabaseError> {
        let client = self.database.connection().await?;
        Ok(client
            .execute(
                r#"
                UPDATE worker_job_run_log
                SET status = $2,
                    completed_at = now(),
                    duration_ms = GREATEST(
                      0,
                      (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint
                    ),
                    result = $3,
                    error_message = $4
                WHERE run_id = $1
                  AND status = 'running'
                "#,
                &[&run_id, &status, &result, &error_message],
            )
            .await?
            == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paladinscat_core::config::BackendConfig;

    #[test]
    fn scheduler_inventory_is_complete_and_unique() {
        let unique = SCHEDULER_KEYS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), 6);
        assert_eq!(unique.len(), SCHEDULER_KEYS.len());
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with migrations 110-111"]
    async fn live_staged_ownership_is_exclusive_and_job_leases_do_not_overlap() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "worker-coordination-integration").expect("database");
        let client = database.connection().await.expect("fixture connection");
        client
            .execute(
                "DELETE FROM worker_job_leases WHERE owner_id LIKE 'coordination-test-%'",
                &[],
            )
            .await
            .expect("clean fixture job leases");
        client
            .execute(
                "DELETE FROM worker_scheduler_ownership \
                 WHERE owner_id LIKE 'coordination-test-%'",
                &[],
            )
            .await
            .expect("clean fixture scheduler ownership");
        drop(client);
        let repository = WorkerCoordinationRepository::new(database);
        let suffix = uuid::Uuid::new_v4();
        let owner_a = format!("coordination-test-a-{suffix}");
        let owner_b = format!("coordination-test-b-{suffix}");
        let job_key = format!("coordination-test-job-{suffix}");
        let lease_duration = Duration::from_secs(60);

        assert!(
            !repository
                .acquire_scheduler_owner("auto_ingester", &owner_a, lease_duration)
                .await
                .expect("Rust cannot acquire a TypeScript-assigned stage")
        );
        let client = repository
            .database
            .connection()
            .await
            .expect("assignment fixture connection");
        client
            .execute(
                r#"
                UPDATE worker_scheduler_assignments
                SET desired_engine = 'rust',
                    generation = generation + 1,
                    updated_by = 'coordination-integration-test',
                    updated_at = now()
                WHERE scheduler_key = ANY($1::text[])
                "#,
                &[&vec!["auto_ingester", "ranked_tracker"]],
            )
            .await
            .expect("assign staged Rust domains");
        drop(client);

        assert!(
            repository
                .acquire_scheduler_owner("auto_ingester", &owner_a, lease_duration)
                .await
                .expect("owner A staged acquisition")
        );
        assert!(
            !repository
                .acquire_scheduler_owner("auto_ingester", &owner_b, lease_duration)
                .await
                .expect("same-stage owner B exclusion")
        );
        assert!(
            repository
                .acquire_scheduler_owner("ranked_tracker", &owner_b, lease_duration)
                .await
                .expect("independent stage owner B acquisition")
        );
        assert!(
            repository
                .heartbeat_scheduler_owner("auto_ingester", &owner_a, lease_duration)
                .await
                .expect("owner A heartbeat")
        );

        let job = repository
            .acquire_job(&job_key, "auto_ingester", &owner_a, lease_duration)
            .await
            .expect("owner A job acquisition")
            .expect("owner A owns job");
        assert!(
            repository
                .acquire_job(&job_key, "auto_ingester", &owner_a, lease_duration,)
                .await
                .expect("same-owner overlap check")
                .is_none()
        );
        assert!(
            repository
                .acquire_job(&job_key, "auto_ingester", &owner_b, lease_duration,)
                .await
                .expect("non-owner job check")
                .is_none()
        );
        assert!(repository.release_job(&job).await.expect("release job"));
        assert!(
            repository
                .release_scheduler_owner("auto_ingester", &owner_a)
                .await
                .expect("release owner A")
        );
        assert!(
            repository
                .acquire_scheduler_owner("auto_ingester", &owner_b, lease_duration)
                .await
                .expect("owner B staged takeover")
        );
        assert!(
            repository
                .release_scheduler_owner("auto_ingester", &owner_b)
                .await
                .expect("release owner B auto ingest")
        );
        assert!(
            repository
                .release_scheduler_owner("ranked_tracker", &owner_b)
                .await
                .expect("release owner B ranked")
        );

        let client = repository
            .database
            .connection()
            .await
            .expect("final-wave assignment fixture");
        client
            .execute(
                r#"
                UPDATE worker_scheduler_assignments
                SET desired_engine = 'rust',
                    generation = generation + 1,
                    updated_by = 'coordination-integration-test',
                    updated_at = now()
                "#,
                &[],
            )
            .await
            .expect("assign final Rust wave");
        drop(client);
        assert!(
            repository
                .acquire_all_scheduler_owners(&owner_a, lease_duration)
                .await
                .expect("final-wave all-domain acquisition")
        );
        assert!(
            !repository
                .acquire_all_scheduler_owners(&owner_b, lease_duration)
                .await
                .expect("final-wave exclusion")
        );
        assert_eq!(
            repository
                .release_scheduler_owners(&owner_a)
                .await
                .expect("release final-wave owner"),
            SCHEDULER_KEYS.len() as u64
        );

        let client = repository
            .database
            .connection()
            .await
            .expect("assignment cleanup connection");
        client
            .execute(
                r#"
                UPDATE worker_scheduler_assignments
                SET desired_engine = 'typescript',
                    generation = generation + 1,
                    updated_by = 'coordination-integration-cleanup',
                    updated_at = now()
                "#,
                &[],
            )
            .await
            .expect("restore TypeScript assignments");
    }
}
