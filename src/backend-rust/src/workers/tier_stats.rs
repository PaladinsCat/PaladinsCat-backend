use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;

const UPSERT_TIER_STATS_SQL: &str = r#"
    INSERT INTO tier_stats (
      source,
      tier_0, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6,
      tier_7, tier_8, tier_9, tier_10, tier_11, tier_12, tier_13,
      tier_14, tier_15, tier_16, tier_17, tier_18, tier_19,
      tier_20, tier_21, tier_22, tier_23, tier_24, tier_25, tier_26,
      updated_at
    )
    VALUES (
      $1,
      ($2::bigint[])[1], ($2::bigint[])[2], ($2::bigint[])[3],
      ($2::bigint[])[4], ($2::bigint[])[5], ($2::bigint[])[6],
      ($2::bigint[])[7], ($2::bigint[])[8], ($2::bigint[])[9],
      ($2::bigint[])[10], ($2::bigint[])[11], ($2::bigint[])[12],
      ($2::bigint[])[13], ($2::bigint[])[14], ($2::bigint[])[15],
      ($2::bigint[])[16], ($2::bigint[])[17], ($2::bigint[])[18],
      ($2::bigint[])[19], ($2::bigint[])[20], ($2::bigint[])[21],
      ($2::bigint[])[22], ($2::bigint[])[23], ($2::bigint[])[24],
      ($2::bigint[])[25], ($2::bigint[])[26], ($2::bigint[])[27],
      now()
    )
    ON CONFLICT (source) DO UPDATE SET
      tier_0 = EXCLUDED.tier_0,
      tier_1 = EXCLUDED.tier_1,
      tier_2 = EXCLUDED.tier_2,
      tier_3 = EXCLUDED.tier_3,
      tier_4 = EXCLUDED.tier_4,
      tier_5 = EXCLUDED.tier_5,
      tier_6 = EXCLUDED.tier_6,
      tier_7 = EXCLUDED.tier_7,
      tier_8 = EXCLUDED.tier_8,
      tier_9 = EXCLUDED.tier_9,
      tier_10 = EXCLUDED.tier_10,
      tier_11 = EXCLUDED.tier_11,
      tier_12 = EXCLUDED.tier_12,
      tier_13 = EXCLUDED.tier_13,
      tier_14 = EXCLUDED.tier_14,
      tier_15 = EXCLUDED.tier_15,
      tier_16 = EXCLUDED.tier_16,
      tier_17 = EXCLUDED.tier_17,
      tier_18 = EXCLUDED.tier_18,
      tier_19 = EXCLUDED.tier_19,
      tier_20 = EXCLUDED.tier_20,
      tier_21 = EXCLUDED.tier_21,
      tier_22 = EXCLUDED.tier_22,
      tier_23 = EXCLUDED.tier_23,
      tier_24 = EXCLUDED.tier_24,
      tier_25 = EXCLUDED.tier_25,
      tier_26 = EXCLUDED.tier_26,
      updated_at = now()
    "#;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TierCounts {
    pub source: &'static str,
    pub counts: Vec<i64>,
    pub diamond_plus: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TierStatsRefreshSummary {
    pub matches: Option<TierCounts>,
    pub profiles: Option<TierCounts>,
    pub errors: Vec<String>,
}

#[derive(Clone)]
pub struct TierStatsRepository {
    database: Database,
}

impl TierStatsRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn refresh_match_tiers(&self) -> Result<TierCounts, DatabaseError> {
        self.refresh_source(
            "matches",
            r#"
            SELECT mp.league_tier::smallint AS tier, COUNT(*)::bigint AS count
            FROM match_players mp
            JOIN matches m ON m.match_id = mp.match_id
            WHERE m.is_ranked = true
              AND mp.league_tier BETWEEN 0 AND 26
            GROUP BY mp.league_tier
            "#,
        )
        .await
    }

    pub async fn refresh_profile_tiers(&self) -> Result<TierCounts, DatabaseError> {
        self.refresh_source(
            "profiles",
            r#"
            SELECT kbm_tier::smallint AS tier, COUNT(DISTINCT id)::bigint AS count
            FROM players
            WHERE kbm_tier BETWEEN 0 AND 26
            GROUP BY kbm_tier
            "#,
        )
        .await
    }

    pub async fn refresh(&self) -> TierStatsRefreshSummary {
        let mut summary = TierStatsRefreshSummary {
            matches: None,
            profiles: None,
            errors: Vec::new(),
        };
        match self.refresh_match_tiers().await {
            Ok(counts) => summary.matches = Some(counts),
            Err(error) => summary.errors.push(format!("matches: {error}")),
        }
        match self.refresh_profile_tiers().await {
            Ok(counts) => summary.profiles = Some(counts),
            Err(error) => summary.errors.push(format!("profiles: {error}")),
        }
        summary
    }

    async fn refresh_source(
        &self,
        source: &'static str,
        aggregate_sql: &str,
    ) -> Result<TierCounts, DatabaseError> {
        let client = self.database.connection().await?;
        let rows = client.query(aggregate_sql, &[]).await?;
        let mut counts = vec![0_i64; 27];
        for row in rows {
            let tier = row.get::<_, i16>("tier");
            if let Some(value) = counts.get_mut(usize::try_from(tier).unwrap_or(usize::MAX)) {
                *value = row.get::<_, i64>("count");
            }
        }
        client
            .execute(UPSERT_TIER_STATS_SQL, &[&source, &counts])
            .await?;
        Ok(TierCounts {
            source,
            diamond_plus: counts[18..].iter().sum(),
            counts,
        })
    }
}

#[cfg(test)]
mod tests {
    use paladinscat_core::config::BackendConfig;

    use super::*;

    #[test]
    fn diamond_plus_uses_tiers_eighteen_through_twenty_six() {
        let counts = (0_i64..=26).collect::<Vec<_>>();
        assert_eq!(counts[18..].iter().sum::<i64>(), 198);
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with the complete schema"]
    async fn live_database_matches_typescript_tier_projection() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "tier-stats-integration").expect("database");
        let repository = TierStatsRepository::new(database.clone());

        let client = database.connection().await.expect("fixture connection");
        let expected_match_rows = client
            .query(
                "SELECT mp.league_tier::smallint AS tier,COUNT(*)::bigint AS count \
                 FROM match_players mp JOIN matches m ON m.match_id=mp.match_id \
                 WHERE m.is_ranked=TRUE AND mp.league_tier BETWEEN 0 AND 26 \
                 GROUP BY mp.league_tier",
                &[],
            )
            .await
            .expect("expected match tiers");
        let expected_profile_rows = client
            .query(
                "SELECT kbm_tier::smallint AS tier,COUNT(DISTINCT id)::bigint AS count \
                 FROM players WHERE kbm_tier BETWEEN 0 AND 26 GROUP BY kbm_tier",
                &[],
            )
            .await
            .expect("expected profile tiers");
        let to_counts = |rows: Vec<tokio_postgres::Row>| {
            let mut counts = vec![0_i64; 27];
            for row in rows {
                let tier = row.get::<_, i16>("tier");
                counts[usize::try_from(tier).expect("valid tier")] = row.get("count");
            }
            counts
        };
        let expected_matches = to_counts(expected_match_rows);
        let expected_profiles = to_counts(expected_profile_rows);
        client
            .execute(
                "DELETE FROM tier_stats WHERE source IN ('matches', 'profiles')",
                &[],
            )
            .await
            .expect("clean tier stats");
        drop(client);

        let summary = repository.refresh().await;
        assert!(summary.errors.is_empty());
        assert_eq!(
            summary.matches.as_ref().expect("matches").counts,
            expected_matches
        );
        assert_eq!(
            summary.profiles.as_ref().expect("profiles").counts,
            expected_profiles
        );

        let client = database
            .connection()
            .await
            .expect("verification connection");
        let rows = client
            .query(
                "SELECT source, tier_0, tier_18, tier_26 \
                 FROM tier_stats WHERE source IN ('matches', 'profiles') \
                 ORDER BY source",
                &[],
            )
            .await
            .expect("tier stats rows");
        assert_eq!(rows.len(), 2);
        for row in rows {
            let expected = if row.get::<_, String>("source") == "matches" {
                &expected_matches
            } else {
                &expected_profiles
            };
            assert_eq!(row.get::<_, i32>("tier_0"), expected[0] as i32);
            assert_eq!(row.get::<_, i32>("tier_18"), expected[18] as i32);
            assert_eq!(row.get::<_, i32>("tier_26"), expected[26] as i32);
        }
    }
}
