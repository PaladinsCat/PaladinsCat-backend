use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use futures::{StreamExt, stream};
use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use regex::Regex;
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::Value;

use super::policy::champion_page_warm_urls;

const ACTIVITY_API_WARM_URLS: &[&str] = &[
    "/matches/overview?view=activity-v3",
    "/stats/presence?view=activity-v4",
    "/stats/presence/hourly?view=activity-v3",
    "/stats/activity-banner",
];

const DEPLOYMENT_CRITICAL_API_WARM_URLS: &[&str] = &[
    "/champions/overview",
    "/players/overview",
    "/matches/overview",
    "/stats/overview",
    "/matches/overview?view=activity-v3",
    "/stats/presence?view=activity-v4",
];

const STATIC_MAIN_API_WARM_URLS: &[&str] = &[
    "/players/leaderboard/class?role=Frontline&limit=100&queueId=486&mode=account",
    "/players/leaderboard/champion-elo?limit=100&queueId=486",
    "/stats/ranked-leaderboard?tier=26&top=100",
    "/players/boosted?limit=100",
    "/matches/recent?limit=20",
    "/matches/compositions?limit=200",
    "/stats/champions?sort=winrate&limit=100",
    "/stats/regions",
    "/stats/platforms",
    "/operations/stats",
    "/stats/loadouts",
    "/stats/items?mode=ranked&limit=200",
    "/stats/maps?queueId=486&limit=100",
    "/stats/skins?limit=200",
    "/stats/broken-skins",
    "/stats/talents",
    "/stats/cards?limit=200",
    "/champions/overview?scope=ranked",
    "/stats/tiers?source=profiles",
    "/stats/tiers?source=matches",
    "/stats/tiers/summary",
    "/stats/baselines?queueId=486",
    "/meta/changelog?page=1&perPage=10",
    "/notifications?limit=5",
    "/champions/overview",
    "/players/overview",
    "/matches/overview",
    "/stats/overview",
    "/stats/page-data",
];

#[derive(Clone, Debug, Default, Serialize)]
pub struct WarmResult {
    pub discovered: usize,
    pub warmed: usize,
    pub deferred: usize,
    pub failed: usize,
    pub failures: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CacheWarmError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Client(#[from] reqwest::Error),
    #[error("cache warm failed for {failed} critical route(s)")]
    Critical { failed: usize },
}

#[derive(Clone)]
pub struct CacheWarmer {
    database: Database,
    client: Client,
    api_origin: String,
    frontend_origin: String,
    service_token: Option<String>,
    failure_tracker: Arc<Mutex<HashMap<String, (u32, SystemTime)>>>,
}

impl CacheWarmer {
    pub fn new(database: Database, config: &BackendConfig) -> Result<Self, CacheWarmError> {
        Ok(Self {
            database,
            client: Client::builder()
                .timeout(Duration::from_secs(env_u64(
                    "SITE_CACHE_WARM_REQUEST_TIMEOUT_SECONDS",
                    60,
                )))
                .user_agent("PaladinsCat-internal-cache-warmer/2.0")
                .build()?,
            api_origin: std::env::var("SITE_CACHE_WARM_API_ORIGIN")
                .unwrap_or_else(|_| format!("http://{}:{}", config.api_host, config.api_port)),
            frontend_origin: std::env::var("SITE_CACHE_WARM_FRONTEND_ORIGIN")
                .unwrap_or_else(|_| "http://frontend:3000".to_owned())
                .trim_end_matches('/')
                .to_owned(),
            service_token: config.service_token.clone(),
            failure_tracker: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn warm_deployment_critical(&self) -> Result<WarmResult, CacheWarmError> {
        let result = self
            .warm_api_urls(DEPLOYMENT_CRITICAL_API_WARM_URLS.iter().copied(), false)
            .await;
        if result.failed > 0 {
            return Err(CacheWarmError::Critical {
                failed: result.failed,
            });
        }
        Ok(result)
    }

    pub async fn warm_main_site(&self) -> Result<(WarmResult, WarmResult), CacheWarmError> {
        let urls = main_api_warm_urls();
        let api = self
            .warm_api_urls(urls.iter().map(String::as_str), true)
            .await;
        let sitemap = self
            .client
            .get(format!("{}/sitemap.xml", self.frontend_origin))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let pages = main_page_warm_paths(&sitemap, env_f64("SITE_CACHE_WARM_MIN_PRIORITY", 0.8));
        let concurrency = env_usize("SITE_CACHE_WARM_PAGE_CONCURRENCY", 2).max(1);
        let client = self.client.clone();
        let origin = self.frontend_origin.clone();
        let outcomes = stream::iter(pages.iter().cloned())
            .map(move |path| {
                let client = client.clone();
                let origin = origin.clone();
                async move {
                    let ok = client
                        .get(format!("{origin}{path}"))
                        .header("accept", "text/html")
                        .send()
                        .await
                        .is_ok_and(|response| {
                            response.status().is_success()
                                && response
                                    .headers()
                                    .get("content-type")
                                    .and_then(|value| value.to_str().ok())
                                    .is_some_and(|value| value.contains("text/html"))
                        });
                    (path, ok)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let failures = outcomes
            .iter()
            .filter(|(_, ok)| !ok)
            .map(|(path, _)| path.clone())
            .take(10)
            .collect::<Vec<_>>();
        let failed = outcomes.iter().filter(|(_, ok)| !ok).count();
        let page_result = WarmResult {
            discovered: pages.len(),
            warmed: outcomes.len().saturating_sub(failed),
            deferred: 0,
            failed,
            failures,
        };
        Ok((api, page_result))
    }

    pub async fn warm_champion_pages(&self) -> Result<WarmResult, CacheWarmError> {
        let rows = self
            .database
            .query_json(
                "SELECT c.name,t.talent_id FROM champions c LEFT JOIN talents t ON t.champion_id=c.id \
                 WHERE c.id>0 ORDER BY c.name,t.talent_id",
                &[],
            )
            .await?;
        let pairs = rows
            .into_iter()
            .filter_map(|row| {
                Some((
                    row.get("name")?.as_str()?.to_owned(),
                    row.get("talent_id")
                        .and_then(Value::as_i64)
                        .and_then(|value| i32::try_from(value).ok()),
                ))
            })
            .collect::<Vec<_>>();
        let urls = champion_page_warm_urls(&pairs);
        Ok(self
            .warm_api_urls(urls.iter().map(String::as_str), true)
            .await)
    }

    /// Purpose: populate public API caches without allowing one slow route to
    /// serialize every other major-page warm. Input: canonical API paths and a
    /// flag for whether per-route failure back-off applies. Output: one aggregate
    /// success/failure result; no response body is kept. Routes that keep failing
    /// (e.g. statement timeouts) are deferred for an exponentially growing window
    /// so the warmer stops re-firing them every cycle; real user traffic is
    /// unaffected because back-off only governs pre-population.
    async fn warm_api_urls<'a>(
        &self,
        urls: impl IntoIterator<Item = &'a str>,
        backoff: bool,
    ) -> WarmResult {
        let all_urls: Vec<String> = urls.into_iter().map(str::to_owned).collect();
        let discovered = all_urls.len();

        // Decide which routes are currently in back-off and skip them this cycle.
        let mut to_warm: Vec<String> = Vec::new();
        let mut deferred = 0usize;
        if backoff {
            let now = SystemTime::now();
            let tracker = self
                .failure_tracker
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for url in all_urls {
                let in_backoff = tracker.get(&url).is_some_and(|(_, until)| now < *until);
                if in_backoff {
                    deferred += 1;
                } else {
                    to_warm.push(url);
                }
            }
        } else {
            to_warm = all_urls;
        }

        let concurrency = env_usize("SITE_CACHE_WARM_API_CONCURRENCY", 2).max(1);
        let client = self.client.clone();
        let origin = self.api_origin.trim_end_matches('/').to_owned();
        let service_token = self.service_token.clone();
        let outcomes = stream::iter(to_warm)
            .map(move |path| {
                let client = client.clone();
                let origin = origin.clone();
                let service_token = service_token.clone();
                async move {
                    let mut request = client
                        .get(format!("{origin}{path}"))
                        .header("x-paladinscat-cache-revalidate", "1")
                        .header("x-paladinscat-internal-request", "cache-warmer");
                    if let Some(token) = service_token.as_deref() {
                        request = request.bearer_auth(token);
                    }
                    let failure = match request.send().await {
                        Ok(response) if response.status().is_success() => None,
                        Ok(response) => Some(format!("{path}: {}", response.status())),
                        Err(error) => Some(format!("{path}: {error}")),
                    };
                    (path, failure)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        // Record outcomes: clear the back-off on success, extend it on failure.
        if backoff {
            let now = SystemTime::now();
            let mut tracker = self
                .failure_tracker
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for (path, failure) in &outcomes {
                match failure {
                    None => {
                        tracker.remove(path);
                    }
                    Some(_) => {
                        let count = tracker.get(path).map_or(0, |entry| entry.0) + 1;
                        let until = now + Duration::from_secs(warm_backoff_seconds(count));
                        tracker.insert(path.clone(), (count, until));
                    }
                }
            }
        }

        let mut result = WarmResult {
            discovered,
            warmed: outcomes
                .iter()
                .filter(|(_, failure)| failure.is_none())
                .count(),
            failed: outcomes
                .iter()
                .filter(|(_, failure)| failure.is_some())
                .count(),
            deferred,
            ..WarmResult::default()
        };
        for failure in outcomes
            .into_iter()
            .filter_map(|(_, failure)| failure)
            .take(10)
        {
            result.failures.push(failure);
        }
        result
    }
}

pub fn main_api_warm_urls() -> Vec<String> {
    let mut urls = BTreeSet::new();
    urls.extend(
        ACTIVITY_API_WARM_URLS
            .iter()
            .map(|value| (*value).to_owned()),
    );
    for metric in ["gpm", "hpm", "dpm", "mpm"] {
        let role = match metric {
            "hpm" => "&role=Support",
            "dpm" => "&role=Damage",
            "mpm" => "&role=Frontline",
            _ => "",
        };
        urls.insert(format!("/players/leaderboard/performance?metric={metric}&limit=100{role}&queueId=486&scope=ranked"));
        urls.insert(format!(
            "/stats/performance-metrics?metric={metric}{role}&queueId=486&scope=ranked"
        ));
        urls.insert(format!(
            "/players/leaderboard/performance?metric={metric}&limit=100{role}&scope=casual"
        ));
        urls.insert(format!(
            "/stats/performance-metrics?metric={metric}{role}&scope=casual"
        ));
    }
    for metric in ["dpm", "hpm", "gpm", "egpm", "mpm", "kda"] {
        urls.insert(format!(
            "/stats/performance-metrics?metric={metric}&includeRoles=1"
        ));
        urls.insert(format!(
            "/stats/performance-metrics/by-champion?metric={metric}"
        ));
    }
    urls.extend(
        STATIC_MAIN_API_WARM_URLS
            .iter()
            .map(|value| (*value).to_owned()),
    );
    for scope in [
        "tierMin=1&tierMax=15",
        "tierMin=16&tierMax=26",
        "tierMin=21&tierMax=26",
    ] {
        for path in [
            "/matches/overview",
            "/stats/overview",
            "/stats/page-data",
            "/stats/champions?sort=winrate&limit=100",
            "/stats/items?mode=ranked&limit=200",
            "/stats/maps?queueId=486&limit=100",
            "/stats/platforms",
            "/stats/skins?limit=200",
            "/stats/broken-skins",
            "/stats/baselines?queueId=486",
        ] {
            urls.insert(format!(
                "{path}{}{scope}",
                if path.contains('?') { "&" } else { "?" }
            ));
        }
    }
    urls.into_iter().collect()
}

pub fn main_page_warm_paths(xml: &str, minimum_priority: f64) -> Vec<String> {
    let entry = Regex::new(r"(?is)<url>(.*?)</url>").expect("static sitemap entry regex");
    let location = Regex::new(r"(?is)<loc>(.*?)</loc>").expect("static location regex");
    let priority = Regex::new(r"(?is)<priority>(.*?)</priority>").expect("static priority regex");
    let mut seen = BTreeSet::new();
    for capture in entry.captures_iter(xml) {
        let Some(body) = capture.get(1).map(|value| value.as_str()) else {
            continue;
        };
        let Some(location) = location
            .captures(body)
            .and_then(|row| row.get(1))
            .map(|value| decode_xml(value.as_str().trim()))
        else {
            continue;
        };
        let Some(priority) = priority
            .captures(body)
            .and_then(|row| row.get(1))
            .and_then(|value| value.as_str().trim().parse::<f64>().ok())
        else {
            continue;
        };
        if priority < minimum_priority {
            continue;
        }
        if let Ok(url) = url::Url::parse(&location) {
            let mut path = url.path().to_owned();
            if let Some(query) = url.query() {
                path.push('?');
                path.push_str(query);
            }
            if path.starts_with('/') {
                seen.insert(path);
            }
        }
    }
    seen.into_iter().collect()
}

fn decode_xml(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&apos;", "'")
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_f64(name: &str, fallback: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// Purpose: size the cache-warmer back-off window for a route that has failed
/// `consecutive_failures` times in a row. Input: failure count. Output: seconds
/// to defer the route — a 30-minute base that doubles per consecutive failure and
/// caps at 6 hours. The base must exceed the 10-minute warm cycle, otherwise a
/// persistently timing-out route is still re-fired on most cycles (prod-verified:
/// a 5-minute base re-fired the casual percentile scans on cycles 2 and 4).
fn warm_backoff_seconds(consecutive_failures: u32) -> u64 {
    const BASE_SECONDS: u64 = 1800;
    const MAX_SECONDS: u64 = 6 * 3600;
    let shift = consecutive_failures.saturating_sub(1).min(20);
    BASE_SECONDS.saturating_mul(1u64 << shift).min(MAX_SECONDS)
}

#[allow(dead_code)]
fn _status_is_success(status: StatusCode) -> bool {
    status.is_success()
}

#[cfg(test)]
mod tests {
    use super::{main_api_warm_urls, warm_backoff_seconds};

    #[test]
    fn activity_default_requests_are_all_warmed() {
        let urls = main_api_warm_urls();
        for expected in [
            "/matches/overview?view=activity-v3",
            "/stats/presence?view=activity-v4",
            "/stats/presence/hourly?view=activity-v3",
        ] {
            assert!(urls.iter().any(|url| url == expected), "missing {expected}");
        }
    }

    #[test]
    fn warm_backoff_grows_then_caps() {
        // Base exceeds the 10-minute warm cycle so a failing route is skipped,
        // not re-fired, on the next cycle.
        assert_eq!(warm_backoff_seconds(1), 1800);
        assert_eq!(warm_backoff_seconds(2), 3600);
        assert_eq!(warm_backoff_seconds(3), 7200);
        assert_eq!(warm_backoff_seconds(4), 14400);
        // Doubles until it hits the 6-hour cap and stays there.
        assert_eq!(warm_backoff_seconds(100), 6 * 3600);
    }
}
