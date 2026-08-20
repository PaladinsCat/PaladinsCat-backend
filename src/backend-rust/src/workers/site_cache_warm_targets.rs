use std::collections::BTreeSet;

use serde::Serialize;

pub const ACTIVITY_API_WARM_URLS: &[&str] = &[
    "/matches/overview?view=activity-v3",
    "/stats/presence?view=activity-v4",
    "/stats/activity-banner",
];

pub const DEPLOYMENT_CRITICAL_API_WARM_URLS: &[&str] = &[
    "/champions/overview",
    "/players/overview",
    "/matches/overview",
    "/stats/overview",
    "/matches/overview?view=activity-v3",
    "/stats/presence?view=activity-v4",
    "/players/automatic-afk?limit=32&offset=0",
    "/players/wall-shooters?limit=32&offset=0",
    "/players/master-feeding?limit=32&offset=0",
    "/players/performance-diff/tank-diff?limit=32&offset=0",
    "/players/performance-diff/support-diff?limit=32&offset=0",
    "/players/performance-diff/dps-diff?limit=32&offset=0",
    "/players/performance-diff/flank-diff?limit=32&offset=0",
    "/players/performance-diff/the-noob?limit=32&offset=0",
    "/players/performance-diff/hypercarry?limit=32&offset=0",
    "/players/boosted?limit=100&offset=0",
    "/players/search?cheater=true&limit=100&offset=0&perPage=100",
    "/players/search?susOnly=true&limit=100&offset=0&perPage=100",
    "/players/search?weirdoOnly=true&limit=100&offset=0&perPage=100",
    "/players/search?hallOfFameOnly=true&limit=100&offset=0&perPage=100",
    "/players/search?dropperOnly=true&limit=100&offset=0&perPage=100",
    "/players/search?afkWintradeOnly=true&limit=100&offset=0&perPage=100",
    "/players/search?altAccountOnly=true&limit=100&offset=0&perPage=100",
    "/player-ext/private?page=1&perPage=24",
    "/coplay/parties?page=1&perPage=24&kind=pairs",
    "/coplay/parties?page=1&perPage=24&kind=stacks",
    "/players/alt-account-relations?page=1&perPage=24",
];

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheWarmTarget {
    pub url: String,
    pub priority: f64,
    pub category: String,
}

pub fn get_deployment_critical_targets() -> Vec<CacheWarmTarget> {
    DEPLOYMENT_CRITICAL_API_WARM_URLS
        .iter()
        .map(|url| CacheWarmTarget {
            url: (*url).to_owned(),
            priority: 1.0,
            category: "deployment_critical".to_owned(),
        })
        .collect()
}

pub fn get_activity_targets() -> Vec<CacheWarmTarget> {
    ACTIVITY_API_WARM_URLS
        .iter()
        .map(|url| CacheWarmTarget {
            url: (*url).to_owned(),
            priority: 0.9,
            category: "activity".to_owned(),
        })
        .collect()
}

pub fn get_all_warm_targets() -> Vec<CacheWarmTarget> {
    let mut urls = BTreeSet::new();
    let mut targets = Vec::new();
    for url in DEPLOYMENT_CRITICAL_API_WARM_URLS
        .iter()
        .map(|v| (*v).to_owned())
    {
        urls.insert(url.clone());
        targets.push(CacheWarmTarget {
            url,
            priority: 1.0,
            category: "deployment_critical".to_owned(),
        });
    }
    for metric in ["gpm", "hpm", "dpm", "mpm"] {
        let role = match metric {
            "hpm" => "&role=Support",
            "dpm" => "&role=Damage",
            "mpm" => "&role=Frontline",
            _ => "",
        };
        for suffix in [
            &format!(
                "/players/leaderboard/performance?metric={metric}&limit=100{role}&queueId=486&scope=ranked"
            ),
            &format!("/stats/performance-metrics?metric={metric}{role}&queueId=486&scope=ranked"),
        ] {
            if urls.insert(suffix.to_owned()) {
                targets.push(CacheWarmTarget {
                    url: suffix.to_owned(),
                    priority: 0.85,
                    category: "leaderboard".to_owned(),
                });
            }
        }
    }
    targets
}
