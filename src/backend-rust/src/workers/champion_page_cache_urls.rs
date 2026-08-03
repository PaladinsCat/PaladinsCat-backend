use std::collections::HashSet;

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChampionPageUrl {
    pub champion_name: String,
    pub slug: String,
    pub urls: Vec<String>,
}

pub fn generate_champion_page_urls(champions: &[(String, Option<i32>)]) -> Vec<ChampionPageUrl> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for (name, talent_id) in champions {
        let slug = slugify(name);
        if slug.is_empty() {
            continue;
        }
        let mut champion_urls = Vec::new();
        let page_url = format!("/champions/{}/page-data", slug);
        if seen.insert(page_url.clone()) {
            champion_urls.push(page_url);
        }
        if let Some(talent_id) = talent_id.filter(|id| *id > 0) {
            let talent_url = format!("/champions/{}/talents/{}/page-data", slug, talent_id);
            if seen.insert(talent_url.clone()) {
                champion_urls.push(talent_url);
            }
        }
        urls.push(ChampionPageUrl {
            champion_name: name.clone(),
            slug,
            urls: champion_urls,
        });
    }
    urls
}

pub fn slugify(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}
