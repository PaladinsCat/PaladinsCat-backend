use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::json;

use crate::config::BackendConfig;

#[derive(Clone)]
pub struct SearchIndex {
    client: Client,
    base_url: Option<String>,
    api_key: Option<String>,
}

impl SearchIndex {
    pub fn new(config: &BackendConfig) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_millis(config.meilisearch_timeout_ms))
                .build()?,
            base_url: config
                .meilisearch_url
                .as_deref()
                .map(|value| value.trim_end_matches('/').to_owned()),
            api_key: config.meilisearch_api_key.clone(),
        })
    }

    pub fn configured(&self) -> bool {
        self.base_url.is_some()
    }

    pub async fn health_check(&self) -> bool {
        let Some(base_url) = self.base_url.as_deref() else {
            return false;
        };
        self.request(self.client.get(format!("{base_url}/stats")))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }

    pub async fn initialize_indices(&self) {
        let Some(base_url) = self.base_url.as_deref() else {
            return;
        };
        for uid in ["players", "matches"] {
            let result = self
                .request(self.client.post(format!("{base_url}/indexes")))
                .json(&json!({ "uid": uid, "primaryKey": "objectID" }))
                .send()
                .await;
            match result {
                Ok(response)
                    if response.status().is_success()
                        || response.status() == StatusCode::BAD_REQUEST => {}
                Ok(response) => {
                    tracing::warn!(
                        index = uid,
                        status = %response.status(),
                        "MeiliSearch index initialization was rejected"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        index = uid,
                        error = %error,
                        "MeiliSearch index initialization failed"
                    );
                }
            }
        }
    }

    fn request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.api_key.as_deref() {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(values: &[(&str, &str)]) -> BackendConfig {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        BackendConfig::from_lookup(|name| values.get(name).cloned()).expect("config")
    }

    #[tokio::test]
    async fn missing_optional_search_service_is_explicitly_degraded() {
        let search =
            SearchIndex::new(&config(&[("DATABASE_URL", "postgres://fixture")])).expect("search");
        assert!(!search.configured());
        assert!(!search.health_check().await);
        search.initialize_indices().await;
    }

    #[test]
    fn configured_search_client_does_not_expose_its_key() {
        let search = SearchIndex::new(&config(&[
            ("DATABASE_URL", "postgres://fixture"),
            ("MEILISEARCH_URL", "http://meilisearch:7700/"),
            ("MEILISEARCH_API_KEY", "secret"),
        ]))
        .expect("search");
        assert!(search.configured());
        assert_eq!(search.base_url.as_deref(), Some("http://meilisearch:7700"));
    }
}
