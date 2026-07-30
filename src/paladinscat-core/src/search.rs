use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

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

    /// Search one existing MeiliSearch read model.
    ///
    /// This deliberately preserves the TypeScript route contract: an absent
    /// service, missing index, or transient search failure degrades to an empty
    /// result set instead of failing the public request.
    pub async fn search(&self, index: &str, query: &str, limit: usize) -> Vec<Value> {
        let Some(base_url) = self.base_url.as_deref() else {
            return Vec::new();
        };
        if query.trim().is_empty() {
            return Vec::new();
        }
        let response = self
            .request(
                self.client
                    .post(format!("{base_url}/indexes/{index}/search")),
            )
            .json(&json!({
                "q": query.trim(),
                "limit": limit.min(100),
            }))
            .send()
            .await;
        let Ok(response) = response else {
            tracing::warn!(index, "MeiliSearch request failed");
            return Vec::new();
        };
        if !response.status().is_success() {
            tracing::warn!(index, status = %response.status(), "MeiliSearch search was rejected");
            return Vec::new();
        }
        match response.json::<Value>().await {
            Ok(payload) => payload
                .get("hits")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            Err(error) => {
                tracing::warn!(index, error = %error, "MeiliSearch response was invalid");
                Vec::new()
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
