use crate::Args;
use elasticsearch::auth::Credentials;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{ClearScrollParts, Elasticsearch, ScrollParts, SearchParts};
use reqwest::Url;
use serde_json::{json, Value};

static SCROLL_DURATION: &str = "10m";

pub(crate) struct Es {
    pub(crate) client: Elasticsearch,
}

impl Es {
    pub(crate) fn new(args: Args) -> Self {
        let url = Url::parse(&args.es_addr).unwrap();
        let conn_pool = SingleNodeConnectionPool::new(url);

        // 构建 TransportBuilder
        let builder = TransportBuilder::new(conn_pool)
            .auth(Credentials::Basic(args.es_user, args.es_pass))
            .build()
            .unwrap();

        let client = Elasticsearch::new(builder);

        Es {
            client,
        }
    }

    pub(crate) async fn search(&self, index: &str, batch_size: usize) -> Result<Value, Box<dyn std::error::Error>> {
        let response = self.client
            .search(SearchParts::Index(&[&index]))
            .scroll(SCROLL_DURATION)
            .body(json!({
                "query": {
                    "match_all": {}
                },
                "size": batch_size,
            }))
            .send()
            .await?;

        let response_body = response.json::<Value>().await?;
        Ok(response_body)
    }

    pub(crate) async fn scroll(&self, scroll_id: String) -> Result<Value, Box<dyn std::error::Error>> {
        let response = self.client
            .scroll(ScrollParts::ScrollId(&scroll_id))
            .scroll(SCROLL_DURATION)
            .send()
            .await?;

        let response_body = response.json::<Value>().await?;
        Ok(response_body)
    }

    pub(crate) async fn clear_scroll(&self, scroll_id: String) -> Result<(), Box<dyn std::error::Error>> {
        let response = self.client
            .clear_scroll(ClearScrollParts::None)
            .body(json!({
                "scroll_id": scroll_id
                })
            ).send().await?;

        if response.status_code().is_success() {
            Ok(())
        } else {
            Err("Failed to clear scroll".into())
        }
    }

    pub(crate) fn extract_search_result(response_body: Value) -> Result<(String, Vec<Value>, u64), Box<dyn std::error::Error>> {
        if response_body["error"].is_object() {
            println!("Error: {:?}", response_body["error"]);
            return Err("Error in response".into());
        }

        let scroll_id = response_body["_scroll_id"].as_str().unwrap().to_string();
        let hits = response_body["hits"]["hits"].as_array().unwrap().to_vec();
        let total = response_body["hits"]["total"]["value"].as_u64().unwrap_or(0);

        Ok((scroll_id, hits, total))
    }
}