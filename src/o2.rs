use reqwest::Client;
use serde_json::{json, Value};
use crate::Args;

pub(crate) struct O2Client{
    o2_addr: String,
    o2_user: String,
    o2_pass: String,
    o2_org: String,
    o2_stream: String,

    client: Client,
}

impl O2Client {
    pub(crate) fn new(args: Args) -> Self {
        O2Client {
            o2_addr: args.o2_addr,
            o2_user: args.o2_user,
            o2_pass: args.o2_pass,
            o2_org: args.o2_org,
            o2_stream: args.o2_stream,
            client: Client::new(),
        }
    }

    pub(crate) async fn send_to_json(&self, hits: &Vec<Value>) -> Result<(u64, u64), Box<dyn std::error::Error>> {
        let url = format!("{}/api/{}/{}/_json", self.o2_addr, self.o2_org, self.o2_stream);

        // 提取 _source 字段组成新的数组
        let sources: Vec<Value> = hits.iter().map(|hit| {
            hit["_source"].clone()
        }).collect();

        let body = json!(sources);
        let response = self.client.post(&url)
            .basic_auth(&self.o2_user, Some(&self.o2_pass))
            .json(&body)
            .send()
            .await?;

        let code = response.status();
        let body = response.json::<Value>().await?;
        
        if code.is_success() {
            println!("Sent to O2 successfully: {}", body["status"]);
            let successful = body["status"][0].get("successful").and_then(|v| v.as_u64()).unwrap_or(0);
            let failed = body["status"][0].get("failed").and_then(|v| v.as_u64()).unwrap_or(0);
            Ok((successful, failed))
        } else {
            let status = body["status"].as_str();
            println!("Error: {} - {}", code, status.unwrap_or("Unknown error"));
            Err("Error in response".into())
        }
    }
}
