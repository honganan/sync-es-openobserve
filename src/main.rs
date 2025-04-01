mod es;
mod o2;

use clap::Parser;

/// This is a program that migrates data from ElasticSearch to OpenObserve.
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, required = false, default_value = "1000", env = "BATCH_SIZE")]
    batch_size: usize,
    #[arg(long, required = false, default_value = "3", env = "MAX_RETRIES")]
    max_retries: i32,

    #[arg(long, required = true, env = "ES_ADDR")]
    es_addr: String,
    #[arg(long, required = true, env = "ES_USER")]
    es_user: String,
    #[arg(long, required = true, env = "ES_PASS")]
    es_pass: String,
    #[arg(long, required = true, env = "ES_INDEX")]
    es_index: String,

    #[arg(long, required = true, env = "O2_ADDR")]
    o2_addr: String,
    #[arg(long, required = true, env = "O2_USER")]
    o2_user: String,
    #[arg(long, required = true, env = "O2_PASS")]
    o2_pass: String,
    #[arg(long, required = true, env = "O2_ORG")]
    o2_org: String,
    #[arg(long, required = true, env = "O2_STREAM")]
    o2_stream: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Starting the program with args: {:?}", args);

    let es = es::Es::new(args.clone());
    let o2_client = o2::O2Client::new(args.clone());

    // Initial search
    let (mut scroll_id, mut hits, total) = es.search(&args.es_index.clone(), args.batch_size).await?;

    println!("Found {} records to process...", total);

    // Loop and migrate to O2, then scroll next batch
    let mut processed = 0;
    let mut failed_count = 0;
    while !hits.is_empty() {
        let result = o2_client.send_to_json(&hits).await;
        match result {
            Ok((successful, failed)) => {
                processed += successful as usize;
                failed_count += failed as usize;
            },
            Err(e) => {
                failed_count += hits.len();
                println!("Error writing data to OpenObserve: {}", e);
            }
        }

        // Scroll next batch
        let (new_scroll_id, new_hits, _) = es.scroll_with_retry(scroll_id, args.max_retries).await?;
        scroll_id = new_scroll_id;
        hits = new_hits;
    }

    println!(
        "Migration completed! {} records migrated successfully, {} records failed.",
        processed, failed_count
    );

    es.clear_scroll(scroll_id).await?;
    Ok(())
}
