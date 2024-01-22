///
/// This lambda function will run configured datafusion queries and report results CloudWatch
/// metrics.
///
use aws_lambda_events::event::eventbridge::EventBridgeEvent;
use deltalake::datafusion::common::*;
use deltalake::datafusion::execution::context::SessionContext;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;

use std::sync::Arc;

mod config;

async fn function_handler(_event: LambdaEvent<EventBridgeEvent>) -> Result<(), Error> {
    let conf: config::Configuration = config::Configuration::from_file("manifest.yml");
    debug!("Configuration loaded: {conf:?}");

    for (name, gauge) in conf.gauges.iter() {
        debug!("Querying the {name} table");
        let ctx = SessionContext::new();
        let table = deltalake::open_table(&gauge.url)
            .await
            .expect("Failed to register table");
        ctx.register_table("source", Arc::new(table))
            .expect("Failed to register table with datafusion");

        debug!("Running query: {}", gauge.query);

        let df = ctx
            .sql(&gauge.query)
            .await
            .expect("Failed to execute query");

        match gauge.measurement_type {
            config::Measurement::Count => {
                let count = df.count().await.expect("Failed to collect batches");

                debug!("Found {count} distinct records");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    info!("Starting the query-metrics lambda");
    run(service_fn(function_handler)).await
}
