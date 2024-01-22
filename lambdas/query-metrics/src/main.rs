///
/// This lambda function will run configured datafusion queries and report results CloudWatch
/// metrics.
///
use aws_lambda_events::event::eventbridge::EventBridgeEvent;
use aws_sdk_cloudwatch::types::MetricDatum;
use deltalake::datafusion::common::*;
use deltalake::datafusion::execution::context::SessionContext;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;

use std::sync::Arc;

mod config;

async fn function_handler(_event: LambdaEvent<EventBridgeEvent>) -> Result<(), Error> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let cloudwatch = aws_sdk_cloudwatch::Client::new(&aws_config);

    let conf = config::Configuration::from_base64(
        std::env::var("MANIFEST_B64").expect("The `MANIFEST_B64` variable was not defined"),
    )
    .expect("The `MANIFEST_B64` environment variable does not contain a valid manifest yml");
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

                let datum = MetricDatum::builder()
                    .metric_name(&gauge.name)
                    .value(count as f64)
                    .build();
                let res = cloudwatch
                    .put_metric_data()
                    .metric_data(datum)
                    .send()
                    .await?;
                debug!("Result of CloudWatch send: {res:?}");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    info!("Starting the query-metrics lambda");
    run(service_fn(function_handler)).await
}
