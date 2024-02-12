///
/// This lambda function will run configured datafusion queries and report results CloudWatch
/// metrics.
///
use aws_lambda_events::event::cloudwatch_events::CloudWatchEvent;
use aws_sdk_cloudwatch::{
    primitives::DateTime,
    types::{Dimension, MetricDatum, StandardUnit},
};
use deltalake_core::arrow::{array::PrimitiveArray, datatypes::Int64Type};
use deltalake_core::datafusion::common::*;
use deltalake_core::datafusion::execution::context::SessionContext;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::log::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

mod config;

async fn function_handler(_event: LambdaEvent<CloudWatchEvent>) -> Result<(), Error> {
    deltalake_aws::register_handlers(None);

    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let cloudwatch = aws_sdk_cloudwatch::Client::new(&aws_config);

    let conf = config::Configuration::from_base64(
        std::env::var("MANIFEST_B64").expect("The `MANIFEST_B64` variable was not defined"),
    )
    .expect("The `MANIFEST_B64` environment variable does not contain a valid manifest yml");
    debug!("Configuration loaded: {conf:?}");

    for (name, gauges) in conf.gauges.iter() {
        for gauge in gauges.iter() {
            debug!("Querying the {name} table");
            let ctx = SessionContext::new();
            let table = deltalake_core::open_table(&gauge.url)
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
                        .timestamp(DateTime::from(SystemTime::now()))
                        .value(count as f64)
                        .unit(StandardUnit::Count)
                        .build();

                    let res = cloudwatch
                        .put_metric_data()
                        .namespace(format!("DataLake/{name}"))
                        .metric_data(datum)
                        .send()
                        .await?;
                    debug!("Result of CloudWatch send: {res:?}");
                }
                config::Measurement::DimensionalCount => {
                    let batches = df.collect().await.expect("Failed to collect batches");
                    debug!("I see this many batches: {}", batches.len());

                    // Interestingly the collect produces a lot of zero row batches
                    for batch in batches.iter().filter(|b| b.num_rows() > 0) {
                        if let Some(_counts) = batch.column_by_name("count") {
                            // Fetching the count column just to ensure that it exists before doing
                            // any more computation
                            let schema = batch.schema();
                            let fields = schema.fields();

                            for row in 0..batch.num_rows() {
                                let mut dimensions: HashMap<String, String> = HashMap::new();
                                let mut counted = false;
                                let mut count = 0;

                                for (idx, column) in batch.columns().iter().enumerate() {
                                    let field = &fields[idx];
                                    let name = field.name();
                                    if name == "count" {
                                        let arr: &PrimitiveArray<Int64Type> =
                                            arrow::array::cast::as_primitive_array(&column);
                                        count = arr.value(row);
                                        counted = true;
                                    } else {
                                        let arr = arrow::array::cast::as_string_array(&column);
                                        dimensions.insert(name.into(), arr.value(row).into());
                                    }
                                }

                                if counted {
                                    debug!("{count}: {dimensions:?}");
                                    let mut dims: Vec<Dimension> = vec![];

                                    for (key, value) in dimensions.iter() {
                                        dims.push(
                                            Dimension::builder().name(key).value(value).build(),
                                        );
                                    }
                                    let datum = MetricDatum::builder()
                                        .metric_name(&gauge.name)
                                        .timestamp(DateTime::from(SystemTime::now()))
                                        .set_dimensions(Some(dims))
                                        .value(count as f64)
                                        .unit(StandardUnit::Count)
                                        .build();

                                    let res = cloudwatch
                                        .put_metric_data()
                                        .namespace(format!("DataLake/{name}"))
                                        .metric_data(datum)
                                        .send()
                                        .await?;
                                    debug!("Result of CloudWatch send: {res:?}");
                                }
                            }
                        } else {
                            error!("The result set must have a column named `count`");
                        }
                    }
                }
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
