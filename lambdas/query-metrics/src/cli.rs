///
/// The CLI helps test a manifest
///
use std::collections::HashMap;
use std::sync::Arc;

use deltalake_core::arrow::util::pretty::print_batches;
use deltalake_core::arrow::{array::PrimitiveArray, datatypes::Int64Type};
use deltalake_core::datafusion::common::*;
use deltalake_core::datafusion::execution::context::SessionContext;
use tracing::log::*;

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    deltalake_aws::register_handlers(None);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let conf = config::Configuration::from_file("prod-manifest.yml");

    for (name, gauges) in conf.gauges.iter() {
        for gauge in gauges.iter() {
            println!("Querying the {name} table");
            let ctx = SessionContext::new();
            let table = deltalake_core::open_table(&gauge.url)
                .await
                .expect("Failed to register table");
            println!("table opened");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            ctx.register_table("source", Arc::new(table))
                .expect("Failed to register table with datafusion");

            println!("Running query: {}", gauge.query);

            let df = ctx
                .sql(&gauge.query)
                .await
                .expect("Failed to execute query");

            match gauge.measurement_type {
                config::Measurement::Count => {
                    let count = df.count().await.expect("Failed to collect batches");
                    println!("Counted {count} rows");
                }
                config::Measurement::DimensionalCount => {
                    println!("Need to run dimensional count");
                    let batches = df.collect().await.expect("Failed to collect batches");
                    //let batches = df.explain(false, false).unwrap().collect().await.expect("Failed to collect batches");
                    let _ = print_batches(&batches);

                    println!("I see this many batches: {}", batches.len());
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
                                    println!("{count}: {dimensions:?}");
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
