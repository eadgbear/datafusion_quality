use arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_quality::{
    RuleSet,
    rules::{
        column::{dfq_in_range, dfq_not_null},
        dfq_gt,
    },
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new DataFusion context
    let ctx = SessionContext::new();

    // Create a sample DataFrame
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("score", DataType::Float64, true),
    ]);

    let id_data = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_data = StringArray::from(vec![
        Some("Alice"),
        Some("Bob"),
        None,
        Some("Charlie"),
        Some("Dave"),
    ]);
    let age_data = Int32Array::from(vec![Some(25), Some(30), Some(15), Some(40), Some(20)]);
    let score_data = Float64Array::from(vec![
        Some(85.5),
        Some(92.0),
        Some(78.5),
        Some(95.0),
        Some(88.5),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_data),
            Arc::new(name_data),
            Arc::new(age_data),
            Arc::new(score_data),
        ],
    )
    .unwrap();

    let df = ctx.read_batch(batch.clone()).unwrap();

    // Create a new RuleSet instance
    let mut rule_set = RuleSet::new();

    // Add rules using fluent API
    rule_set
        .with_column_rule("name", dfq_not_null())
        .with_column_rule("age", dfq_in_range(18.0, 100.0))
        .with_column_rule("score", dfq_gt(lit(50.0)));

    // Apply rules
    let result_df = rule_set.apply(&df.clone()).await?;

    // Show the results
    println!("Results:");
    result_df.clone().show().await?;

    // Get total count of rows
    let total_count = df.clone().count().await?;

    // Partition the data into good and bad records
    let (good_data, bad_data) = rule_set.partition(&df).await?;

    // Get count of good rows
    let good_count = good_data.clone().count().await?;

    // Show good data
    println!("\nGood records:");
    good_data.clone().show().await?;

    // Show bad data
    println!("\nBad records:");
    bad_data.clone().show().await?;

    // Calculate and show failure ratio
    let failure_ratio = 1.0 - (good_count as f64 / total_count as f64);
    println!("\nFailure ratio: {:.2}%", failure_ratio * 100.0);
    // Only write files if good data is >= 90% of total
    if (good_count as f64 / total_count as f64) <= 0.9 {
        // Show good data
        println!("Too many validation failures - output files not written");
        println!(
            "Good records: {} ({}%)",
            good_count,
            (good_count as f64 / total_count as f64) * 100.0
        );
    }

    Ok(())
}
