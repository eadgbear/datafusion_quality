# DataFusion Quality (DFQ)

A data quality framework for DataFusion, inspired by Great Expectations and Spark Expectations.

## Features

- Schema-level rules
- Column-level rules
- Table-level aggregate rules
- Custom rule support
- Rich expression support
- Async processing
- Fluent API for rule creation

## Getting Started

Add the following to your `Cargo.toml`:

```toml
[dependencies]
datafusion-quality = "0.1.0"
```

## Basic Usage

You can use either the traditional API or the new fluent API for creating rules. For a complete example, see the [basic example](modules/datafusion_quality/examples/basic/src/main.rs).

### Using the Fluent API

```rust
use datafusion_quality::{rules::{column::{dfq_in_range, dfq_not_null}, dfq_gt}, RuleSet};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new DataFusion context
    let ctx = SessionContext::new();
    
    // Create a sample DataFrame
    let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?;
    
    // Create a new RuleSet instance
    let mut rule_set = RuleSet::new();
    
    // Add rules using fluent API
    rule_set.with_column_rule("name", dfq_not_null())
            .with_column_rule("age", dfq_in_range(18.0, 100.0))
            .with_column_rule("score", dfq_gt(lit(50.0)));
    
    // Apply rules
    let result_df = rule_set.apply(&df).await?;
    
    // Show the results
    result_df.show().await?;
    
    // Partition data into good and bad records
    let (good_data, bad_data) = rule_set.partition(&df).await?;
    
    Ok(())
}
```

### Using the Traditional API

```rust
use dfq::{RuleSet, rules::{column::{NotNullRule, RangeRule, PatternRule, CustomRule}}};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new DataFusion context
    let ctx = SessionContext::new();
    
    // Create a sample DataFrame
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("email", DataType::Utf8, false),
    ]);
    
    let df = ctx.read_csv("data.csv", CsvReadOptions::new().schema(&schema)).await?;
    
    // Create a new RuleSet instance
    let mut rule_set = RuleSet::new();
    
    // Add rules
    rule_set.add_column_rule(Arc::new(NotNullRule::new("name")));
    rule_set.add_column_rule(Arc::new(RangeRule::new("age", 18.0, 100.0)));
    rule_set.add_column_rule(Arc::new(PatternRule::new("email", "%@%.%")));
    rule_set.add_column_rule(Arc::new(CustomRule::new("age", "age > 25")));
    
    // Apply rules
    let result_df = rule_set.apply(&df).await?;
    
    // Show the results
    result_df.show().await?;
    
    Ok(())
}
```

## Available Rules

### Column Rules
- `dfq_not_null()`: Checks if values in a column are not null
- `dfq_null()`: Checks if values in a column are null
- `dfq_in_range(min, max)`: Checks if values in a column fall within a specified range
- `dfq_not_in_range(min, max)`: Checks if values in a column fall outside a specified range
- `dfq_like(pattern)`: Checks if string values match a case-sensitive pattern
- `dfq_not_like(pattern)`: Checks if string values do not match a case-sensitive pattern
- `dfq_ilike(pattern)`: Checks if string values match a case-insensitive pattern
- `dfq_not_ilike(pattern)`: Checks if string values do not match a case-insensitive pattern
- `dfq_lt(value)`: Checks if values are less than a specified value
- `dfq_lte(value)`: Checks if values are less than or equal to a specified value
- `dfq_not_lt(value)`: Checks if values are not less than a specified value
- `dfq_not_lte(value)`: Checks if values are not less than or equal to a specified value
- `dfq_gt(value)`: Checks if values are greater than a specified value
- `dfq_gte(value)`: Checks if values are greater than or equal to a specified value
- `dfq_not_gt(value)`: Checks if values are not greater than a specified value
- `dfq_not_gte(value)`: Checks if values are not greater than or equal to a specified value
- `dfq_eq(value)`: Checks if values are equal to a specified value
- `dfq_not_eq(value)`: Checks if values are not equal to a specified value
- `dfq_str_length(min, max)`: Checks if string length is within specified bounds
- `dfq_str_min_length(min)`: Checks if string length is at least the specified minimum
- `dfq_str_max_length(max)`: Checks if string length is at most the specified maximum
- `dfq_str_empty()`: Checks if strings are empty
- `dfq_str_not_empty()`: Checks if strings are not empty
- `dfq_custom(rule_name, expression)`: Applies a custom SQL expression to a column

### Table Rules
- `dfq_null_count()`: Counts the number of null values in a column
- `dfq_not_null_count()`: Counts the number of non-null values in a column
- `dfq_count()`: Counts the total number of rows in a column
- `dfq_count_distinct()`: Counts the number of distinct values in a column
- `dfq_avg()`: Calculates the average value of a column
- `dfq_stddev()`: Calculates the standard deviation of a column
- `dfq_max()`: Finds the maximum value in a column
- `dfq_min()`: Finds the minimum value in a column
- `dfq_sum()`: Calculates the sum of values in a column
- `dfq_median()`: Calculates the median value of a column
- `dfq_last_value()`: Gets the last value in a column
- `dfq_stddev_pop()`: Calculates the population standard deviation of a column
- `dfq_var_pop()`: Calculates the population variance of a column
- `dfq_var_samp()`: Calculates the sample variance of a column
- `dfq_covar_pop(x, y)`: Calculates the population covariance between two columns
- `dfq_covar_samp(x, y)`: Calculates the sample covariance between two columns
- `dfq_regr_avgx(x, y)`: Calculates the average of x values in a linear regression
- `dfq_regr_avgy(x, y)`: Calculates the average of y values in a linear regression
- `dfq_regr_count(x, y)`: Counts the number of rows used in a linear regression
- `dfq_regr_intercept(x, y)`: Calculates the intercept of a linear regression
- `dfq_regr_r2(x, y)`: Calculates the R-squared value of a linear regression
- `dfq_regr_slope(x, y)`: Calculates the slope of a linear regression
- `dfq_regr_sxx(x, y)`: Calculates the sum of squared deviations from the mean for x values
- `dfq_regr_sxy(x, y)`: Calculates the sum of products of deviations from the mean for x and y values
- `dfq_regr_syy(x, y)`: Calculates the sum of squared deviations from the mean for y values
- `dfq_nth_value(n, sort_exprs)`: Gets the nth value in a column with optional sorting
- `dfq_first_value(sort_exprs)`: Gets the first value in a column with optional sorting
- `dfq_custom_agg(aggregation, rule_name)`: Creates a custom aggregation rule with a specified expression and name

### Schema Rules
- `ColumnExistsRule`: Checks if a column exists in the schema
- `ColumnTypeRule`: Checks if a column has a specific data type
- `ColumnNullableRule`: Checks if a column is nullable

## Creating Custom Rules

You can create custom rules by implementing the appropriate trait (`ColumnRule`, `TableRule`, or `SchemaRule`):

```rust
use dfq::{ColumnRule, ValidationError};
use datafusion::prelude::*;

pub struct CustomColumnRule {
    column_name: String,
    expression: String,
}

impl CustomColumnRule {
    pub fn new(column_name: &str, expression: &str) -> Self {
        Self {
            column_name: column_name.to_string(),
            expression: expression.to_string(),
        }
    }
}

impl ColumnRule for CustomColumnRule {
    fn apply(&self, df: &DataFrame) -> Result<DataFrame, ValidationError> {
        let result_col = format!("{}_custom", self.column_name);
        
        df.select(vec![
            col("*"),
            sql(&self.expression).alias(&result_col),
        ])
    }
    
    fn name(&self) -> &str {
        "custom"
    }
    
    fn description(&self) -> &str {
        "Applies a custom SQL expression to a column"
    }
    
    fn column_name(&self) -> &str {
        &self.column_name
    }
}
```

## Rule Results

Each rule adds a new column to the DataFrame with a name in the format `<column_name>_<rule_name>`. The value in these columns is a boolean indicating whether the rule passed for that row. One final column is created called `dq_pass` that is the boolean `AND` of all of the rule columns.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License - see the LICENSE file for details. 