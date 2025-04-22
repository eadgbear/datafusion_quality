pub mod error;
pub mod rules;

use crate::error::ValidationError;
use datafusion::{common::DFSchema, logical_expr::ExprSchemable, prelude::*};
use error::DataFusionSnafu;
use snafu::ResultExt;
use std::sync::Arc;

/// The main RuleSet struct that holds the context and rules
#[derive(Clone, Default)]
pub struct RuleSet {
    pub(crate) schema_rules: Vec<Arc<dyn SchemaRule>>,
    pub(crate) column_rules: Vec<(String, Arc<dyn ColumnRule>)>,
    pub(crate) table_rules: Vec<(String, Arc<dyn TableRule>)>,
}

impl std::fmt::Debug for RuleSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleSet")
            .field("schema_rules", &self.schema_rules)
            .field("column_rules", &self.column_rules)
            .field("table_rules", &self.table_rules)
            .finish_non_exhaustive()
    }
}

/// Trait for schema-level rules
pub trait SchemaRule: Send + Sync + std::fmt::Debug {
    /// Validate the schema
    fn validate_schema(&self, _schema: &DFSchema) -> Result<bool, ValidationError> {
        unimplemented!("validate_schema must be implemented")
    }

    /// Validate the schema with access to the RuleSet
    fn validate_schema_with_ruleset(
        &self,
        schema: &DFSchema,
        _rule_set: &RuleSet,
    ) -> Result<bool, ValidationError> {
        self.validate_schema(schema)
    }

    /// Get the name of the rule
    fn name(&self) -> &str;

    /// Get the description of the rule
    fn description(&self) -> &str;
}

/// Trait for column-level rules
pub trait ColumnRule: Send + Sync + std::fmt::Debug {
    /// Apply the rule to a DataFrame, adding a new column
    fn apply(&self, _df: DataFrame, _column_name: &str) -> Result<DataFrame, ValidationError> {
        unimplemented!("apply must be implemented")
    }

    /// Apply the rule to a DataFrame with access to the RuleSet
    fn apply_with_ruleset(
        &self,
        df: DataFrame,
        column_name: &str,
        _rule_set: &RuleSet,
    ) -> Result<DataFrame, ValidationError> {
        self.apply(df, column_name)
    }

    /// Get the name of the rule
    fn name(&self) -> &str;

    /// Get the name of the new column
    fn new_column_name(&self, column_name: &str) -> String;

    /// Get the description of the rule
    fn description(&self) -> &str;
}

/// Trait for table-level aggregate rules
pub trait TableRule: Send + Sync + std::fmt::Debug {
    /// Apply the rule to a DataFrame, adding a new column with aggregated results
    fn apply(&self, _df: DataFrame, _column_name: &str) -> Result<DataFrame, ValidationError> {
        unimplemented!("apply must be implemented")
    }

    /// Apply the rule to a DataFrame with access to the RuleSet
    fn apply_with_ruleset(
        &self,
        df: DataFrame,
        column_name: &str,
        _rule_set: &RuleSet,
    ) -> Result<DataFrame, ValidationError> {
        self.apply(df, column_name)
    }

    /// Get the name of the rule
    fn name(&self) -> &str;

    /// Get the name of the new column
    fn new_column_name(&self, column_name: &str) -> String;

    /// Get the description of the rule
    fn description(&self) -> &str;
}

impl RuleSet {
    /// Create a new RuleSet instance
    pub fn new() -> Self {
        Self {
            schema_rules: Vec::new(),
            column_rules: Vec::new(),
            table_rules: Vec::new(),
        }
    }

    /// Add a schema rule
    pub fn with_schema_rule(&mut self, rule: Arc<dyn SchemaRule>) -> &mut Self {
        self.schema_rules.push(rule);
        self
    }

    /// Add a column rule
    pub fn with_column_rule(
        &mut self,
        column_name: impl AsRef<str>,
        rule: Arc<dyn ColumnRule>,
    ) -> &mut Self {
        let column_name = column_name.as_ref().to_string();
        self.column_rules.push((column_name, rule));
        self
    }

    /// Add a table rule
    pub fn with_table_rule(
        &mut self,
        column_name: impl AsRef<str>,
        table_rule: Arc<dyn TableRule>,
        check: Option<Arc<dyn ColumnRule>>,
    ) -> &mut Self {
        let column_name = column_name.as_ref().to_string();
        if let Some(check) = check {
            let column_name = table_rule.new_column_name(&column_name);
            self.column_rules.push((column_name, check));
        }
        self.table_rules.push((column_name, table_rule));
        self
    }

    pub async fn apply_table_rules(&self, df: DataFrame) -> Result<DataFrame, ValidationError> {
        let mut result_df = df;
        for (column_name, rule) in &self.table_rules {
            result_df = rule.apply_with_ruleset(result_df, column_name, self)?;
        }
        Ok(result_df)
    }

    /// Apply all rules to a DataFrame
    pub async fn apply(&self, df: &DataFrame) -> Result<DataFrame, ValidationError> {
        // First validate schema
        for rule in &self.schema_rules {
            if !rule.validate_schema(df.schema())? {
                return Err(ValidationError::Schema {
                    message: format!("Schema rule '{}' failed", rule.name()),
                });
            }
        }

        let mut result_df = df.clone();
        // Apply table calculations
        result_df = self.apply_table_rules(result_df).await?;

        let mut check_columns = Vec::new();

        // Then apply column rules
        for (column_name, rule) in &self.column_rules {
            result_df = rule.apply_with_ruleset(result_df, column_name, self)?;
            check_columns.push(rule.new_column_name(column_name));
        }

        let dq_pass_col = check_columns
            .into_iter()
            .map(|col_name| {
                col(col_name)
                    .cast_to(&arrow::datatypes::DataType::Boolean, result_df.schema())
                    .map_err(|e| ValidationError::Column {
                        message: format!("Error casting column to boolean: {}", e),
                    })
            })
            .reduce(|acc, col| Ok(acc?.and(col?)))
            .unwrap_or(Ok(lit(true)))?;

        result_df = result_df.with_column("dfq_pass", dq_pass_col)?;

        Ok(result_df)
    }

    pub async fn partition(
        &self,
        df: &DataFrame,
    ) -> Result<(DataFrame, DataFrame), ValidationError> {
        let dq_df = self.apply(df).await?.cache().await?;

        let pass_expr = col("dfq_pass").eq(lit(true));
        let pass_df = dq_df.clone().filter(pass_expr.clone())?.select_columns(
            &df.schema()
                .fields()
                .iter()
                .map(|s| s.name().as_str())
                .collect::<Vec<&str>>(),
        )?;
        let fail_df = dq_df.filter(pass_expr.not())?;
        Ok((pass_df, fail_df))
    }

    pub async fn derived_statistics(
        &self,
        df: &DataFrame,
        extra_columns: Option<Vec<&str>>,
    ) -> Result<DataFrame, ValidationError> {
        let dq_df = self.apply(df).await?;

        let mut table_rules_names = Vec::new();
        if let Some(extra_columns) = extra_columns {
            table_rules_names.extend(extra_columns.iter().map(|s| col(*s)));
        }

        for (column_name, rule) in &self.table_rules {
            table_rules_names.push(col(rule.new_column_name(column_name)));
        }

        dq_df.select(table_rules_names).context(DataFusionSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::column::*;
    use crate::rules::table::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::assert_batches_eq;
    use std::sync::Arc;

    async fn create_test_df() -> (SessionContext, DataFrame) {
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

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(id_data),
            Arc::new(name_data),
            Arc::new(age_data),
            Arc::new(score_data),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch.clone()).unwrap();
        ctx.register_batch("test_data", batch).unwrap();

        (ctx, df)
    }

    #[tokio::test]
    async fn test_partition() {
        // Create test DataFrame
        let (_ctx, df) = create_test_df().await;

        // Verify initial test data
        let expected_data = vec![
            "+----+---------+-----+-------+",
            "| id | name    | age | score |",
            "+----+---------+-----+-------+",
            "| 1  | Alice   | 25  | 85.5  |",
            "| 2  | Bob     | 30  | 92.0  |",
            "| 3  |         | 15  | 78.5  |",
            "| 4  | Charlie | 40  | 95.0  |",
            "| 5  | Dave    | 20  | 88.5  |",
            "+----+---------+-----+-------+",
        ];
        assert_batches_eq!(&expected_data, &df.clone().collect().await.unwrap());

        // Create RuleSet
        let mut rule_set = RuleSet::new();

        // Add column rules
        rule_set
            .with_column_rule("name", dfq_not_null())
            .with_column_rule("score", dfq_in_range(80.0, 100.0));

        // Add table rule with its own column rule
        rule_set.with_table_rule("name", dfq_null_count(), Some(dfq_in_range(0.0, 10.0)));

        // Apply partition
        let (pass_df, fail_df) = rule_set.partition(&df).await.unwrap();

        // Expected pass DataFrame (rows where name is not null AND score is between 80 and 100)
        let expected_pass = vec![
            "+----+---------+-----+-------+",
            "| id | name    | age | score |",
            "+----+---------+-----+-------+",
            "| 1  | Alice   | 25  | 85.5  |",
            "| 2  | Bob     | 30  | 92.0  |",
            "| 4  | Charlie | 40  | 95.0  |",
            "| 5  | Dave    | 20  | 88.5  |",
            "+----+---------+-----+-------+",
        ];

        // Expected fail DataFrame (rows where name is null OR score is not between 80 and 100)
        let expected_fail = vec![
            "+----+------+-----+-------+-----------------+---------------+----------------+--------------------------+----------+",
            "| id | name | age | score | name_null_count | name_not_null | score_in_range | name_null_count_in_range | dfq_pass |",
            "+----+------+-----+-------+-----------------+---------------+----------------+--------------------------+----------+",
            "| 3  |      | 15  | 78.5  | 1               | false         | false          | true                     | false    |",
            "+----+------+-----+-------+-----------------+---------------+----------------+--------------------------+----------+",
        ];

        // Compare results
        assert_batches_eq!(&expected_pass, &pass_df.collect().await.unwrap());
        assert_batches_eq!(&expected_fail, &fail_df.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_derived_statistics() {
        // Create test DataFrame
        let (_, df) = create_test_df().await;

        // Create RuleSet with table rules
        let mut rule_set = RuleSet::new();

        // Add various table rules
        rule_set
            .with_table_rule("score", dfq_avg(), None)
            .with_table_rule("score", dfq_stddev(), None)
            .with_table_rule("age", dfq_null_count(), None);

        // Get derived statistics
        let stats_df = rule_set
            .derived_statistics(&df, Some(vec!["id"]))
            .await
            .unwrap();

        let expected = vec![
            "+----+-----------+-------------------+----------------+",
            "| id | score_avg | score_stddev      | age_null_count |",
            "+----+-----------+-------------------+----------------+",
            "| 1  | 87.9      | 6.358065743604732 | 0              |",
            "| 2  | 87.9      | 6.358065743604732 | 0              |",
            "| 3  | 87.9      | 6.358065743604732 | 0              |",
            "| 4  | 87.9      | 6.358065743604732 | 0              |",
            "| 5  | 87.9      | 6.358065743604732 | 0              |",
            "+----+-----------+-------------------+----------------+",
        ];

        assert_batches_eq!(&expected, &stats_df.collect().await.unwrap());
    }
}
