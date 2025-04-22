use crate::{ColumnRule, ValidationError, error::DataFusionSnafu};
use datafusion::{logical_expr::Between, prelude::*};
use snafu::ResultExt;
use std::sync::Arc;

/// Rule that checks if values in a column are not null
#[derive(Debug, Clone, Default)]
pub struct NullRule {
    negated: Option<bool>,
}

impl NullRule {
    pub fn new(negated: Option<bool>) -> Self {
        Self { negated }
    }
}

impl ColumnRule for NullRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let col = col(column_name);
        let is_not_null = if self.negated.unwrap_or_default() {
            col.is_not_null()
        } else {
            col.is_null()
        };

        df.with_column(&self.new_column_name(column_name), is_not_null)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        if self.negated.unwrap_or_default() {
            "not_null"
        } else {
            "null"
        }
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.name())
    }

    fn description(&self) -> &str {
        "Checks if values in a column are null/not null"
    }
}

/// Creates a rule that checks if values in a column are not null.
///
/// # Arguments
///
/// * `column_name` - The name of the column to check
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_null;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if age column is not null
/// let rule = dfq_not_null();
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_not_null() -> Arc<NullRule> {
    Arc::new(NullRule::new(Some(true)))
}

/// Creates a rule that checks if values in a column are null.
///
/// # Arguments
///
/// * `column_name` - The name of the column to check
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_null;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if age column is null
/// let rule = dfq_null();
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_null() -> Arc<NullRule> {
    Arc::new(NullRule::new(Some(false)))
}

/// Rule that checks if values in a column fall within a specified range
#[derive(Debug, Clone)]
pub struct RangeRule {
    min: f64,
    max: f64,
    negated: Option<bool>,
}

impl RangeRule {
    pub fn new(min: f64, max: f64, negated: Option<bool>) -> Self {
        Self { min, max, negated }
    }
}

impl ColumnRule for RangeRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let col = col(column_name);
        let in_range = Expr::Between(Between {
            expr: Box::new(col),
            negated: self.negated.unwrap_or(false),
            low: Box::new(lit(self.min)),
            high: Box::new(lit(self.max)),
        });

        df.with_column(&self.new_column_name(column_name), in_range)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        if self.negated.unwrap_or_default() {
            "not_in_range"
        } else {
            "in_range"
        }
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.name())
    }

    fn description(&self) -> &str {
        "Checks if values in a column (does not) fall within a specified range"
    }
}

/// Creates a rule that checks if values in a column fall within a specified range.
///
/// # Arguments
///
/// * `min` - The minimum value of the range (inclusive)
/// * `max` - The maximum value of the range (inclusive)
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_in_range;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if score is between 0 and 100
/// let rule = dfq_in_range(0.0, 100.0);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("score", rule);
/// ```
pub fn dfq_in_range(min: f64, max: f64) -> Arc<RangeRule> {
    Arc::new(RangeRule::new(min, max, None))
}

/// Creates a rule that checks if values in a column do not fall within a specified range.
///
/// # Arguments
///
/// * `min` - The minimum value of the range (inclusive)
/// * `max` - The maximum value of the range (inclusive)
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_in_range;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if score is not between 0 and 100
/// let rule = dfq_not_in_range(0.0, 100.0);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("score", rule);
/// ```
pub fn dfq_not_in_range(min: f64, max: f64) -> Arc<RangeRule> {
    Arc::new(RangeRule::new(min, max, Some(true)))
}

/// Rule that checks if values in a column match a pattern
#[derive(Debug, Clone)]
pub struct PatternRule {
    pattern: String,
    negated: Option<bool>,
    case_sensitive: Option<bool>,
}

impl PatternRule {
    pub fn new(pattern: &str, negated: Option<bool>, case_sensitive: Option<bool>) -> Self {
        Self {
            pattern: pattern.to_string(),
            negated,
            case_sensitive,
        }
    }
}

impl ColumnRule for PatternRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let col = col(column_name);
        let matches_pattern = match (
            self.negated.unwrap_or_default(),
            self.case_sensitive.unwrap_or_default(),
        ) {
            (true, true) => col.not_like(lit(&self.pattern)),
            (false, true) => col.like(lit(&self.pattern)),
            (true, false) => col.not_ilike(lit(&self.pattern)),
            (false, false) => col.ilike(lit(&self.pattern)),
        };

        df.with_column(&self.new_column_name(column_name), matches_pattern)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        match (
            self.negated.unwrap_or_default(),
            self.case_sensitive.unwrap_or_default(),
        ) {
            (true, true) => "not_like",
            (false, true) => "like",
            (true, false) => "not_ilike",
            (false, false) => "ilike",
        }
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.name())
    }

    fn description(&self) -> &str {
        "Checks if values in a column match a pattern"
    }
}

/// Creates a rule that checks if values in a column match a pattern (case-sensitive).
///
/// # Arguments
///
/// * `pattern` - The SQL LIKE pattern to match against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_like;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name starts with 'A'
/// let rule = dfq_like("A%");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_like(pattern: &str) -> Arc<PatternRule> {
    Arc::new(PatternRule::new(pattern, Some(false), Some(true)))
}

/// Creates a rule that checks if values in a column do not match a pattern (case-sensitive).
///
/// # Arguments
///
/// * `pattern` - The SQL LIKE pattern to match against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_like;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name does not start with 'A'
/// let rule = dfq_not_like("A%");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_not_like(pattern: &str) -> Arc<PatternRule> {
    Arc::new(PatternRule::new(pattern, Some(true), Some(true)))
}

/// Creates a rule that checks if values in a column match a pattern (case-insensitive).
///
/// # Arguments
///
/// * `pattern` - The SQL LIKE pattern to match against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_ilike;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name starts with 'a' (case-insensitive)
/// let rule = dfq_ilike("a%");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_ilike(pattern: &str) -> Arc<PatternRule> {
    Arc::new(PatternRule::new(pattern, Some(false), Some(false)))
}

/// Creates a rule that checks if values in a column do not match a pattern (case-insensitive).
///
/// # Arguments
///
/// * `pattern` - The SQL LIKE pattern to match against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_ilike;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name does not start with 'a' (case-insensitive)
/// let rule = dfq_not_ilike("a%");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_not_ilike(pattern: &str) -> Arc<PatternRule> {
    Arc::new(PatternRule::new(pattern, Some(true), Some(false)))
}

/// Rule that checks if values in a column are less than a value
#[derive(Debug, Clone)]
pub struct ComparisonRule {
    value: Expr,
    negated: bool,
    equals: bool,
    comparison_type: ComparisonType,
}

#[derive(Debug, Clone, Copy)]
pub enum ComparisonType {
    LessThan,
    GreaterThan,
    Equals,
}

impl ComparisonRule {
    pub fn new(value: Expr, negated: bool, equals: bool, comparison_type: ComparisonType) -> Self {
        Self {
            value,
            negated,
            equals,
            comparison_type,
        }
    }
}

impl ColumnRule for ComparisonRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let col = col(column_name);
        let comparison = match (self.comparison_type, self.equals) {
            (ComparisonType::LessThan, true) => col.lt_eq(self.value.clone()),
            (ComparisonType::LessThan, false) => col.lt(self.value.clone()),
            (ComparisonType::GreaterThan, true) => col.gt_eq(self.value.clone()),
            (ComparisonType::GreaterThan, false) => col.gt(self.value.clone()),
            (ComparisonType::Equals, _) => col.eq(self.value.clone()),
        };

        let expr = if self.negated {
            comparison.not()
        } else {
            comparison
        };

        df.with_column(&self.new_column_name(column_name), expr)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        match (self.comparison_type, self.negated, self.equals) {
            (ComparisonType::LessThan, false, false) => "less_than",
            (ComparisonType::LessThan, false, true) => "less_than_equals",
            (ComparisonType::LessThan, true, false) => "not_less_than",
            (ComparisonType::LessThan, true, true) => "not_less_than_equals",
            (ComparisonType::GreaterThan, false, false) => "greater_than",
            (ComparisonType::GreaterThan, false, true) => "greater_than_equals",
            (ComparisonType::GreaterThan, true, false) => "not_greater_than",
            (ComparisonType::GreaterThan, true, true) => "not_greater_than_equals",
            (ComparisonType::Equals, false, _) => "equals",
            (ComparisonType::Equals, true, _) => "not_equals",
        }
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.name())
    }

    fn description(&self) -> &str {
        "Checks if values in a column satisfy a comparison with a value"
    }
}

/// Creates a rule that checks if values in a column are less than a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_lt;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is less than 30
/// let rule = dfq_lt(lit(30));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_lt(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        false,
        false,
        ComparisonType::LessThan,
    ))
}

/// Creates a rule that checks if values in a column are less than or equal to a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_lte;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is less than or equal to 30
/// let rule = dfq_lte(lit(30));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_lte(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        false,
        true,
        ComparisonType::LessThan,
    ))
}

/// Creates a rule that checks if values in a column are not less than a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_lt;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is not less than 30
/// let rule = dfq_not_lt(lit(30));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_not_lt(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        true,
        false,
        ComparisonType::LessThan,
    ))
}

/// Creates a rule that checks if values in a column are not less than or equal to a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_lte;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is not less than or equal to 30
/// let rule = dfq_not_lte(lit(30));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_not_lte(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        true,
        true,
        ComparisonType::LessThan,
    ))
}

/// Creates a rule that checks if values in a column are greater than a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_gt;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is greater than 25
/// let rule = dfq_gt(lit(25));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_gt(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        false,
        false,
        ComparisonType::GreaterThan,
    ))
}

/// Creates a rule that checks if values in a column are greater than or equal to a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_gte;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is greater than or equal to 25
/// let rule = dfq_gte(lit(25));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_gte(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        false,
        true,
        ComparisonType::GreaterThan,
    ))
}

/// Creates a rule that checks if values in a column are not greater than a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_gt;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is not greater than 25
/// let rule = dfq_not_gt(lit(25));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_not_gt(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        true,
        false,
        ComparisonType::GreaterThan,
    ))
}

/// Creates a rule that checks if values in a column are not greater than or equal to a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_gte;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is not greater than or equal to 25
/// let rule = dfq_not_gte(lit(25));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_not_gte(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        true,
        true,
        ComparisonType::GreaterThan,
    ))
}

/// Creates a rule that checks if values in a column are equal to a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_eq;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is equal to 25
/// let rule = dfq_eq(lit(25));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_eq(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        false,
        false,
        ComparisonType::Equals,
    ))
}

/// Creates a rule that checks if values in a column are not equal to a value.
///
/// # Arguments
///
/// * `value` - The value to compare against
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_not_eq;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a rule to check if age is not equal to 25
/// let rule = dfq_not_eq(lit(25));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_not_eq(value: Expr) -> Arc<ComparisonRule> {
    Arc::new(ComparisonRule::new(
        value,
        true,
        false,
        ComparisonType::Equals,
    ))
}

#[derive(Debug, Clone)]
pub struct LengthRule {
    min: Option<u32>,
    max: Option<u32>,
}

impl LengthRule {
    pub fn new(min: Option<u32>, max: Option<u32>) -> Self {
        Self { min, max }
    }
}

impl ColumnRule for LengthRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let mut expr = char_length(col(column_name));

        match (self.min, self.max) {
            (Some(min), Some(max)) => {
                expr = expr.between(lit(min), lit(max));
            }
            (Some(min), None) => {
                expr = expr.gt_eq(lit(min));
            }
            (None, Some(max)) => {
                expr = expr.lt(lit(max));
            }
            (None, None) => {
                return Err(ValidationError::Configuration {
                    message: "Length rule must have either a minimum or maximum length".to_string(),
                });
            }
        }

        df.with_column(&self.new_column_name(column_name), expr)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        match (self.min, self.max) {
            (Some(_), Some(_)) => "length_range",
            (Some(_), None) => "min_length",
            (None, Some(_)) => "max_length",
            (None, None) => "length",
        }
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_length", column_name)
    }

    fn description(&self) -> &str {
        "Checks if the length of a column is between a minimum and maximum value"
    }
}

/// Creates a rule that checks if the length of a string column is between a minimum and maximum value.
///
/// # Arguments
///
/// * `min` - Optional minimum length (inclusive)
/// * `max` - Optional maximum length (inclusive)
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_str_length;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name length is between 3 and 10 characters
/// let rule = dfq_str_length(Some(3), Some(10));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_str_length(min: Option<u32>, max: Option<u32>) -> Arc<LengthRule> {
    Arc::new(LengthRule::new(min, max))
}

/// Creates a rule that checks if the length of a string column is at least a minimum value.
///
/// # Arguments
///
/// * `min` - Minimum length (inclusive)
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_str_min_length;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name is at least 3 characters long
/// let rule = dfq_str_min_length(3);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_str_min_length(min: u32) -> Arc<LengthRule> {
    Arc::new(LengthRule::new(Some(min), None))
}

/// Creates a rule that checks if the length of a string column is at most a maximum value.
///
/// # Arguments
///
/// * `max` - Maximum length (inclusive)
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_str_max_length;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name is at most 10 characters long
/// let rule = dfq_str_max_length(10);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_str_max_length(max: u32) -> Arc<LengthRule> {
    Arc::new(LengthRule::new(None, Some(max)))
}

/// Creates a rule that checks if a string column is empty (length = 0).
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_str_empty;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name is empty
/// let rule = dfq_str_empty();
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_str_empty() -> Arc<LengthRule> {
    Arc::new(LengthRule::new(None, Some(0)))
}

/// Creates a rule that checks if a string column is not empty (length > 0).
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_str_not_empty;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if name is not empty
/// let rule = dfq_str_not_empty();
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("name", rule);
/// ```
pub fn dfq_str_not_empty() -> Arc<LengthRule> {
    Arc::new(LengthRule::new(Some(1), None))
}

/// Rule that applies a custom SQL expression to a column
#[derive(Debug, Clone)]
pub struct CustomRule {
    rule_name: String,
    expression: Expr,
}

impl CustomRule {
    pub fn new(rule_name: &str, expression: Expr) -> Self {
        Self {
            rule_name: rule_name.to_string(),
            expression,
        }
    }
}

impl ColumnRule for CustomRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let expr = self.expression.clone();
        df.with_column(&self.new_column_name(column_name), expr)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        "custom"
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.rule_name)
    }

    fn description(&self) -> &str {
        "Applies a custom SQL expression to a column"
    }
}

/// Creates a rule that applies a custom SQL expression to a column.
///
/// # Arguments
///
/// * `rule_name` - A name for the custom rule
/// * `expression` - The SQL expression to apply
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::column::dfq_custom;
/// use datafusion_quality::RuleSet;
/// use datafusion::prelude::*;
///
/// // Create a custom rule to check if age is greater than 25
/// let rule = dfq_custom("age_gt_25", col("age").gt(lit(25)));
/// let mut ruleset = RuleSet::new();
/// ruleset.with_column_rule("age", rule);
/// ```
pub fn dfq_custom(rule_name: &str, expression: Expr) -> Arc<CustomRule> {
    Arc::new(CustomRule::new(rule_name, expression))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;

    async fn create_test_df() -> DataFrame {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("score", DataType::Float64, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(Int32Array::from(vec![Some(25), None, Some(30)])),
                Arc::new(Float64Array::from(vec![Some(85.5), Some(92.0), None])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.read_batch(batch).unwrap()
    }

    #[tokio::test]
    async fn test_not_null_rule() {
        let df = create_test_df().await;
        let rule = dfq_null();
        let result = rule.apply(df.clone(), "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------+",
            "| id | name    | age | score | age_null |",
            "+----+---------+-----+-------+----------+",
            "| 1  | Alice   | 25  | 85.5  | false    |",
            "| 2  | Bob     |     | 92.0  | true     |",
            "| 3  | Charlie | 30  |       | false    |",
            "+----+---------+-----+-------+----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test negated not null rule
        let df = create_test_df().await;
        let rule = dfq_not_null();
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------+",
            "| id | name    | age | score | age_not_null |",
            "+----+---------+-----+-------+--------------+",
            "| 1  | Alice   | 25  | 85.5  | true         |",
            "| 2  | Bob     |     | 92.0  | false        |",
            "| 3  | Charlie | 30  |       | true         |",
            "+----+---------+-----+-------+--------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_range_rule() {
        let df = create_test_df().await;
        let rule = dfq_in_range(0.0, 100.0);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_in_range |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | true           |",
            "| 2  | Bob     |     | 92.0  | true           |",
            "| 3  | Charlie | 30  |       |                |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test negated range rule
        let df = create_test_df().await;
        let rule = dfq_not_in_range(0.0, 100.0);
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_not_in_range |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | false              |",
            "| 2  | Bob     |     | 92.0  | false              |",
            "| 3  | Charlie | 30  |       |                    |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_pattern_rule() {
        // Test case sensitive pattern match
        let df = create_test_df().await;
        let rule = dfq_like("A%");
        let result = rule.apply(df, "name").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------+",
            "| id | name    | age | score | name_like |",
            "+----+---------+-----+-------+-----------+",
            "| 1  | Alice   | 25  | 85.5  | true      |",
            "| 2  | Bob     |     | 92.0  | false     |",
            "| 3  | Charlie | 30  |       | false     |",
            "+----+---------+-----+-------+-----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test case insensitive pattern match
        let df = create_test_df().await;
        let rule = dfq_ilike("a%");
        let result = rule.apply(df, "name").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+------------+",
            "| id | name    | age | score | name_ilike |",
            "+----+---------+-----+-------+------------+",
            "| 1  | Alice   | 25  | 85.5  | true       |",
            "| 2  | Bob     |     | 92.0  | false      |",
            "| 3  | Charlie | 30  |       | false      |",
            "+----+---------+-----+-------+------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test negated case sensitive pattern match
        let df = create_test_df().await;
        let rule = dfq_not_like("A%");
        let result = rule.apply(df, "name").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | name_not_like |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | false         |",
            "| 2  | Bob     |     | 92.0  | true          |",
            "| 3  | Charlie | 30  |       | true          |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test negated case insensitive pattern match
        let df = create_test_df().await;
        let rule = dfq_not_ilike("a%");
        let result = rule.apply(df, "name").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | name_not_ilike |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | false          |",
            "| 2  | Bob     |     | 92.0  | true           |",
            "| 3  | Charlie | 30  |       | true           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_custom_rule() {
        let df = create_test_df().await;
        let rule = dfq_custom("age_gt_25", col("age").gt(lit(25)));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | age_age_gt_25 |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | false         |",
            "| 2  | Bob     |     | 92.0  |               |",
            "| 3  | Charlie | 30  |       | true          |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_less_than_rule() {
        let df = create_test_df().await;
        let rule = dfq_lt(lit(30));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | age_less_than |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | true          |",
            "| 2  | Bob     |     | 92.0  |               |",
            "| 3  | Charlie | 30  |       | false         |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_less_than_equals_rule() {
        let df = create_test_df().await;
        let rule = dfq_lte(lit(30));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------------+",
            "| id | name    | age | score | age_less_than_equals |",
            "+----+---------+-----+-------+----------------------+",
            "| 1  | Alice   | 25  | 85.5  | true                 |",
            "| 2  | Bob     |     | 92.0  |                      |",
            "| 3  | Charlie | 30  |       | true                 |",
            "+----+---------+-----+-------+----------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_not_less_than_rule() {
        let df = create_test_df().await;
        let rule = dfq_not_lt(lit(30));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------+",
            "| id | name    | age | score | age_not_less_than |",
            "+----+---------+-----+-------+-------------------+",
            "| 1  | Alice   | 25  | 85.5  | false             |",
            "| 2  | Bob     |     | 92.0  |                   |",
            "| 3  | Charlie | 30  |       | true              |",
            "+----+---------+-----+-------+-------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_not_less_than_equals_rule() {
        let df = create_test_df().await;
        let rule = dfq_not_lte(lit(30));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------------+",
            "| id | name    | age | score | age_not_less_than_equals |",
            "+----+---------+-----+-------+--------------------------+",
            "| 1  | Alice   | 25  | 85.5  | false                    |",
            "| 2  | Bob     |     | 92.0  |                          |",
            "| 3  | Charlie | 30  |       | false                    |",
            "+----+---------+-----+-------+--------------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_greater_than_rule() {
        let df = create_test_df().await;
        let rule = dfq_gt(lit(25));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+------------------+",
            "| id | name    | age | score | age_greater_than |",
            "+----+---------+-----+-------+------------------+",
            "| 1  | Alice   | 25  | 85.5  | false            |",
            "| 2  | Bob     |     | 92.0  |                  |",
            "| 3  | Charlie | 30  |       | true             |",
            "+----+---------+-----+-------+------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_greater_than_equals_rule() {
        let df = create_test_df().await;
        let rule = dfq_gte(lit(25));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------------+",
            "| id | name    | age | score | age_greater_than_equals |",
            "+----+---------+-----+-------+-------------------------+",
            "| 1  | Alice   | 25  | 85.5  | true                    |",
            "| 2  | Bob     |     | 92.0  |                         |",
            "| 3  | Charlie | 30  |       | true                    |",
            "+----+---------+-----+-------+-------------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_not_greater_than_rule() {
        let df = create_test_df().await;
        let rule = dfq_not_gt(lit(25));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------------+",
            "| id | name    | age | score | age_not_greater_than |",
            "+----+---------+-----+-------+----------------------+",
            "| 1  | Alice   | 25  | 85.5  | true                 |",
            "| 2  | Bob     |     | 92.0  |                      |",
            "| 3  | Charlie | 30  |       | false                |",
            "+----+---------+-----+-------+----------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_not_greater_than_equals_rule() {
        let df = create_test_df().await;
        let rule = dfq_not_gte(lit(25));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------------------------+",
            "| id | name    | age | score | age_not_greater_than_equals |",
            "+----+---------+-----+-------+-----------------------------+",
            "| 1  | Alice   | 25  | 85.5  | false                       |",
            "| 2  | Bob     |     | 92.0  |                             |",
            "| 3  | Charlie | 30  |       | false                       |",
            "+----+---------+-----+-------+-----------------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_equals_rule() {
        let df = create_test_df().await;
        let rule = dfq_eq(lit(25));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+------------+",
            "| id | name    | age | score | age_equals |",
            "+----+---------+-----+-------+------------+",
            "| 1  | Alice   | 25  | 85.5  | true       |",
            "| 2  | Bob     |     | 92.0  |            |",
            "| 3  | Charlie | 30  |       | false      |",
            "+----+---------+-----+-------+------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_not_equals_rule() {
        let df = create_test_df().await;
        let rule = dfq_not_eq(lit(25));
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | age_not_equals |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | false          |",
            "| 2  | Bob     |     | 92.0  |                |",
            "| 3  | Charlie | 30  |       | true           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_string_length_rules() {
        // Create a test dataframe with strings of various lengths
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some(""),       // empty string
                    Some("a"),      // length 1
                    Some("abc"),    // length 3
                    Some("abcdef"), // length 6
                    None,           // null
                ])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();

        // Test dfq_str_length with min=2 and max=5
        let rule = dfq_str_length(Some(2), Some(5));
        let result = rule.apply(df.clone(), "text").unwrap();

        let expected = vec![
            "+----+--------+-------------+",
            "| id | text   | text_length |",
            "+----+--------+-------------+",
            "| 1  |        | false       |",
            "| 2  | a      | false       |",
            "| 3  | abc    | true        |",
            "| 4  | abcdef | false       |",
            "| 5  |        |             |",
            "+----+--------+-------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test dfq_str_min_length with min=3
        let rule = dfq_str_min_length(3);
        let result = rule.apply(df.clone(), "text").unwrap();

        let expected = vec![
            "+----+--------+-------------+",
            "| id | text   | text_length |",
            "+----+--------+-------------+",
            "| 1  |        | false       |",
            "| 2  | a      | false       |",
            "| 3  | abc    | true        |",
            "| 4  | abcdef | true        |",
            "| 5  |        |             |",
            "+----+--------+-------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test dfq_str_max_length with max=3
        let rule = dfq_str_max_length(3);
        let result = rule.apply(df.clone(), "text").unwrap();

        let expected = vec![
            "+----+--------+-------------+",
            "| id | text   | text_length |",
            "+----+--------+-------------+",
            "| 1  |        | true        |",
            "| 2  | a      | true        |",
            "| 3  | abc    | false       |",
            "| 4  | abcdef | false       |",
            "| 5  |        |             |",
            "+----+--------+-------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test dfq_str_empty
        let rule = dfq_str_empty();
        let result = rule.apply(df.clone(), "text").unwrap();

        let expected = vec![
            "+----+--------+-------------+",
            "| id | text   | text_length |",
            "+----+--------+-------------+",
            "| 1  |        | false       |",
            "| 2  | a      | false       |",
            "| 3  | abc    | false       |",
            "| 4  | abcdef | false       |",
            "| 5  |        |             |",
            "+----+--------+-------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test dfq_str_not_empty
        let rule = dfq_str_not_empty();
        let result = rule.apply(df, "text").unwrap();

        let expected = vec![
            "+----+--------+-------------+",
            "| id | text   | text_length |",
            "+----+--------+-------------+",
            "| 1  |        | false       |",
            "| 2  | a      | true        |",
            "| 3  | abc    | true        |",
            "| 4  | abcdef | true        |",
            "| 5  |        |             |",
            "+----+--------+-------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }
}
