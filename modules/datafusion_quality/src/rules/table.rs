use crate::{TableRule, ValidationError, error::DataFusionSnafu};

use datafusion::functions_aggregate::{count::count_all, expr_fn::*};
use datafusion::logical_expr::{SortExpr, Subquery};
use datafusion::prelude::*;
use snafu::ResultExt;
use std::sync::Arc;

/// Rule that counts null values in a column across the entire table
#[derive(Debug, Clone, Default)]
pub struct NullCountRule {
    negated: Option<bool>,
}

impl TableRule for NullCountRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let new_column_name = self.new_column_name(column_name);
        let subquery = if !self.negated.unwrap_or(false) {
            df.clone()
                .aggregate(
                    vec![],
                    vec![
                        count_all().alias("count_all"),
                        count(col(column_name)).alias(new_column_name.as_str()),
                    ],
                )?
                .select(vec![
                    col("count_all")
                        .sub(col(new_column_name.as_str()))
                        .alias(new_column_name.as_str()),
                ])?
        } else {
            df.clone()
                .aggregate(vec![], vec![count(col(column_name)).alias("count_all")])?
                .select(vec![col("count_all").alias(new_column_name.as_str())])?
        };

        let subquery_expr = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(subquery.logical_plan().clone()),
            outer_ref_columns: vec![],
        });

        df.with_column(&new_column_name, subquery_expr)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        if self.negated.unwrap_or(false) {
            "not_null_count"
        } else {
            "null_count"
        }
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.name())
    }

    fn description(&self) -> &str {
        "Counts the number of (not) null values in a column across the entire table"
    }
}

pub fn dfq_null_count() -> Arc<NullCountRule> {
    std::sync::Arc::new(NullCountRule { negated: None })
}

pub fn dfq_not_null_count() -> Arc<NullCountRule> {
    std::sync::Arc::new(NullCountRule {
        negated: Some(true),
    })
}

#[derive(Debug, strum::Display, Clone)]
pub enum CalculationType {
    Count,
    CountDistinct,
    Avg,
    StdDev,
    Max,
    Min,
    Sum,
    Median,
    CovarPop { x: Option<Expr>, y: Option<Expr> },
    CovarSamp { x: Option<Expr>, y: Option<Expr> },
    FirstValue(Option<Vec<SortExpr>>),
    LastValue,
    NthValue(i64, Option<Vec<SortExpr>>),
    RegrAvgX { x: Option<Expr>, y: Option<Expr> },
    RegrAvgY { x: Option<Expr>, y: Option<Expr> },
    RegrCount { x: Option<Expr>, y: Option<Expr> },
    RegrIntercept { x: Option<Expr>, y: Option<Expr> },
    RegrR2 { x: Option<Expr>, y: Option<Expr> },
    RegrSlope { x: Option<Expr>, y: Option<Expr> },
    RegrSxx { x: Option<Expr>, y: Option<Expr> },
    RegrSxy { x: Option<Expr>, y: Option<Expr> },
    RegrSyy { x: Option<Expr>, y: Option<Expr> },
    StddevPop,
    VarPop,
    VarSamp,
}

#[derive(Debug, Clone)]
pub struct CalculationRule {
    calculation_type: CalculationType,
}

impl TableRule for CalculationRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let new_column_name = self.new_column_name(column_name);
        let source_column = col(column_name);
        let calc_expr = match self.calculation_type.clone() {
            CalculationType::Count => count(source_column),
            CalculationType::CountDistinct => count_distinct(source_column),
            CalculationType::Avg => avg(source_column),
            CalculationType::StdDev => stddev(source_column),
            CalculationType::Max => max(source_column),
            CalculationType::Min => min(source_column),
            CalculationType::Sum => sum(source_column),
            CalculationType::Median => median(source_column),
            CalculationType::CovarPop {
                x: Some(x),
                y: None,
            } => covar_pop(source_column, x),
            CalculationType::CovarPop {
                x: None,
                y: Some(y),
            } => covar_pop(y, source_column),
            CalculationType::CovarPop { .. } => {
                return Err(ValidationError::Configuration {
                    message: "CovarPop must have either x or y".to_string(),
                });
            }
            CalculationType::CovarSamp {
                x: Some(x),
                y: None,
            } => covar_samp(source_column, x),
            CalculationType::CovarSamp {
                x: None,
                y: Some(y),
            } => covar_samp(y, source_column),
            CalculationType::CovarSamp { .. } => {
                return Err(ValidationError::Configuration {
                    message: "CovarSamp must have either x or y".to_string(),
                });
            }
            CalculationType::FirstValue(sort_exprs) => first_value(source_column, sort_exprs),
            CalculationType::LastValue => last_value(vec![source_column]),
            CalculationType::NthValue(n, sort_exprs) => {
                nth_value(source_column, n, sort_exprs.unwrap_or_default())
            }
            CalculationType::RegrAvgX {
                x: Some(x),
                y: None,
            } => regr_avgx(source_column, x),
            CalculationType::RegrAvgX {
                x: None,
                y: Some(y),
            } => regr_avgx(y, source_column),
            CalculationType::RegrAvgX { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrAvgX must have either x or y".to_string(),
                });
            }
            CalculationType::RegrAvgY {
                x: Some(x),
                y: None,
            } => regr_avgy(source_column, x),
            CalculationType::RegrAvgY {
                x: None,
                y: Some(y),
            } => regr_avgy(y, source_column),
            CalculationType::RegrAvgY { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrAvgY must have either x or y".to_string(),
                });
            }
            CalculationType::RegrCount {
                x: Some(x),
                y: None,
            } => regr_count(source_column, x),
            CalculationType::RegrCount {
                x: None,
                y: Some(y),
            } => regr_count(y, source_column),
            CalculationType::RegrCount { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrCount must have either x or y".to_string(),
                });
            }
            CalculationType::RegrIntercept {
                x: Some(x),
                y: None,
            } => regr_intercept(source_column, x),
            CalculationType::RegrIntercept {
                x: None,
                y: Some(y),
            } => regr_intercept(y, source_column),
            CalculationType::RegrIntercept { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrIntercept must have either x or y".to_string(),
                });
            }
            CalculationType::RegrR2 {
                x: Some(x),
                y: None,
            } => regr_r2(source_column, x),
            CalculationType::RegrR2 {
                x: None,
                y: Some(y),
            } => regr_r2(y, source_column),
            CalculationType::RegrR2 { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrR2 must have either x or y".to_string(),
                });
            }
            CalculationType::RegrSlope {
                x: Some(x),
                y: None,
            } => regr_slope(source_column, x),
            CalculationType::RegrSlope {
                x: None,
                y: Some(y),
            } => regr_slope(y, source_column),
            CalculationType::RegrSlope { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrSlope must have either x or y".to_string(),
                });
            }
            CalculationType::RegrSxx {
                x: Some(x),
                y: None,
            } => regr_sxx(source_column, x),
            CalculationType::RegrSxx {
                x: None,
                y: Some(y),
            } => regr_sxx(y, source_column),
            CalculationType::RegrSxx { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrSxx must have either x or y".to_string(),
                });
            }
            CalculationType::RegrSxy {
                x: Some(x),
                y: None,
            } => regr_sxy(source_column, x),
            CalculationType::RegrSxy {
                x: None,
                y: Some(y),
            } => regr_sxy(y, source_column),
            CalculationType::RegrSxy { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrSxy must have either x or y".to_string(),
                });
            }
            CalculationType::RegrSyy {
                x: Some(x),
                y: None,
            } => regr_syy(source_column, x),
            CalculationType::RegrSyy {
                x: None,
                y: Some(y),
            } => regr_syy(y, source_column),
            CalculationType::RegrSyy { .. } => {
                return Err(ValidationError::Configuration {
                    message: "RegrSyy must have either x or y".to_string(),
                });
            }
            CalculationType::StddevPop => stddev_pop(source_column),
            CalculationType::VarPop => var_pop(source_column),
            CalculationType::VarSamp => var_sample(source_column),
        }
        .alias(new_column_name.clone());

        let subq_df = df
            .clone()
            .aggregate(vec![], vec![calc_expr])?
            .select_columns(&[new_column_name.as_str()])?;

        let subq_expr = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(subq_df.logical_plan().clone()),
            outer_ref_columns: vec![],
        });

        df.with_column(&new_column_name, subq_expr)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        "calculation"
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!(
            "{}_{}",
            column_name,
            self.calculation_type.to_string().to_ascii_lowercase()
        )
    }

    fn description(&self) -> &str {
        "Calculates a value for a column across the entire table"
    }
}

/// Macro to create a zero argument calculation rule
#[macro_export]
macro_rules! calc_empty_variant {
    ($name:ident, $ctype:ident, $(#[$($attrss:tt)*])*) => {
        $(#[$($attrss)*])*
        pub fn $name() -> Arc<CalculationRule> {
            std::sync::Arc::new(CalculationRule { calculation_type: CalculationType::$ctype})
        }
    }
}

#[macro_export]
macro_rules! calc_xy_variant {
    ($name:ident, $ctype:ident, $(#[$($attrss:tt)*])*) => {
        $(#[$($attrss)*])*
        pub fn $name(x: Option<Expr>, y: Option<Expr>) -> Arc<CalculationRule> {
            std::sync::Arc::new(CalculationRule { calculation_type: CalculationType::$ctype{ x, y } } )
        }
    }
}

calc_empty_variant!(dfq_count, Count, #[doc = r#"Creates a rule that counts the number of rows in a column.

# Examples

```
use datafusion_quality::rules::table::dfq_count;
use datafusion_quality::RuleSet;

// Create a rule to count the number of rows in the age column
let rule = dfq_count();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_count_distinct, CountDistinct, #[doc = r#"Creates a rule that counts the number of distinct values in a column.

# Examples

```
use datafusion_quality::rules::table::dfq_count_distinct;
use datafusion_quality::RuleSet;

// Create a rule to count distinct values in the age column
let rule = dfq_count_distinct();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_avg, Avg, #[doc = r#"Creates a rule that calculates the average value of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_avg;
use datafusion_quality::RuleSet;

// Create a rule to calculate the average of the age column
let rule = dfq_avg();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_stddev, StdDev, #[doc = r#"Creates a rule that calculates the standard deviation of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_stddev;
use datafusion_quality::RuleSet;

// Create a rule to calculate the standard deviation of the age column
let rule = dfq_stddev();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_max, Max, #[doc = r#"Creates a rule that calculates the maximum value of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_max;
use datafusion_quality::RuleSet;

// Create a rule to find the maximum value in the age column
let rule = dfq_max();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_min, Min, #[doc = r#"Creates a rule that calculates the minimum value of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_min;
use datafusion_quality::RuleSet;

// Create a rule to find the minimum value in the age column
let rule = dfq_min();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_sum, Sum, #[doc = r#"Creates a rule that calculates the sum of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_sum;
use datafusion_quality::RuleSet;

// Create a rule to calculate the sum of the age column
let rule = dfq_sum();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_median, Median, #[doc = r#"Creates a rule that calculates the median of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_median;
use datafusion_quality::RuleSet;

// Create a rule to calculate the median of the age column
let rule = dfq_median();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_last_value, LastValue, #[doc = r#"Creates a rule that calculates the last value of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_last_value;
use datafusion_quality::RuleSet;

// Create a rule to get the last value in the age column
let rule = dfq_last_value();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_stddev_pop, StddevPop, #[doc = r#"Creates a rule that calculates the population standard deviation of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_stddev_pop;
use datafusion_quality::RuleSet;

// Create a rule to calculate the population standard deviation of the age column
let rule = dfq_stddev_pop();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_var_pop, VarPop, #[doc = r#"Creates a rule that calculates the population variance of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_var_pop;
use datafusion_quality::RuleSet;

// Create a rule to calculate the population variance of the age column
let rule = dfq_var_pop();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_empty_variant!(dfq_var_samp, VarSamp, #[doc = r#"Creates a rule that calculates the sample variance of a column.

# Examples

```
use datafusion_quality::rules::table::dfq_var_samp;
use datafusion_quality::RuleSet;

// Create a rule to calculate the sample variance of the age column
let rule = dfq_var_samp();
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_covar_pop, CovarPop, #[doc = r#"Creates a rule that calculates the population covariance of two columns.

# Examples

```
use datafusion_quality::rules::table::dfq_covar_pop;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the population covariance between age and score columns
let rule = dfq_covar_pop(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_covar_samp, CovarSamp, #[doc = r#"Creates a rule that calculates the sample covariance of two columns.

# Examples

```
use datafusion_quality::rules::table::dfq_covar_samp;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the sample covariance between age and score columns
let rule = dfq_covar_samp(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_avgx, RegrAvgX, #[doc = r#"Creates a rule that calculates the average of x values in a column.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_avgx;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the average of x values between age and score columns
let rule = dfq_regr_avgx(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_avgy, RegrAvgY, #[doc = r#"Creates a rule that calculates the average of y values in a column.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_avgy;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the average of y values between age and score columns
let rule = dfq_regr_avgy(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_count, RegrCount, #[doc = r#"Creates a rule that calculates the number of rows in a column.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_count;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to count rows between age and score columns
let rule = dfq_regr_count(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_intercept, RegrIntercept, #[doc = r#"Creates a rule that calculates the intercept of a linear regression.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_intercept;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the intercept between age and score columns
let rule = dfq_regr_intercept(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_r2, RegrR2, #[doc = r#"Creates a rule that calculates the R-squared value of a linear regression.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_r2;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the R-squared value between age and score columns
let rule = dfq_regr_r2(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_slope, RegrSlope, #[doc = r#"Creates a rule that calculates the slope of a linear regression.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_slope;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the slope between age and score columns
let rule = dfq_regr_slope(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_sxx, RegrSxx, #[doc = r#"Creates a rule that calculates the sum of squared deviations from the mean for x values.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_sxx;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the sum of squared deviations for x values between age and score columns
let rule = dfq_regr_sxx(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_sxy, RegrSxy, #[doc = r#"Creates a rule that calculates the sum of the products of deviations from the mean for x and y values.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_sxy;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the sum of products of deviations between age and score columns
let rule = dfq_regr_sxy(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);
calc_xy_variant!(dfq_regr_syy, RegrSyy, #[doc = r#"Creates a rule that calculates the sum of squared deviations from the mean for y values.

# Examples

```
use datafusion_quality::rules::table::dfq_regr_syy;
use datafusion_quality::RuleSet;
use datafusion::prelude::*;

// Create a rule to calculate the sum of squared deviations for y values between age and score columns
let rule = dfq_regr_syy(Some(col("age")), Some(col("score")));
let mut ruleset = RuleSet::new();
ruleset.with_table_rule("age", rule, None);
```"#]);

/// Returns the nth value in the column.
///
/// # Arguments
///
/// * `n` - The nth value to return (1-based index)
/// * `sort_exprs` - Optional sort expressions to determine the order
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::table::dfq_nth_value;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to get the 3rd value in the age column
/// let rule = dfq_nth_value(3, None);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_table_rule("age", rule, None);
/// ```
pub fn dfq_nth_value(n: i64, sort_exprs: Option<Vec<SortExpr>>) -> Arc<CalculationRule> {
    std::sync::Arc::new(CalculationRule {
        calculation_type: CalculationType::NthValue(n, sort_exprs),
    })
}

/// Returns the first value in the column.
///
/// # Arguments
///
/// * `sort_exprs` - Optional sort expressions to determine the order
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::table::dfq_first_value;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to get the first value in the age column
/// let rule = dfq_first_value(None);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_table_rule("age", rule, None);
/// ```
pub fn dfq_first_value(sort_exprs: Option<Vec<SortExpr>>) -> Arc<CalculationRule> {
    std::sync::Arc::new(CalculationRule {
        calculation_type: CalculationType::FirstValue(sort_exprs),
    })
}

#[derive(Debug, Clone, Default)]
pub struct CustomAggregationRuleBuilder {
    aggregation: Expr,
    rule_name: String,
    group_by_exprs: Option<Vec<Expr>>,
    aggregate_exprs: Option<Vec<Expr>>,
    order_by: Option<Vec<SortExpr>>,
    window_exprs: Option<Vec<Expr>>,
    filter: Option<Expr>,
}

impl CustomAggregationRuleBuilder {
    pub fn new(aggregation: Expr, rule_name: String) -> Self {
        Self {
            aggregation,
            rule_name,
            ..Default::default()
        }
    }

    pub fn with_group_by(mut self, group_by_exprs: Vec<Expr>) -> Self {
        self.group_by_exprs = Some(group_by_exprs);
        self
    }

    pub fn with_aggregate_exprs(mut self, aggregate_exprs: Vec<Expr>) -> Self {
        self.aggregate_exprs = Some(aggregate_exprs);
        self
    }

    pub fn with_order_by(mut self, order_by: Vec<SortExpr>) -> Self {
        self.order_by = Some(order_by);
        self
    }

    pub fn with_window_exprs(mut self, window_exprs: Vec<Expr>) -> Self {
        self.window_exprs = Some(window_exprs);
        self
    }

    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn build(self) -> Arc<CustomAggregationRule> {
        std::sync::Arc::new(CustomAggregationRule {
            aggregation: self.aggregation,
            rule_name: self.rule_name,
            group_by_exprs: self.group_by_exprs,
            aggregate_exprs: self.aggregate_exprs,
            order_by: self.order_by,
            window_exprs: self.window_exprs,
            filter: self.filter,
        })
    }
}

/// Rule that applies a custom aggregation across the entire table
#[derive(Debug, Clone, Default)]
pub struct CustomAggregationRule {
    aggregation: Expr,
    group_by_exprs: Option<Vec<Expr>>,
    aggregate_exprs: Option<Vec<Expr>>,
    order_by: Option<Vec<SortExpr>>,
    window_exprs: Option<Vec<Expr>>,
    filter: Option<Expr>,
    rule_name: String,
}

impl CustomAggregationRule {
    pub fn builder(aggregation: Expr, rule_name: String) -> CustomAggregationRuleBuilder {
        CustomAggregationRuleBuilder::new(aggregation, rule_name)
    }
}

impl TableRule for CustomAggregationRule {
    fn apply(&self, df: DataFrame, column_name: &str) -> Result<DataFrame, ValidationError> {
        let mut subquery = df.clone();
        if let Some(filter) = self.filter.clone() {
            subquery = subquery.filter(filter)?;
        }

        match (self.group_by_exprs.clone(), self.aggregate_exprs.clone()) {
            (Some(group_by), Some(aggregate)) => {
                subquery = subquery.aggregate(group_by, aggregate)?;
            }
            (None, Some(aggregate)) => {
                subquery = subquery.aggregate(vec![], aggregate)?;
            }
            _ => {
                return Err(ValidationError::Configuration {
                    message: "Group by requires aggregate expressions".to_string(),
                });
            }
        }

        if let Some(window_exprs) = self.window_exprs.clone() {
            subquery = subquery.window(window_exprs)?;
        }
        if let Some(order_by) = self.order_by.clone() {
            subquery = subquery.sort(order_by)?;
        }
        subquery = subquery.select(vec![self.aggregation.clone()])?;

        let subq_expr = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(subquery.logical_plan().clone()),
            outer_ref_columns: vec![],
        });
        df.with_column(&self.new_column_name(column_name), subq_expr)
            .context(DataFusionSnafu)
    }

    fn name(&self) -> &str {
        &self.rule_name
    }

    fn new_column_name(&self, column_name: &str) -> String {
        format!("{}_{}", column_name, self.rule_name)
    }

    fn description(&self) -> &str {
        "Applies a custom aggregation across the entire table"
    }
}

pub fn dfq_custom_agg(aggregation: Expr, rule_name: String) -> Arc<CustomAggregationRule> {
    CustomAggregationRule::builder(aggregation, rule_name).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::assert_batches_eq;
    use std::sync::Arc;

    async fn create_test_df() -> DataFrame {
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
        let age_data = Int32Array::from(vec![Some(25), Some(30), Some(15), Some(40), Some(25)]);
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

        let ctx = SessionContext::new();
        ctx.read_batch(batch).unwrap()
    }

    #[tokio::test]
    async fn test_null_count_rule() {
        let df = create_test_df().await;
        let rule = NullCountRule::default();
        let result = rule.apply(df, "name").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------------+",
            "| id | name    | age | score | name_null_count |",
            "+----+---------+-----+-------+-----------------+",
            "| 1  | Alice   | 25  | 85.5  | 1               |",
            "| 2  | Bob     | 30  | 92.0  | 1               |",
            "| 3  |         | 15  | 78.5  | 1               |",
            "| 4  | Charlie | 40  | 95.0  | 1               |",
            "| 5  | Dave    | 25  | 88.5  | 1               |",
            "+----+---------+-----+-------+-----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        // Test negated null count rule
        let df = create_test_df().await;
        let rule = NullCountRule {
            negated: Some(true),
        };
        let result = rule.apply(df, "name").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------------+",
            "| id | name    | age | score | name_not_null_count |",
            "+----+---------+-----+-------+---------------------+",
            "| 1  | Alice   | 25  | 85.5  | 4                   |",
            "| 2  | Bob     | 30  | 92.0  | 4                   |",
            "| 3  |         | 15  | 78.5  | 4                   |",
            "| 4  | Charlie | 40  | 95.0  | 4                   |",
            "| 5  | Dave    | 25  | 88.5  | 4                   |",
            "+----+---------+-----+-------+---------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_count_rule() {
        let df = create_test_df().await;
        let rule = dfq_count();
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------+",
            "| id | name    | age | score | age_count |",
            "+----+---------+-----+-------+-----------+",
            "| 1  | Alice   | 25  | 85.5  | 5         |",
            "| 2  | Bob     | 30  | 92.0  | 5         |",
            "| 3  |         | 15  | 78.5  | 5         |",
            "| 4  | Charlie | 40  | 95.0  | 5         |",
            "| 5  | Dave    | 25  | 88.5  | 5         |",
            "+----+---------+-----+-------+-----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_count_distinct_rule() {
        let df = create_test_df().await;
        let rule = dfq_count_distinct();
        let result = rule.apply(df, "age").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------+",
            "| id | name    | age | score | age_countdistinct |",
            "+----+---------+-----+-------+-------------------+",
            "| 1  | Alice   | 25  | 85.5  | 4                 |",
            "| 2  | Bob     | 30  | 92.0  | 4                 |",
            "| 3  |         | 15  | 78.5  | 4                 |",
            "| 4  | Charlie | 40  | 95.0  | 4                 |",
            "| 5  | Dave    | 25  | 88.5  | 4                 |",
            "+----+---------+-----+-------+-------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_avg_rule() {
        let df = create_test_df().await;
        let rule = dfq_avg();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------+",
            "| id | name    | age | score | score_avg |",
            "+----+---------+-----+-------+-----------+",
            "| 1  | Alice   | 25  | 85.5  | 87.9      |",
            "| 2  | Bob     | 30  | 92.0  | 87.9      |",
            "| 3  |         | 15  | 78.5  | 87.9      |",
            "| 4  | Charlie | 40  | 95.0  | 87.9      |",
            "| 5  | Dave    | 25  | 88.5  | 87.9      |",
            "+----+---------+-----+-------+-----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_stddev_rule() {
        let df = create_test_df().await;
        let rule = dfq_stddev();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------+",
            "| id | name    | age | score | score_stddev      |",
            "+----+---------+-----+-------+-------------------+",
            "| 1  | Alice   | 25  | 85.5  | 6.358065743604732 |",
            "| 2  | Bob     | 30  | 92.0  | 6.358065743604732 |",
            "| 3  |         | 15  | 78.5  | 6.358065743604732 |",
            "| 4  | Charlie | 40  | 95.0  | 6.358065743604732 |",
            "| 5  | Dave    | 25  | 88.5  | 6.358065743604732 |",
            "+----+---------+-----+-------+-------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_max_rule() {
        let df = create_test_df().await;
        let rule = dfq_max();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------+",
            "| id | name    | age | score | score_max |",
            "+----+---------+-----+-------+-----------+",
            "| 1  | Alice   | 25  | 85.5  | 95.0      |",
            "| 2  | Bob     | 30  | 92.0  | 95.0      |",
            "| 3  |         | 15  | 78.5  | 95.0      |",
            "| 4  | Charlie | 40  | 95.0  | 95.0      |",
            "| 5  | Dave    | 25  | 88.5  | 95.0      |",
            "+----+---------+-----+-------+-----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_min_rule() {
        let df = create_test_df().await;
        let rule = dfq_min();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------+",
            "| id | name    | age | score | score_min |",
            "+----+---------+-----+-------+-----------+",
            "| 1  | Alice   | 25  | 85.5  | 78.5      |",
            "| 2  | Bob     | 30  | 92.0  | 78.5      |",
            "| 3  |         | 15  | 78.5  | 78.5      |",
            "| 4  | Charlie | 40  | 95.0  | 78.5      |",
            "| 5  | Dave    | 25  | 88.5  | 78.5      |",
            "+----+---------+-----+-------+-----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_sum_rule() {
        let df = create_test_df().await;
        let rule = dfq_sum();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------+",
            "| id | name    | age | score | score_sum |",
            "+----+---------+-----+-------+-----------+",
            "| 1  | Alice   | 25  | 85.5  | 439.5     |",
            "| 2  | Bob     | 30  | 92.0  | 439.5     |",
            "| 3  |         | 15  | 78.5  | 439.5     |",
            "| 4  | Charlie | 40  | 95.0  | 439.5     |",
            "| 5  | Dave    | 25  | 88.5  | 439.5     |",
            "+----+---------+-----+-------+-----------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_median_rule() {
        let df = create_test_df().await;
        let rule = dfq_median();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------+",
            "| id | name    | age | score | score_median |",
            "+----+---------+-----+-------+--------------+",
            "| 1  | Alice   | 25  | 85.5  | 88.5         |",
            "| 2  | Bob     | 30  | 92.0  | 88.5         |",
            "| 3  |         | 15  | 78.5  | 88.5         |",
            "| 4  | Charlie | 40  | 95.0  | 88.5         |",
            "| 5  | Dave    | 25  | 88.5  | 88.5         |",
            "+----+---------+-----+-------+--------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_last_value_rule() {
        let df = create_test_df().await;
        let rule = dfq_last_value();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------------+",
            "| id | name    | age | score | score_lastvalue |",
            "+----+---------+-----+-------+-----------------+",
            "| 1  | Alice   | 25  | 85.5  | 88.5            |",
            "| 2  | Bob     | 30  | 92.0  | 88.5            |",
            "| 3  |         | 15  | 78.5  | 88.5            |",
            "| 4  | Charlie | 40  | 95.0  | 88.5            |",
            "| 5  | Dave    | 25  | 88.5  | 88.5            |",
            "+----+---------+-----+-------+-----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_stddev_pop_rule() {
        let df = create_test_df().await;
        let rule = dfq_stddev_pop();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------+",
            "| id | name    | age | score | score_stddevpop   |",
            "+----+---------+-----+-------+-------------------+",
            "| 1  | Alice   | 25  | 85.5  | 5.686826883245172 |",
            "| 2  | Bob     | 30  | 92.0  | 5.686826883245172 |",
            "| 3  |         | 15  | 78.5  | 5.686826883245172 |",
            "| 4  | Charlie | 40  | 95.0  | 5.686826883245172 |",
            "| 5  | Dave    | 25  | 88.5  | 5.686826883245172 |",
            "+----+---------+-----+-------+-------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_var_pop_rule() {
        let df = create_test_df().await;
        let rule = dfq_var_pop();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_varpop       |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 32.339999999999996 |",
            "| 2  | Bob     | 30  | 92.0  | 32.339999999999996 |",
            "| 3  |         | 15  | 78.5  | 32.339999999999996 |",
            "| 4  | Charlie | 40  | 95.0  | 32.339999999999996 |",
            "| 5  | Dave    | 25  | 88.5  | 32.339999999999996 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_var_samp_rule() {
        let df = create_test_df().await;
        let rule = dfq_var_samp();
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | score_varsamp |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | 40.425        |",
            "| 2  | Bob     | 30  | 92.0  | 40.425        |",
            "| 3  |         | 15  | 78.5  | 40.425        |",
            "| 4  | Charlie | 40  | 95.0  | 40.425        |",
            "| 5  | Dave    | 25  | 88.5  | 40.425        |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_covar_pop_rule() {
        let df = create_test_df().await;
        let rule = dfq_covar_pop(Some(col("age")), None);
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------+",
            "| id | name    | age | score | score_covarpop    |",
            "+----+---------+-----+-------+-------------------+",
            "| 1  | Alice   | 25  | 85.5  | 44.20000000000001 |",
            "| 2  | Bob     | 30  | 92.0  | 44.20000000000001 |",
            "| 3  |         | 15  | 78.5  | 44.20000000000001 |",
            "| 4  | Charlie | 40  | 95.0  | 44.20000000000001 |",
            "| 5  | Dave    | 25  | 88.5  | 44.20000000000001 |",
            "+----+---------+-----+-------+-------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_covar_samp_rule() {
        let df = create_test_df().await;
        let rule = dfq_covar_samp(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_covarsamp    |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 55.250000000000014 |",
            "| 2  | Bob     | 30  | 92.0  | 55.250000000000014 |",
            "| 3  |         | 15  | 78.5  | 55.250000000000014 |",
            "| 4  | Charlie | 40  | 95.0  | 55.250000000000014 |",
            "| 5  | Dave    | 25  | 88.5  | 55.250000000000014 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_covar_samp(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_covarsamp    |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 55.249999999999986 |",
            "| 2  | Bob     | 30  | 92.0  | 55.249999999999986 |",
            "| 3  |         | 15  | 78.5  | 55.249999999999986 |",
            "| 4  | Charlie | 40  | 95.0  | 55.249999999999986 |",
            "| 5  | Dave    | 25  | 88.5  | 55.249999999999986 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_avgx_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_avgx(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_regravgx |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | 27.0           |",
            "| 2  | Bob     | 30  | 92.0  | 27.0           |",
            "| 3  |         | 15  | 78.5  | 27.0           |",
            "| 4  | Charlie | 40  | 95.0  | 27.0           |",
            "| 5  | Dave    | 25  | 88.5  | 27.0           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_avgx(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_regravgx |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | 87.9           |",
            "| 2  | Bob     | 30  | 92.0  | 87.9           |",
            "| 3  |         | 15  | 78.5  | 87.9           |",
            "| 4  | Charlie | 40  | 95.0  | 87.9           |",
            "| 5  | Dave    | 25  | 88.5  | 87.9           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_avgy_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_avgy(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_regravgy |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | 87.9           |",
            "| 2  | Bob     | 30  | 92.0  | 87.9           |",
            "| 3  |         | 15  | 78.5  | 87.9           |",
            "| 4  | Charlie | 40  | 95.0  | 87.9           |",
            "| 5  | Dave    | 25  | 88.5  | 87.9           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_avgy(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_regravgy |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | 27.0           |",
            "| 2  | Bob     | 30  | 92.0  | 27.0           |",
            "| 3  |         | 15  | 78.5  | 27.0           |",
            "| 4  | Charlie | 40  | 95.0  | 27.0           |",
            "| 5  | Dave    | 25  | 88.5  | 27.0           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_count_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_count(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------------+",
            "| id | name    | age | score | score_regrcount |",
            "+----+---------+-----+-------+-----------------+",
            "| 1  | Alice   | 25  | 85.5  | 5               |",
            "| 2  | Bob     | 30  | 92.0  | 5               |",
            "| 3  |         | 15  | 78.5  | 5               |",
            "| 4  | Charlie | 40  | 95.0  | 5               |",
            "| 5  | Dave    | 25  | 88.5  | 5               |",
            "+----+---------+-----+-------+-----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_count(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-----------------+",
            "| id | name    | age | score | score_regrcount |",
            "+----+---------+-----+-------+-----------------+",
            "| 1  | Alice   | 25  | 85.5  | 5               |",
            "| 2  | Bob     | 30  | 92.0  | 5               |",
            "| 3  |         | 15  | 78.5  | 5               |",
            "| 4  | Charlie | 40  | 95.0  | 5               |",
            "| 5  | Dave    | 25  | 88.5  | 5               |",
            "+----+---------+-----+-------+-----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_intercept_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_intercept(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------------+",
            "| id | name    | age | score | score_regrintercept |",
            "+----+---------+-----+-------+---------------------+",
            "| 1  | Alice   | 25  | 85.5  | 69.81818181818183   |",
            "| 2  | Bob     | 30  | 92.0  | 69.81818181818183   |",
            "| 3  |         | 15  | 78.5  | 69.81818181818183   |",
            "| 4  | Charlie | 40  | 95.0  | 69.81818181818183   |",
            "| 5  | Dave    | 25  | 88.5  | 69.81818181818183   |",
            "+----+---------+-----+-------+---------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_intercept(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------------+",
            "| id | name    | age | score | score_regrintercept |",
            "+----+---------+-----+-------+---------------------+",
            "| 1  | Alice   | 25  | 85.5  | -93.1354359925789   |",
            "| 2  | Bob     | 30  | 92.0  | -93.1354359925789   |",
            "| 3  |         | 15  | 78.5  | -93.1354359925789   |",
            "| 4  | Charlie | 40  | 95.0  | -93.1354359925789   |",
            "| 5  | Dave    | 25  | 88.5  | -93.1354359925789   |",
            "+----+---------+-----+-------+---------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_r2_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_r2(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_regrr2       |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 0.9152939412679669 |",
            "| 2  | Bob     | 30  | 92.0  | 0.9152939412679669 |",
            "| 3  |         | 15  | 78.5  | 0.9152939412679669 |",
            "| 4  | Charlie | 40  | 95.0  | 0.9152939412679669 |",
            "| 5  | Dave    | 25  | 88.5  | 0.9152939412679669 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_r2(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_regrr2       |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 0.9152939412679678 |",
            "| 2  | Bob     | 30  | 92.0  | 0.9152939412679678 |",
            "| 3  |         | 15  | 78.5  | 0.9152939412679678 |",
            "| 4  | Charlie | 40  | 95.0  | 0.9152939412679678 |",
            "| 5  | Dave    | 25  | 88.5  | 0.9152939412679678 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_slope_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_slope(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_regrslope    |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 0.6696969696969696 |",
            "| 2  | Bob     | 30  | 92.0  | 0.6696969696969696 |",
            "| 3  |         | 15  | 78.5  | 0.6696969696969696 |",
            "| 4  | Charlie | 40  | 95.0  | 0.6696969696969696 |",
            "| 5  | Dave    | 25  | 88.5  | 0.6696969696969696 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_slope(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+-------------------+",
            "| id | name    | age | score | score_regrslope   |",
            "+----+---------+-----+-------+-------------------+",
            "| 1  | Alice   | 25  | 85.5  | 1.366728509585653 |",
            "| 2  | Bob     | 30  | 92.0  | 1.366728509585653 |",
            "| 3  |         | 15  | 78.5  | 1.366728509585653 |",
            "| 4  | Charlie | 40  | 95.0  | 1.366728509585653 |",
            "| 5  | Dave    | 25  | 88.5  | 1.366728509585653 |",
            "+----+---------+-----+-------+-------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_sxx_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_sxx(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | score_regrsxx |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | 330.0         |",
            "| 2  | Bob     | 30  | 92.0  | 330.0         |",
            "| 3  |         | 15  | 78.5  | 330.0         |",
            "| 4  | Charlie | 40  | 95.0  | 330.0         |",
            "| 5  | Dave    | 25  | 88.5  | 330.0         |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_sxx(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | score_regrsxx |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | 161.7         |",
            "| 2  | Bob     | 30  | 92.0  | 161.7         |",
            "| 3  |         | 15  | 78.5  | 161.7         |",
            "| 4  | Charlie | 40  | 95.0  | 161.7         |",
            "| 5  | Dave    | 25  | 88.5  | 161.7         |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_sxy_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_sxy(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_regrsxy      |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 220.99999999999994 |",
            "| 2  | Bob     | 30  | 92.0  | 220.99999999999994 |",
            "| 3  |         | 15  | 78.5  | 220.99999999999994 |",
            "| 4  | Charlie | 40  | 95.0  | 220.99999999999994 |",
            "| 5  | Dave    | 25  | 88.5  | 220.99999999999994 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_sxy(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+--------------------+",
            "| id | name    | age | score | score_regrsxy      |",
            "+----+---------+-----+-------+--------------------+",
            "| 1  | Alice   | 25  | 85.5  | 221.00000000000006 |",
            "| 2  | Bob     | 30  | 92.0  | 221.00000000000006 |",
            "| 3  |         | 15  | 78.5  | 221.00000000000006 |",
            "| 4  | Charlie | 40  | 95.0  | 221.00000000000006 |",
            "| 5  | Dave    | 25  | 88.5  | 221.00000000000006 |",
            "+----+---------+-----+-------+--------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_regr_syy_rule() {
        let df = create_test_df().await;
        let rule = dfq_regr_syy(Some(col("age")), None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | score_regrsyy |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | 161.7         |",
            "| 2  | Bob     | 30  | 92.0  | 161.7         |",
            "| 3  |         | 15  | 78.5  | 161.7         |",
            "| 4  | Charlie | 40  | 95.0  | 161.7         |",
            "| 5  | Dave    | 25  | 88.5  | 161.7         |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_regr_syy(None, Some(col("age")));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+---------------+",
            "| id | name    | age | score | score_regrsyy |",
            "+----+---------+-----+-------+---------------+",
            "| 1  | Alice   | 25  | 85.5  | 330.0         |",
            "| 2  | Bob     | 30  | 92.0  | 330.0         |",
            "| 3  |         | 15  | 78.5  | 330.0         |",
            "| 4  | Charlie | 40  | 95.0  | 330.0         |",
            "| 5  | Dave    | 25  | 88.5  | 330.0         |",
            "+----+---------+-----+-------+---------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_nth_value_rule() {
        let df = create_test_df().await;
        let rule = dfq_nth_value(2, None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_nthvalue |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | 92.0           |",
            "| 2  | Bob     | 30  | 92.0  | 92.0           |",
            "| 3  |         | 15  | 78.5  | 92.0           |",
            "| 4  | Charlie | 40  | 95.0  | 92.0           |",
            "| 5  | Dave    | 25  | 88.5  | 92.0           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_nth_value(2, Some(vec![col("score").sort(true, false)]));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+----------------+",
            "| id | name    | age | score | score_nthvalue |",
            "+----+---------+-----+-------+----------------+",
            "| 1  | Alice   | 25  | 85.5  | 85.5           |",
            "| 2  | Bob     | 30  | 92.0  | 85.5           |",
            "| 3  |         | 15  | 78.5  | 85.5           |",
            "| 4  | Charlie | 40  | 95.0  | 85.5           |",
            "| 5  | Dave    | 25  | 88.5  | 85.5           |",
            "+----+---------+-----+-------+----------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_first_value_rule() {
        let df = create_test_df().await;
        let rule = dfq_first_value(None);
        let result = rule.apply(df.clone(), "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+------------------+",
            "| id | name    | age | score | score_firstvalue |",
            "+----+---------+-----+-------+------------------+",
            "| 1  | Alice   | 25  | 85.5  | 85.5             |",
            "| 2  | Bob     | 30  | 92.0  | 85.5             |",
            "| 3  |         | 15  | 78.5  | 85.5             |",
            "| 4  | Charlie | 40  | 95.0  | 85.5             |",
            "| 5  | Dave    | 25  | 88.5  | 85.5             |",
            "+----+---------+-----+-------+------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());

        let rule = dfq_first_value(Some(vec![col("score").sort(true, false)]));
        let result = rule.apply(df, "score").unwrap();

        let expected = vec![
            "+----+---------+-----+-------+------------------+",
            "| id | name    | age | score | score_firstvalue |",
            "+----+---------+-----+-------+------------------+",
            "| 1  | Alice   | 25  | 85.5  | 78.5             |",
            "| 2  | Bob     | 30  | 92.0  | 78.5             |",
            "| 3  |         | 15  | 78.5  | 78.5             |",
            "| 4  | Charlie | 40  | 95.0  | 78.5             |",
            "| 5  | Dave    | 25  | 88.5  | 78.5             |",
            "+----+---------+-----+-------+------------------+",
        ];

        assert_batches_eq!(&expected, &result.collect().await.unwrap());
    }

    #[tokio::test]
    async fn test_custom_aggregation_rule_builder() {
        let df = create_test_df().await;

        // Test with_group_by - groups by name and gets max score per group
        let group_by_rule =
            CustomAggregationRule::builder(col("max_score"), "max_score_by_name".to_string())
                .with_group_by(vec![col("name")])
                .with_aggregate_exprs(vec![max(col("score")).alias("max_score")])
                .build();

        let group_by_result = group_by_rule
            .apply(df.clone(), "score")
            .unwrap()
            .sort(vec![col("id").sort(true, false)])
            .unwrap();

        let group_by_expected = vec![
            "+----+---------+-----+-------+-------------------------+",
            "| id | name    | age | score | score_max_score_by_name |",
            "+----+---------+-----+-------+-------------------------+",
            "| 1  | Alice   | 25  | 85.5  | 92.0                    |",
            "| 1  | Alice   | 25  | 85.5  | 85.5                    |",
            "| 1  | Alice   | 25  | 85.5  | 78.5                    |",
            "| 1  | Alice   | 25  | 85.5  | 88.5                    |",
            "| 1  | Alice   | 25  | 85.5  | 95.0                    |",
            "| 2  | Bob     | 30  | 92.0  | 92.0                    |",
            "| 2  | Bob     | 30  | 92.0  | 85.5                    |",
            "| 2  | Bob     | 30  | 92.0  | 78.5                    |",
            "| 2  | Bob     | 30  | 92.0  | 88.5                    |",
            "| 2  | Bob     | 30  | 92.0  | 95.0                    |",
            "| 3  |         | 15  | 78.5  | 92.0                    |",
            "| 3  |         | 15  | 78.5  | 85.5                    |",
            "| 3  |         | 15  | 78.5  | 78.5                    |",
            "| 3  |         | 15  | 78.5  | 88.5                    |",
            "| 3  |         | 15  | 78.5  | 95.0                    |",
            "| 4  | Charlie | 40  | 95.0  | 92.0                    |",
            "| 4  | Charlie | 40  | 95.0  | 85.5                    |",
            "| 4  | Charlie | 40  | 95.0  | 78.5                    |",
            "| 4  | Charlie | 40  | 95.0  | 88.5                    |",
            "| 4  | Charlie | 40  | 95.0  | 95.0                    |",
            "| 5  | Dave    | 25  | 88.5  | 92.0                    |",
            "| 5  | Dave    | 25  | 88.5  | 85.5                    |",
            "| 5  | Dave    | 25  | 88.5  | 78.5                    |",
            "| 5  | Dave    | 25  | 88.5  | 88.5                    |",
            "| 5  | Dave    | 25  | 88.5  | 95.0                    |",
            "+----+---------+-----+-------+-------------------------+",
        ];

        assert_batches_eq!(
            &group_by_expected,
            &group_by_result.collect().await.unwrap()
        );

        // Test with_filter - filters out null names and gets max score
        let filter_rule = CustomAggregationRule::builder(
            col("max_score_non_null"),
            "max_score_non_null".to_string(),
        )
        .with_filter(col("name").is_not_null())
        .with_aggregate_exprs(vec![max(col("score")).alias("max_score_non_null")])
        .build();

        let filter_result = filter_rule.apply(df.clone(), "score").unwrap();

        let filter_expected = vec![
            "+----+---------+-----+-------+--------------------------+",
            "| id | name    | age | score | score_max_score_non_null |",
            "+----+---------+-----+-------+--------------------------+",
            "| 1  | Alice   | 25  | 85.5  | 95.0                     |",
            "| 2  | Bob     | 30  | 92.0  | 95.0                     |",
            "| 3  |         | 15  | 78.5  | 95.0                     |",
            "| 4  | Charlie | 40  | 95.0  | 95.0                     |",
            "| 5  | Dave    | 25  | 88.5  | 95.0                     |",
            "+----+---------+-----+-------+--------------------------+",
        ];

        assert_batches_eq!(&filter_expected, &filter_result.collect().await.unwrap());
    }
}
