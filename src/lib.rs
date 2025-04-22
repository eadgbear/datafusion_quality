pub mod rules;
pub mod error;

use datafusion::prelude::*;
use std::sync::Arc;

/// The main RuleSet struct that holds the context and rules
pub struct RuleSet {
    ctx: SessionContext,
    schema_rules: Vec<Arc<dyn SchemaRule>>,
    row_rules: Vec<Arc<dyn RowRule>>,
    column_rules: Vec<Arc<dyn ColumnRule>>,
}

/// Trait for schema-level rules
pub trait SchemaRule: Send + Sync {
    /// Validate the schema
    fn validate_schema(&self, schema: &Schema) -> Result<bool, ValidationError> {
        unimplemented!("validate_schema must be implemented")
    }
    
    /// Validate the schema with access to the RuleSet
    fn validate_schema_with_ruleset(&self, schema: &Schema, rule_set: &RuleSet) -> Result<bool, ValidationError> {
        self.validate_schema(schema)
    }
    
    /// Get the name of the rule
    fn name(&self) -> &str;
    
    /// Get the description of the rule
    fn description(&self) -> &str;
}

/// Trait for row-level rules
pub trait RowRule: Send + Sync {
    /// Apply the rule to a DataFrame, adding a new column
    fn apply(&self, df: &DataFrame) -> Result<DataFrame, ValidationError> {
        unimplemented!("apply must be implemented")
    }
    
    /// Apply the rule to a DataFrame with access to the RuleSet
    fn apply_with_ruleset(&self, df: &DataFrame, rule_set: &RuleSet) -> Result<DataFrame, ValidationError> {
        self.apply(df)
    }
    
    /// Get the name of the rule
    fn name(&self) -> &str;
    
    /// Get the description of the rule
    fn description(&self) -> &str;
    
    /// Get the name of the column being validated
    fn column_name(&self) -> &str;
}

/// Trait for column-level rules
pub trait ColumnRule: Send + Sync {
    /// Apply the rule to a DataFrame, adding a new column with aggregated results
    fn apply(&self, df: &DataFrame) -> Result<DataFrame, ValidationError> {
        unimplemented!("apply must be implemented")
    }
    
    /// Apply the rule to a DataFrame with access to the RuleSet
    fn apply_with_ruleset(&self, df: &DataFrame, rule_set: &RuleSet) -> Result<DataFrame, ValidationError> {
        self.apply(df)
    }
    
    /// Get the name of the rule
    fn name(&self) -> &str;
    
    /// Get the description of the rule
    fn description(&self) -> &str;
    
    /// Get the name of the column being validated
    fn column_name(&self) -> &str;
}

/// Error type for validation failures
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("DataFrame error: {0}")]
    DataFrameError(#[from] datafusion::error::DataFusionError),
    
    #[error("Schema error: {0}")]
    SchemaError(String),
    
    #[error("Validation error: {0}")]
    ValidationError(String),
    
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

impl RuleSet {
    /// Create a new RuleSet instance
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            schema_rules: Vec::new(),
            row_rules: Vec::new(),
            column_rules: Vec::new(),
        }
    }
    
    /// Add a schema rule
    pub fn add_schema_rule(&mut self, rule: Arc<dyn SchemaRule>) {
        self.schema_rules.push(rule);
    }
    
    /// Add a row rule
    pub fn add_row_rule(&mut self, rule: Arc<dyn RowRule>) {
        self.row_rules.push(rule);
    }
    
    /// Add a column rule
    pub fn add_column_rule(&mut self, rule: Arc<dyn ColumnRule>) {
        self.column_rules.push(rule);
    }
    
    /// Apply all rules to a DataFrame
    pub async fn apply(&self, df: &DataFrame) -> Result<DataFrame, ValidationError> {
        // First validate schema
        for rule in &self.schema_rules {
            if !rule.validate_schema(df.schema())? {
                return Err(ValidationError::SchemaError(format!(
                    "Schema rule '{}' failed",
                    rule.name()
                )));
            }
        }
        
        // Then apply row rules
        let mut result_df = df.clone();
        for rule in &self.row_rules {
            result_df = rule.apply_with_ruleset(&result_df, self)?;
        }
        
        // Finally apply column rules
        for rule in &self.column_rules {
            result_df = rule.apply_with_ruleset(&result_df, self)?;
        }
        
        Ok(result_df)
    }
}
