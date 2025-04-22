use crate::{SchemaRule, ValidationError};
use datafusion::{arrow::datatypes::DataType, common::DFSchema};
use std::sync::Arc;

/// Rule that checks if a column exists in the schema
#[derive(Debug, Clone, Default)]
pub struct ColumnExistsRule {
    column_name: String,
}

impl ColumnExistsRule {
    /// Creates a new ColumnExistsRule
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to check for existence
    pub fn new(column_name: String) -> Self {
        Self { column_name }
    }
}

impl SchemaRule for ColumnExistsRule {
    fn validate_schema(&self, schema: &DFSchema) -> Result<bool, ValidationError> {
        match schema.field_with_name(None, &self.column_name) {
            Ok(_) => Ok(true),
            Err(_) => Err(ValidationError::ColumnNotFound {
                column_name: self.column_name.clone(),
            }),
        }
    }

    fn name(&self) -> &str {
        "column_exists"
    }

    fn description(&self) -> &str {
        "Checks if a column exists in the schema"
    }
}

/// Creates a rule that checks if a column exists in the schema
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::schema::dfq_column_exists;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if the 'age' column exists
/// let rule = dfq_column_exists("age");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_schema_rule(rule);
/// ```
pub fn dfq_column_exists(column_name: impl AsRef<str>) -> Arc<ColumnExistsRule> {
    Arc::new(ColumnExistsRule::new(column_name.as_ref().to_string()))
}

/// Rule that checks if a column has a specific data type
#[derive(Debug, Clone)]
pub struct ColumnTypeRule {
    column_name: String,
    expected_type: DataType,
}

impl ColumnTypeRule {
    /// Creates a new ColumnTypeRule
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to check
    /// * `expected_type` - The expected data type of the column
    pub fn new(column_name: String, expected_type: DataType) -> Self {
        Self {
            column_name,
            expected_type,
        }
    }
}

impl SchemaRule for ColumnTypeRule {
    fn validate_schema(&self, schema: &DFSchema) -> Result<bool, ValidationError> {
        match schema.field_with_name(None, &self.column_name) {
            Ok(field) => {
                if field.data_type() == &self.expected_type {
                    Ok(true)
                } else {
                    Err(ValidationError::TypeMismatch {
                        message: format!(
                            "Column: {}, Expected type {:?} but got {:?}",
                            self.column_name,
                            self.expected_type,
                            field.data_type()
                        ),
                    })
                }
            }
            Err(_) => Err(ValidationError::ColumnNotFound {
                column_name: self.column_name.clone(),
            }),
        }
    }

    fn name(&self) -> &str {
        "column_type"
    }

    fn description(&self) -> &str {
        "Checks if a column has a specific data type"
    }
}

/// Creates a rule that checks if a column has a specific data type
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::schema::dfq_column_type;
/// use datafusion_quality::RuleSet;
/// use datafusion::arrow::datatypes::DataType;
///
/// // Create a rule to check if the 'age' column is of type Int32
/// let rule = dfq_column_type("age", DataType::Int32);
/// let mut ruleset = RuleSet::new();
/// ruleset.with_schema_rule(rule);
/// ```
pub fn dfq_column_type(
    column_name: impl AsRef<str>,
    expected_type: DataType,
) -> Arc<ColumnTypeRule> {
    Arc::new(ColumnTypeRule::new(
        column_name.as_ref().to_string(),
        expected_type,
    ))
}

/// Rule that checks if a column is nullable
#[derive(Debug, Clone)]
pub struct ColumnNullableRule {
    column_name: String,
    expected_nullable: bool,
}

impl ColumnNullableRule {
    /// Creates a new ColumnNullableRule
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to check
    /// * `expected_nullable` - Whether the column should be nullable
    pub fn new(column_name: String, expected_nullable: bool) -> Self {
        Self {
            column_name,
            expected_nullable,
        }
    }
}

impl SchemaRule for ColumnNullableRule {
    fn validate_schema(&self, schema: &DFSchema) -> Result<bool, ValidationError> {
        match schema.field_with_name(None, &self.column_name) {
            Ok(field) => {
                if field.is_nullable() == self.expected_nullable {
                    Ok(true)
                } else {
                    Err(ValidationError::ColumnNullabilityMismatch {
                        column_name: self.column_name.clone(),
                        expected: self.expected_nullable,
                    })
                }
            }
            Err(_) => Err(ValidationError::ColumnNotFound {
                column_name: self.column_name.clone(),
            }),
        }
    }

    fn name(&self) -> &str {
        "column_nullable"
    }

    fn description(&self) -> &str {
        "Checks if a column is nullable"
    }
}

/// Creates a rule that checks if a column is nullable
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::schema::dfq_column_nullable;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if the 'name' column is nullable
/// let rule = dfq_column_nullable("name");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_schema_rule(rule);
/// ```
pub fn dfq_column_nullable(column_name: impl AsRef<str>) -> Arc<ColumnNullableRule> {
    Arc::new(ColumnNullableRule::new(
        column_name.as_ref().to_string(),
        true,
    ))
}

/// Creates a rule that checks if a column is not nullable
///
/// # Examples
///
/// ```
/// use datafusion_quality::rules::schema::dfq_column_not_nullable;
/// use datafusion_quality::RuleSet;
///
/// // Create a rule to check if the 'id' column is not nullable
/// let rule = dfq_column_not_nullable("id");
/// let mut ruleset = RuleSet::new();
/// ruleset.with_schema_rule(rule);
/// ```
pub fn dfq_column_not_nullable(column_name: impl AsRef<str>) -> Arc<ColumnNullableRule> {
    Arc::new(ColumnNullableRule::new(
        column_name.as_ref().to_string(),
        false,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::DFSchema;

    #[test]
    fn test_column_exists_rule() {
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema = DFSchema::try_from(arrow_schema).unwrap();

        let rule = dfq_column_exists("id");
        assert!(rule.validate_schema(&schema).unwrap());

        let rule = dfq_column_exists("nonexistent");
        assert!(rule.validate_schema(&schema).is_err());
    }

    #[test]
    fn test_column_type_rule() {
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema = DFSchema::try_from(arrow_schema).unwrap();

        let rule = dfq_column_type("id", DataType::Int32);
        assert!(rule.validate_schema(&schema).unwrap());

        let rule = dfq_column_type("id", DataType::Utf8);
        assert!(rule.validate_schema(&schema).is_err());

        let rule = dfq_column_type("nonexistent", DataType::Int32);
        assert!(rule.validate_schema(&schema).is_err());
    }

    #[test]
    fn test_column_nullable_rule() {
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema = DFSchema::try_from(arrow_schema).unwrap();

        let rule = dfq_column_not_nullable("id");
        assert!(rule.validate_schema(&schema).unwrap());

        let rule = dfq_column_nullable("name");
        assert!(rule.validate_schema(&schema).unwrap());

        let rule = dfq_column_nullable("id");
        assert!(rule.validate_schema(&schema).is_err());

        let rule = dfq_column_nullable("nonexistent");
        assert!(rule.validate_schema(&schema).is_err());
    }
}
