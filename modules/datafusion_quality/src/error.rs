use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ValidationError {
    #[snafu(display("DataFrame error: {}", source))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Validation error: {}", message))]
    Validation { message: String },

    #[snafu(display("Configuration error: {}", message))]
    Configuration { message: String },

    #[snafu(display("Column not found: {}", column_name))]
    ColumnNotFound { column_name: String },

    #[snafu(display("Type mismatch: {}", message))]
    TypeMismatch { message: String },

    #[snafu(display("Column nullable: {}, expected: {}", column_name, expected))]
    ColumnNullabilityMismatch { column_name: String, expected: bool },

    #[snafu(display("Schema error: {}", message))]
    Schema { message: String },

    #[snafu(display("Column error: {}", message))]
    Column { message: String },
}

impl From<datafusion::error::DataFusionError> for ValidationError {
    fn from(source: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion { source }
    }
}
