use deltalake::datafusion::error::DataFusionError;

use duckdb::Error as DuckDBError;

#[derive(Clone, Debug)]
pub enum Error {
    DataFusion(String),
    DuckDB(String),
    Delta(String),
    Csv(String),
    Io(String),
    UnsupportedFormat(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

impl From<DataFusionError> for Error {
    fn from(error: DataFusionError) -> Self {
        Error::DataFusion(error.to_string())
    }
}

impl From<DuckDBError> for Error {
    fn from(error: DuckDBError) -> Self {
        Error::DuckDB(error.to_string())
    }
}

impl From<deltalake::DeltaTableError> for Error {
    fn from(error: deltalake::DeltaTableError) -> Self {
        Error::Delta(error.to_string())
    }
}

impl From<csv::Error> for Error {
    fn from(value: csv::Error) -> Self {
        Error::Csv(value.to_string())
    }
}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Io(value.to_string())
    }
}
