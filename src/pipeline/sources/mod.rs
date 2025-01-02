
use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;
use duckdb::Connection;


use crate::error::Error;

pub enum SourcesType<'a> {
    Csv(&'a str),
    Json(&'a str),
    Parquet(&'a str),
    Delta(&'a str),
}

#[async_trait]
pub trait Sources {
    async fn read_data(&self) -> Result<Vec<RecordBatch>, Error>;
}

impl<'a> SourcesType<'a> {
    async fn read_by_duckdb(tb_path: &str) -> Result<Vec<RecordBatch>, Error> {
        let sql = format!("select * from read_csv('{}')", tb_path);
        let conn = Connection::open_in_memory()?;

        let mut stmt = conn.prepare(&sql)?;
        let arrow_result = stmt.query_arrow([])?;
        let batches = arrow_result.collect::<Vec<RecordBatch>>();

        Ok(batches)
    }

    async fn read(&self) -> Result<Vec<RecordBatch>, Error> {
        match self {
            SourcesType::Csv(path) => {
                let path_splitted: Vec<&str> = path.split(".").collect();
                let record_batches = match path_splitted.last().unwrap().to_lowercase().as_str() {
                    "csv" => Self::read_by_duckdb(path).await?,
                    _ => return Err(Error::UnsupportedFormat(path.to_string())),
                };
                Ok(record_batches)
            }
            SourcesType::Json(_) => unimplemented!(),
            SourcesType::Parquet(_) => unimplemented!(),
            SourcesType::Delta(_) => unimplemented!(),
        }
    }
}

#[async_trait]
impl<'a> Sources for SourcesType<'a> {
    async fn read_data(&self) -> Result<Vec<RecordBatch>, Error> {
        self.read().await
    }
}
