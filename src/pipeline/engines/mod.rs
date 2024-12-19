use crate::error::Error;
use async_trait::async_trait;
use deltalake::datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
use duckdb::{arrow::array::RecordBatch, params, Connection};
use std::sync::Arc;
use tokio::sync::Mutex; // Mutex from tokio for async safety
#[async_trait]
pub trait Engine {
    async fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Error>;
    async fn delta_table_mapping(&self, delta_path: &str, duck_table: &str) -> Result<(), Error>;
}

pub struct DuckDB {
    connection: Arc<Mutex<Option<Connection>>>,
}

impl DuckDB {
    pub async fn new() -> Result<Self, Error> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL parquet; LOAD parquet; INSTALL delta; LOAD delta;")?;
        conn.execute_batch("CREATE TABLE delta_mapping (delta_path TEXT, duck_table TEXT)")?;

        Ok(Self {
            connection: Arc::new(Mutex::new(Some(conn))),
        })
    }

    fn parse_table(&self, sql: &str) -> Vec<String> {
        let dialect = GenericDialect {};
        let parser = Parser::parse_sql(&dialect, sql).unwrap();
        let statement = parser.first().unwrap().clone();
        let mut table_names: Vec<String> = vec![];

        match statement {
            ast::Statement::Query(query) => {
                if let Some(select) = query.as_ref().body.as_select() {
                    for data in select.from.iter() {
                        let relation_string = data.relation.to_string();
                        let table_vec: Vec<&str> = relation_string.split(' ').collect();
                        table_names.push(table_vec.first().unwrap().to_string());

                        for join in &data.joins {
                            let relation_string = join.relation.to_string();
                            let table_vec: Vec<&str> = relation_string.split(' ').collect();
                            table_names.push(table_vec.first().unwrap().to_string());
                        }
                    }
                } else {
                    eprintln!("Expected a SELECT statement in the query.");
                }
                table_names
            }
            _ => unimplemented!(),
        }
    }
}

#[async_trait]
impl Engine for DuckDB {
    async fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Error> {
        let connection = self.connection.lock().await;
        match &*connection {
            Some(conn) => {
                let mut stmt = conn.prepare(sql)?;
                let arrow_result = stmt.query_arrow([])?;
                let batches = arrow_result.collect::<Vec<RecordBatch>>();
                Ok(batches)
            }
            None => Err(Error::DuckDB("Connection to DuckDB is None".to_string())),
        }
    }

    async fn delta_table_mapping(&self, delta_path: &str, duck_table: &str) -> Result<(), Error> {
        let connection = self.connection.lock().await;
        match &*connection {
            Some(conn) => {
                conn.execute(
                    "INSERT INTO delta_mapping (delta_path, duck_table) VALUES (?, ?)",
                    params![delta_path, duck_table],
                )?;
            }
            None => return Err(Error::DuckDB("Connection to DuckDB is None".to_string())),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_duckdb_connection() -> Result<(), Error> {
        let duck_engine = DuckDB::new().await?;
        let result = duck_engine.sql("select 1").await;

        assert!(result.is_ok(), "Query should execute successfully");

        let batches = result.unwrap();

        assert!(
            !batches.is_empty(),
            "The result should contain record batches"
        );

        Ok(())
    }

    #[tokio::test]
    async fn delta_table_mapping() -> Result<(), Error> {
        let duck_engine = DuckDB::new().await?;
        duck_engine
            .delta_table_mapping("delta_path", "duck_table")
            .await?;
        let result = duck_engine.sql("SELECT * FROM delta_mapping").await;

        assert!(result.is_ok(), "Query should execute successfully");

        Ok(())
    }

    #[tokio::test]
    async fn parse_table_names() -> Result<(), Error> {
        let sql = r#"
            select * from delta_scan('s3://some/delta/table/with/auth') y left join delta_scan('s3://some/delta/table/with/auth') b  on a.id = b.id;
            "#;

        let sql2 = r#"
            select * from delta_scan('s3://some/delta/table/with/auth') left join delta_scan('s3://some/delta/table/with/auth')  on a.id = b.id;
            "#;

        let duck_engine = DuckDB::new().await?;

        let tables = duck_engine.parse_table(sql);
        let tables2 = duck_engine.parse_table(sql2);

        assert_eq!(
            tables,
            vec![
                "delta_scan('s3://some/delta/table/with/auth')",
                "delta_scan('s3://some/delta/table/with/auth')"
            ]
        );
        assert_eq!(
            tables2,
            vec![
                "delta_scan('s3://some/delta/table/with/auth')",
                "delta_scan('s3://some/delta/table/with/auth')"
            ]
        );

        Ok(())
    }
}
