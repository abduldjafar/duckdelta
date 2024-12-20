use crate::error::Error;
use async_trait::async_trait;
use deltalake::datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
use duckdb::{arrow::array::RecordBatch, params, Connection};
use regex::Regex;
#[async_trait]
pub trait Engine {
    fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Error>;
    fn delta_table_mapping(&self, delta_path: &str, duck_table: &str) -> Result<(), Error>;
}

pub struct DuckDB {
    connection: Connection,
}

#[derive(Debug)]
struct DeltaMapping {
    delta_path: String,
    duck_table: String,
}

impl DuckDB {
    pub fn new() -> Result<Self, Error> {
        let conn = match Connection::open_in_memory() {
            Ok(conn) => conn,
            Err(e) => return Err(Error::DuckDB(format!("Failed to open connection: {}", e))),
        };

        if let Err(e) =
            conn.execute_batch("INSTALL parquet; LOAD parquet; INSTALL delta; LOAD delta;")
        {
            return Err(Error::DuckDB(format!(
                "Failed to execute batch for libraries: {}",
                e
            )));
        }

        if let Err(e) =
            conn.execute_batch("CREATE TABLE delta_mapping (delta_path TEXT, duck_table TEXT)")
        {
            return Err(Error::DuckDB(format!("Failed to create table: {}", e)));
        }

        // Return the struct with the connection
        Ok(Self { connection: conn })
    }

    fn delta_table_mapping(&self, delta_path: &str, duck_table: &str) -> Result<(), Error> {
        self.connection.execute(
            "INSERT INTO delta_mapping (delta_path, duck_table) VALUES (?, ?)",
            params![delta_path, duck_table],
        )?;
        Ok(())
    }

    fn parse_sql(&self, sql: &str) -> Result<String, Error> {
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
                        let table_name = table_vec.first().unwrap().to_string().replace("'", "''");
                        table_names.push(format!("\'{}\'", table_name));

                        for join in &data.joins {
                            let relation_string = join.relation.to_string();
                            let table_vec: Vec<&str> = relation_string.split(' ').collect();
                            let table_name =
                                table_vec.first().unwrap().to_string().replace("'", "''");
                            table_names.push(format!("\'{}\'", table_name));
                        }
                    }
                } else {
                    eprintln!("Expected a SELECT statement in the query.");
                }
            }
            _ => unimplemented!(),
        }

        let sql_to_check_mapping = format!(
            "select * from delta_mapping where duck_table in ({})",
            table_names.join(",")
        );

        let mut stmt = self.connection.prepare(&sql_to_check_mapping)?;
        let mapping = stmt.query_map([], |row| {
            Ok(DeltaMapping {
                delta_path: row.get(0)?,
                duck_table: row.get(1)?,
            })
        })?;

        let mut modified_sql = sql.to_string();

        for data in mapping {
            let parsed_column = data.unwrap();

            let regex = Regex::new(&format!(
                r"\b{}\b",
                regex::escape(&parsed_column.duck_table)
            ))
            .unwrap();
            modified_sql = regex
                .replace_all(
                    &modified_sql,
                    &format!("delta_scan('{}')", parsed_column.delta_path),
                )
                .to_string();
        }

        Ok(modified_sql)
    }
}

#[async_trait]
impl Engine for DuckDB {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, Error> {
        let parsed_sql = match self.parse_sql(query) {
            Ok(sql) => sql,
            Err(e) => return Err(Error::DuckDB(format!("Failed to parse SQL: {}", e))),
        };

        let mut stmt = self.connection.prepare(&parsed_sql)?;
        let arrow_result = stmt.query_arrow([])?;
        let batches = arrow_result.collect::<Vec<RecordBatch>>();
        Ok(batches)
    }

    fn delta_table_mapping(&self, delta_path: &str, duck_table: &str) -> Result<(), Error> {
        self.delta_table_mapping(delta_path, duck_table)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_delta_table_mapping() -> Result<(), Error> {
        let duck_engine = DuckDB::new()?;
        duck_engine.delta_table_mapping("delta_path", "duck_table")?;
        let result = duck_engine.sql("SELECT * FROM delta_mapping");

        assert!(result.is_ok(), "Query should execute successfully");

        Ok(())
    }

    #[tokio::test]
    async fn parse_table_names() -> Result<(), Error> {
        let sql = r#"select * from delta_auth y left join delta_auth b  on a.id = b.id"#;
        let sql2 = r#"select * from delta_auths y left join delta_auth b  on a.id = b.id"#;

        let duck_engine = DuckDB::new()?;

        duck_engine.delta_table_mapping("s3://some/delta/table/with/auth", "delta_auth")?;
        duck_engine.delta_table_mapping("s3://some/delta/table/with/auth", "delta_auth")?;

        let parsed_sql = duck_engine.parse_sql(sql)?;
        let parsed_sql2 = duck_engine.parse_sql(sql2)?;

        assert_eq!(parsed_sql,"select * from delta_scan('s3://some/delta/table/with/auth') y left join delta_scan('s3://some/delta/table/with/auth') b  on a.id = b.id");
        assert_eq!(parsed_sql2,"select * from delta_auths y left join delta_scan('s3://some/delta/table/with/auth') b  on a.id = b.id");

        Ok(())
    }
}
