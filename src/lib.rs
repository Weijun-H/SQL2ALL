use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Ok, Result};

use arrow::{
    array::RecordBatch,
    datatypes::Schema,
    json::{writer::LineDelimited, WriterBuilder},
};
use db::postgresql::PostgreSQL;
use db::{mysql::MySQL, sqlite::SQLite};
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

extern crate parquet;
#[macro_use]
extern crate parquet_derive;

mod db;

pub enum Database {
    MySQL(MySQL),
    PostgreSQL(PostgreSQL),
    SQLite(SQLite),
}

trait Query {
    async fn query(&self, query: &str, output: &PathBuf) -> Result<()>;
}

impl Database {
    pub async fn query(&self, query: &str, output: &PathBuf) -> Result<()> {
        match self {
            Database::MySQL(mysql) => mysql.query(query, output).await,
            Database::PostgreSQL(postgres) => postgres.query(query, output).await,
            Database::SQLite(sqlite) => sqlite.query(query, output).await,
        }
    }
}

/// From url to database
impl FromStr for Database {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let db_type = s
            .split(':')
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid database url: {}", s))?;
        match db_type {
            "mysql" => Ok(Database::MySQL(MySQL::new(s.to_string()))),
            "postgresql" => Ok(Database::PostgreSQL(PostgreSQL::new(s.to_string()))),
            "sqlite" => {
                let path = s.strip_prefix("sqlite:///").unwrap_or(s);
                Ok(Database::SQLite(SQLite::new(path.to_string())))
            }
            _ => Err(anyhow::anyhow!("Invalid database type: {}", db_type)),
        }
    }
}

/// Format for writing the output
#[derive(Debug)]
enum OutputFormat {
    Parquet,
    Csv,
    Json,
    Arrow,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_lowercase();
        let formt = s.split('.').last();
        match formt {
            Some("parquet") => Ok(OutputFormat::Parquet),
            Some("csv") => Ok(OutputFormat::Csv),
            Some("json") => Ok(OutputFormat::Json),
            Some("arrow") => Ok(OutputFormat::Arrow),
            format => Err(anyhow::anyhow!("Invalid output format: {:?}", format)),
        }
    }
}

impl OutputFormat {
    async fn write(
        &self,
        mut rx: Receiver<RecordBatch>,
        schema: Arc<Schema>,
        path: &Path,
    ) -> Result<()> {
        let file = fs::File::options().write(true).open(path)?;

        match self {
            OutputFormat::Parquet => {
                // write to parquet
                let mut writer = ArrowWriter::try_new(file, schema, None)?;

                while !rx.is_closed() {
                    if let Some(batch) = rx.recv().await {
                        writer.write(&batch)?;
                    }
                }
                writer.close()?;
                Ok(())
            }
            OutputFormat::Csv => {
                // write to csv
                let mut writer = arrow::csv::writer::Writer::new(file);
                while !rx.is_closed() {
                    if let Some(batch) = rx.recv().await {
                        writer.write(&batch)?;
                    }
                }
                Ok(())
            }
            OutputFormat::Json => {
                // write to json
                // create a builder that keeps keys with null values
                let builder = WriterBuilder::new().with_explicit_nulls(true);
                let mut writer = builder.build::<_, LineDelimited>(file);

                while !rx.is_closed() {
                    if let Some(batch) = rx.recv().await {
                        writer.write(&batch)?;
                    }
                }
                writer.finish()?;
                Ok(())
            }
            format => unimplemented!("Output format not supported: {:?}", format),
        }
    }
}
