use std::{fs, path::Path, str::FromStr};

use anyhow::Result;
use arrow_array::RecordBatch;
use db::mysql::MySQL;
use parquet::arrow::ArrowWriter;

extern crate parquet;
#[macro_use]
extern crate parquet_derive;

mod db;

pub enum Database {
    MySQL(MySQL),
    Postgres,
    SQLite,
}

trait Query {
    async fn query(&self, query: &str, output: &Path) -> Result<()>;
}

impl Database {
    pub async fn query(&self, query: &str, output: &Path) -> Result<()> {
        match self {
            Database::MySQL(mysql) => mysql.query(query, output).await,
            Database::Postgres => unimplemented!("Postgres not implemented"),
            Database::SQLite => unimplemented!("SQLite not implemented"),
        }
    }
}

// From url to database
impl FromStr for Database {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let db_type = s.split(':').next().unwrap();
        match db_type {
            "mysql" => Ok(Database::MySQL(MySQL::new(s.to_string()))),
            // TODO: Implement Postgres and SQLite
            "postgres" => Ok(Database::Postgres),
            "sqlite" => Ok(Database::SQLite),
            format => Err(anyhow::anyhow!("Invalid database type: {}", format)),
        }
    }
}

trait FromArrow {
    fn write(&self, batches: Vec<&RecordBatch>, output: &Path) -> Result<()>;
}

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

impl FromArrow for OutputFormat {
    fn write(&self, batches: Vec<&RecordBatch>, path: &Path) -> Result<()> {
        match self {
            OutputFormat::Parquet => {
                // write to parquet
                let file = fs::File::options().append(true).open(path)?;
                let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)?;

                for batch in batches {
                    writer.write(batch)?
                }
                writer.close()?;

                Ok(())
            }
            format => unimplemented!("Output format not supported: {:?}", format),
        }
    }
}
