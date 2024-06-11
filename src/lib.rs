use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Ok, Result};

use arrow::{
    array::RecordBatch,
    json::{writer::LineDelimited, WriterBuilder},
};
use db::mysql::MySQL;
use db::postgresql::PostgreSQL;
use parquet::arrow::ArrowWriter;

extern crate parquet;
#[macro_use]
extern crate parquet_derive;

mod db;

pub enum Database {
    MySQL(MySQL),
    PostgreSQL(PostgreSQL),
    SQLite,
}

trait Query {
    async fn query(&self, query: &str, output: &PathBuf) -> Result<()>;
}

impl Database {
    pub async fn query(&self, query: &str, output: &PathBuf) -> Result<()> {
        match self {
            Database::MySQL(mysql) => mysql.query(query, output).await,
            Database::PostgreSQL(postgres) => postgres.query(query, output).await,
            Database::SQLite => unimplemented!("SQLite not implemented"),
        }
    }
}

/// From url to database
impl FromStr for Database {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let db_type = s.split(':').next().unwrap();
        match db_type {
            "mysql" => Ok(Database::MySQL(MySQL::new(s.to_string()))),
            // TODO: Implement Postgres and SQLite
            "postgresql" => Ok(Database::PostgreSQL(PostgreSQL::new(s.to_string()))),
            "sqlite" => Ok(Database::SQLite),
            format => Err(anyhow::anyhow!("Invalid database type: {}", format)),
        }
    }
}

/// Writer for converting RecordBatch to output format
trait OutputWriter {
    async fn write(&self, batches: Vec<RecordBatch>, output: &Path) -> Result<()>;
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

impl OutputWriter for OutputFormat {
    async fn write(&self, batches: Vec<RecordBatch>, path: &Path) -> Result<()> {
        let file = fs::File::options().append(true).open(path)?;

        match self {
            OutputFormat::Parquet => {
                // write to parquet
                let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)?;

                for batch in batches {
                    writer.write(&batch)?
                }
                writer.close()?;

                Ok(())
            }
            OutputFormat::Csv => {
                // write to csv
                let mut writer = arrow::csv::writer::Writer::new(file);
                for batch in batches {
                    writer.write(&batch)?
                }
                Ok(())
            }
            OutputFormat::Json => {
                // write to json
                // create a builder that keeps keys with null values
                let builder = WriterBuilder::new().with_explicit_nulls(true);
                let mut writer = builder.build::<_, LineDelimited>(file);

                for batch in batches {
                    writer.write(&batch)?
                }
                writer.finish()?;
                Ok(())
            }
            format => unimplemented!("Output format not supported: {:?}", format),
        }
    }
}
