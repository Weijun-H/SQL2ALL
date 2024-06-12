use std::{fs, path::PathBuf, str::FromStr, sync::Arc};

use crate::{OutputFormat, Query};

use anyhow::Result;
use arrow::{
    array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch,
};
use rusqlite::{Column, Connection, Row};
use tokio::sync::mpsc;

use super::conversion::MapArrowType;

pub struct SQLite {
    url: String,
}

impl SQLite {
    pub fn new(url: String) -> Self {
        SQLite { url }
    }

    fn convert_to_recordbatch(row: &Row, schema: &Arc<Schema>) -> Result<RecordBatch> {
        let mut columns = Vec::<ArrayRef>::new();

        for (i, field) in schema.fields.iter().enumerate() {
            match field.data_type() {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(1);
                    if let Ok(value) = row.get(i) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                    columns.append(&mut vec![Arc::new(builder.finish()) as ArrayRef]);
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    if let Ok(value) = row.get::<_, String>(i) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                    columns.append(&mut vec![Arc::new(builder.finish()) as ArrayRef]);
                }
                // Add more cases as needed for other data types
                data_type => unimplemented!("Data type not supported for column: {:?}", data_type),
            };
        }

        let record_batch = record_batch::RecordBatch::try_new(schema.clone(), columns)?;

        Ok(record_batch)
    }

    fn map_schema(columns: &[Column]) -> Arc<Schema> {
        let mut fields = Vec::<Field>::new();
        for column in columns.iter() {
            let data_type = column.map_arrow_type();
            fields.push(Field::new(column.name(), data_type, true));
        }
        Arc::new(Schema::new(fields))
    }
}
impl Query for SQLite {
    async fn query(&self, query: &str, output: &PathBuf) -> Result<()> {
        println!("Querying MySQL database with query: {}", query);

        let conn = Connection::open(self.url.clone())?;

        let format = Arc::new(OutputFormat::from_str(
            output.to_str().expect("Invalid output format"),
        )?);

        fs::File::create(output.clone())?;

        let mut stmt = conn.prepare(query)?;

        let schema = SQLite::map_schema(&stmt.columns());
        let mut stream = stmt.query([])?;

        let (tx, rx) = mpsc::channel(100);

        let output_cloned = output.clone();
        let schema_cloned = schema.clone();
        let write =
            tokio::spawn(async move { format.write(rx, schema_cloned, &output_cloned).await });

        while let Some(row) = stream.next()? {
            let batch = SQLite::convert_to_recordbatch(row, &schema)?;
            tx.send(batch).await?;
        }
        drop(tx);
        let _ = write.await?;

        println!("Done writing to file: {:?}", output);
        Ok(())
    }
}
