use anyhow::Result;
use arrow::{
    array::{ArrayRef, Int32Builder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch,
};
use futures_util::{pin_mut, TryStreamExt};
use std::{fs, path::PathBuf, str::FromStr, sync::Arc};
use tokio::sync::mpsc;
use tokio_postgres::{Column, Row};

use crate::{OutputFormat, Query};

use super::conversion::MapArrowType;

pub struct PostgreSQL {
    url: String,
}

impl PostgreSQL {
    pub fn new(url: String) -> Self {
        PostgreSQL { url }
    }

    /// Convert a PostgreSQL row to Arrow RecordBatch
    fn convert_to_recordbatch(row: &Row, schema: &Arc<Schema>) -> Result<RecordBatch> {
        let mut columns = Vec::<ArrayRef>::new();

        for (i, field) in schema.fields.iter().enumerate().take(row.len()) {
            match field.data_type() {
                DataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(1);
                    if let Ok(value) = row.try_get(i) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                    columns.append(&mut vec![Arc::new(builder.finish()) as ArrayRef]);
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(1);
                    if let Ok(value) = row.try_get(i) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                    columns.append(&mut vec![Arc::new(builder.finish()) as ArrayRef]);
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    if let Ok(value) = row.try_get::<_, String>(i) {
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

    /// Map the PostgreSQL columns to Arrow Schema
    fn map_scheme(columns: &[Column]) -> Arc<Schema> {
        let fields = columns
            .iter()
            .map(|column| {
                let data_type = column.map_arrow_type();
                Field::new(column.name(), data_type, true)
            })
            .collect::<Vec<Field>>();
        Arc::new(Schema::new(fields))
    }
}

impl Query for PostgreSQL {
    async fn query(&self, query: &str, output: &PathBuf) -> Result<()> {
        println!("Querying PostgreSQL database with query: {}", query);
        let postgres = PostgreSQL::new(self.url.clone());

        let (client, connection) =
            tokio_postgres::connect(postgres.url.as_str(), tokio_postgres::NoTls)
                .await
                .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // TODO: customize the query
        // Now we can execute a simple statement that just returns its parameter.
        let stream = client.query_raw(query, vec![""]).await?;

        pin_mut!(stream);

        let (tx, rx) = mpsc::channel(100);

        let output_cloned = output.clone();
        let format = Arc::new(OutputFormat::from_str(
            output.to_str().expect("Invalid output format"),
        )?);

        let mut schema: Arc<Schema> = Arc::new(Schema::new(vec![Field::new(
            "column",
            DataType::Utf8,
            true,
        )]));
        fs::File::create(output.clone())?;
        let mut write = tokio::spawn(async { Ok(()) });

        if let Some(row) = stream.try_next().await? {
            schema = PostgreSQL::map_scheme(row.columns());
            let schema_clone = schema.clone();

            write =
                tokio::spawn(async move { format.write(rx, schema_clone, &output_cloned).await });

            let batch = PostgreSQL::convert_to_recordbatch(&row, &schema)?;
            tx.send(batch).await?;
        }

        while let Some(row) = stream.try_next().await? {
            let batch = PostgreSQL::convert_to_recordbatch(&row, &schema)?;
            tx.send(batch).await?;
        }
        drop(tx);
        let _ = write.await?;

        println!("Done writing to file: {:?}", output);
        Ok(())
    }
}
