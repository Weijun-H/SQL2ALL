use crate::FromArrow;
use anyhow::Result;
use arrow::{
    array::{ArrayRef, Int32Builder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch,
};
use futures_util::{pin_mut, TryStreamExt};
use std::{fs, path::PathBuf, str::FromStr, sync::Arc};
use tokio_postgres::{types::Type, Row};

use crate::{OutputFormat, Query};

pub struct PostgreSQL {
    url: String,
}

impl PostgreSQL {
    pub fn new(url: String) -> Self {
        PostgreSQL { url }
    }

    fn map_arrow_type(columns: &[tokio_postgres::Column]) -> Arc<Schema> {
        let fields = columns
            .iter()
            .map(|column| {
                let name = column.name();
                let data_type = match column.type_() {
                    &Type::BOOL => DataType::Boolean,
                    &Type::INT2 => DataType::Int16,
                    &Type::INT4 => DataType::Int32,
                    &Type::FLOAT4 => DataType::Float32,
                    &Type::FLOAT8 => DataType::Float64,
                    &Type::BIT => DataType::Binary,
                    &Type::NUMERIC => DataType::Float64,
                    &Type::DATE => DataType::Date32,
                    &Type::TIME => DataType::Time32(arrow::datatypes::TimeUnit::Second),
                    &Type::VARCHAR => DataType::Utf8,
                    &Type::TIMESTAMP => {
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
                    }
                    // Add more cases as needed for other data types
                    column_type => {
                        unimplemented!("Data type not supported for column: {:?}", column_type)
                    }
                };
                Field::new(name, data_type, true)
            })
            .collect::<Vec<Field>>();
        Arc::new(Schema::new(fields))
    }
    fn convert_to_recordbatch(row: Row, schema: &Arc<Schema>) -> Result<RecordBatch> {
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

        let format = Arc::new(OutputFormat::from_str(
            output.to_str().expect("Invalid output format"),
        )?);

        // TODO: customize the query
        // Now we can execute a simple statement that just returns its parameter.
        let stream = client.query_raw(query, vec![""]).await?;

        pin_mut!(stream);

        fs::File::create(output.clone())?;

        let mut batches = vec![];
        let mut schema: Option<Arc<Schema>> = None;

        while let Some(row) = stream.try_next().await? {
            if schema.is_none() {
                schema = Some(PostgreSQL::map_arrow_type(row.columns()));
            }

            let batch = PostgreSQL::convert_to_recordbatch(
                row,
                &schema.clone().ok_or(anyhow::anyhow!("No schema"))?,
            )?;
            batches.push(batch);

            // write the batches to the parquet file
            // TODO: customize the batch size
            // TODO: avoid race condition
            if batches.len() == 100_000 {
                let batches_clone = batches.clone();
                let format_clone = format.clone();
                let output_clone = output.clone();

                let task = tokio::task::spawn(async move {
                    format_clone
                        .write(batches_clone, &output_clone)
                        .await
                        .unwrap();
                });
                task.await?;
                batches.clear();
            }
        }
        // write the remaining batches to the file
        if !batches.is_empty() {
            format.write(batches, output).await?;
        }
        println!("Done writing to file: {:?}", output);
        Ok(())
    }
}
