use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch;

use futures::StreamExt;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use mysql_async::{prelude::*, Column, Row};

use crate::{OutputFormat, OutputWriter, Query};

use super::conversion::MapArrowType;

pub struct MySQL {
    url: String,
}

impl MySQL {
    pub fn new(url: String) -> Self {
        MySQL { url }
    }

    fn convert_to_recordbatch(row: Row, schema: &Arc<Schema>) -> Result<RecordBatch> {
        let mut columns = Vec::<ArrayRef>::new();

        for (i, field) in schema.fields.iter().enumerate().take(row.len()) {
            match field.data_type() {
                DataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(1);
                    if let Some(value) = row.get::<i32, _>(i) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                    columns.append(&mut vec![Arc::new(builder.finish()) as ArrayRef]);
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(1);
                    if let Some(value) = row.get::<i64, _>(i) {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                    columns.append(&mut vec![Arc::new(builder.finish()) as ArrayRef]);
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    if let Some(Ok(value)) = row.get_opt::<String, _>(i) {
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
            fields.push(Field::new(column.name_str(), data_type, true));
        }
        Arc::new(Schema::new(fields))
    }
}

impl Query for MySQL {
    async fn query(&self, query: &str, output: &PathBuf) -> Result<()> {
        println!("Querying MySQL database with query: {}", query);

        let pool = mysql_async::Pool::new(self.url.clone().as_str());
        let mut conn = pool.get_conn().await?;

        let format = Arc::new(OutputFormat::from_str(
            output.to_str().expect("Invalid output format"),
        )?);

        // TODO: customize the query
        let mut stream = conn.query_iter(query).await?;
        let mut stream = stream
            .stream::<Row>()
            .await?
            .ok_or_else(|| anyhow::anyhow!("No rows"))?;

        fs::File::create(output.clone())?;

        let mut batches = vec![];
        let schema = MySQL::map_schema(&stream.columns());

        while let Some(row) = stream.next().await {
            let batch = MySQL::convert_to_recordbatch(row?, &schema)?;
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

#[cfg(test)]
mod tests {
    use rand::Rng;
    use serde::Serialize;

    // only for testing
    #[derive(Serialize, Clone, Debug, ParquetRecordWriter)]
    pub struct Payment {
        pub customer_id: i32,
        pub amount: i32,
        pub account_name: Option<String>,
    }

    fn generate_test_data(size: usize) -> Vec<Payment> {
        let mut rng = rand::thread_rng();
        let mut payments: Vec<Payment> = Vec::with_capacity(size);

        for _ in 0..size {
            let customer_id = rng.gen_range(1..1000);
            let amount = rng.gen_range(10..1000);

            let account_name = if rng.gen::<bool>() {
                Some(format!("Account {}", rng.gen_range(1..100)))
            } else {
                None
            };

            let payment = Payment {
                customer_id,
                amount,
                account_name,
            };

            payments.push(payment);
        }

        payments
    }
}
