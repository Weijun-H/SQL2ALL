use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch;
use futures::StreamExt;
use std::str::FromStr;
use std::sync::Arc;
use std::{fs, path::Path};

use arrow_array::{ArrayRef, RecordBatch};
use mysql_async::{prelude::*, Row};

use crate::{FromArrow, OutputFormat, Query};

pub struct MySQL {
    url: String,
}

impl MySQL {
    pub fn new(url: String) -> Self {
        MySQL { url }
    }
}

impl Query for MySQL {
    async fn query(&self, query: &str, output: &Path) -> Result<()> {
        println!("Querying MySQL database with query: {}", query);

        let pool = mysql_async::Pool::new(self.url.clone().as_str());
        let mut conn = pool.get_conn().await?;

        let format = OutputFormat::from_str(output.to_str().expect("Invalid output format"))?;

        // TODO: customize the query
        let mut stream = conn.query_iter(query).await?;
        let mut stream = stream
            .stream::<Row>()
            .await?
            .ok_or_else(|| anyhow::anyhow!("No rows"))?;

        fs::File::create(output)?;

        let mut batches = vec![];
        while let Some(row) = stream.next().await {
            let batch = convert_to_recordbatch(row?).await?;
            batches.push(batch);

            // write the batches to the parquet file
            // TODO: customize the batch size
            if batches.len() == 500_000 {
                println!("Batch size: {}", batches.len());
                // TODO: speed up the writing process, e.g. by using a thread pool
                format.write(batches.iter().collect(), output)?;
                batches.clear();
            }
        }
        if !batches.is_empty() {
            format.write(batches.iter().collect(), output)?;
        }
        Ok(())
    }
}

async fn convert_to_recordbatch(row: Row) -> Result<RecordBatch> {
    let mut schemas: Vec<Field> = vec![];

    row.columns().iter().for_each(|column| {
        let data_type = map_column_type_to_arrow_data_type(column.column_type());
        let field = Field::new(column.name_str(), data_type, true);
        schemas.push(field);
    });

    let mut columns = Vec::<ArrayRef>::new();

    for (i, _) in schemas.iter().enumerate().take(row.len()) {
        match schemas[i].data_type() {
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

    let record_batch = record_batch::RecordBatch::try_new(Arc::new(Schema::new(schemas)), columns)?;

    Ok(record_batch)
}

// TODO: deprecate later
fn map_column_type_to_arrow_data_type(column_type: mysql_async::consts::ColumnType) -> DataType {
    match column_type {
        mysql_async::consts::ColumnType::MYSQL_TYPE_INT24 => arrow::datatypes::DataType::Int32,
        mysql_async::consts::ColumnType::MYSQL_TYPE_LONG => arrow::datatypes::DataType::Int32,
        mysql_async::consts::ColumnType::MYSQL_TYPE_FLOAT => arrow::datatypes::DataType::Float64,
        mysql_async::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            arrow::datatypes::DataType::Float64
        }
        mysql_async::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        mysql_async::consts::ColumnType::MYSQL_TYPE_VARCHAR => arrow::datatypes::DataType::Utf8,
        mysql_async::consts::ColumnType::MYSQL_TYPE_VAR_STRING => arrow::datatypes::DataType::Utf8,
        mysql_async::consts::ColumnType::MYSQL_TYPE_BLOB => arrow::datatypes::DataType::Utf8,
        // Add more cases as needed for other data types
        _ => unimplemented!("Data type not supported for column: {:?}", column_type),
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
