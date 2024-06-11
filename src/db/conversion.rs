use arrow::datatypes::{DataType, TimeUnit};
use tokio_postgres::types::Type;

/// Map the database column type to Arrow data type
pub trait MapArrowType {
    fn map_arrow_type(&self) -> DataType;
}

impl MapArrowType for tokio_postgres::Column {
    fn map_arrow_type(&self) -> DataType {
        match self.type_() {
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
            &Type::TIMESTAMP => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            // Add more cases as needed for other data types
            column_type => {
                unimplemented!("Data type not supported for column: {:?}", column_type)
            }
        }
    }
}

impl MapArrowType for mysql_async::Column {
    fn map_arrow_type(&self) -> DataType {
        match self.column_type() {
            mysql_async::consts::ColumnType::MYSQL_TYPE_INT24 => DataType::Int32,
            mysql_async::consts::ColumnType::MYSQL_TYPE_LONG => DataType::Int32,
            mysql_async::consts::ColumnType::MYSQL_TYPE_FLOAT => {
                arrow::datatypes::DataType::Float64
            }
            mysql_async::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => DataType::Float64,
            mysql_async::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            mysql_async::consts::ColumnType::MYSQL_TYPE_VARCHAR => DataType::Utf8,
            mysql_async::consts::ColumnType::MYSQL_TYPE_VAR_STRING => DataType::Utf8,
            mysql_async::consts::ColumnType::MYSQL_TYPE_BLOB => DataType::Utf8,
            // Add more cases as needed for other data types
            column_type => {
                unimplemented!("Data type not supported for column: {:?}", column_type)
            }
        }
    }
}
