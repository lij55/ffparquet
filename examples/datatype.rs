use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

fn main() -> eyre::Result<()> {
    gendata_0001()
}

fn gendata_0001() -> eyre::Result<()> {
    // replace Array type, Array data, schema data type, column name and target filename

    // output
    let output = "data_001.parquet";

    // column c1
    let c1_array = Int32Array::from(vec![1, 2, -3, 4, -5]);
    // column c2
    let c2_array = UInt32Array::from(vec![1, 2, 3, 4, 5]);

    // schema def
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, false),  // column 1
        Field::new("c2", DataType::UInt32, false), // column 2
    ]);


    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(c1_array), Arc::new(c2_array)],
    ).unwrap();

    let file = File::create(output)?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(86400 / 4 / 6)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_created_by("op".into())
        .set_statistics_enabled(EnabledStatistics::None)
        .set_encoding(Encoding::PLAIN)
        .set_write_batch_size(16 * 1024 * 1024)
        .set_dictionary_enabled(false)
        .set_max_statistics_size(1024)
        .build();
    let mut writer = ArrowWriter::try_new(file, SchemaRef::from(schema), Some(props))?;

    writer.write(&batch)?;
    writer.flush()?;
    writer.close()?;
    Ok(())
}