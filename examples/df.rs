use async_std::task;
use datafusion::{dataframe::DataFrameWriteOptions, prelude::*};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use datafusion::parquet::schema::types::ColumnPath;
use env_logger;
use eyre::Result;

//use parquet::basic::Compression;

#[tokio::main]
async fn main() -> Result<()> {
    let df = run_df_sql_local()?;

    let props = WriterProperties::builder()
        // file settings
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_write_batch_size(16 * 1024 * 1024)
        // .set_data_page_size_limit(10)
        // .set_dictionary_page_size_limit(20)
        .set_max_row_group_size(86400)
        .set_created_by("pp".to_owned())
        // global column settings
        .set_encoding(Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding(
            ColumnPath::from("ss_sold_date_sk"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_compression(
            ColumnPath::from("ss_sold_date_sk"),
            Compression::ZSTD(ZstdLevel::default()),
        )
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        //.set_statistics_enabled(EnabledStatistics::None)
        .set_max_statistics_size(1024)
        .build();

    df.write_parquet(
        "./datafusion-examples/test_parquet/test.parquet",
        DataFrameWriteOptions::new(),
        Some(props),
    )
    .await?;

    Ok(())
}

fn run_df_sql_local() -> Result<DataFrame> {
    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(true)
        .with_target_partitions(8)
        .with_information_schema(true)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_batch_size(6666);

    let ctx = SessionContext::new_with_config(config);

    task::block_on(ctx.register_parquet(
        "hits",
        &format!("store_sales.parquet"),
        ParquetReadOptions::default(),
    ))?;

    match task::block_on(ctx.sql("SELECT *  FROM hits order by 1 limit 86400")) {
        Ok(df) => Ok(df),
        Err(e) => Err(e.into()),
    }
}
