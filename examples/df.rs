use async_std::task;
use clap::Parser;
use datafusion::{dataframe::DataFrameWriteOptions, prelude::*};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use env_logger;
use eyre::Result;

//use parquet::basic::Compression;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short)]
    input: String,
    #[arg(short, default_value = "output.parquet")]
    output: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let df = run_df_sql_local(args.input.as_str())?;

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
        // .set_column_encoding(
        //     ColumnPath::from("collect_time"),
        //     Encoding::DELTA_BINARY_PACKED,
        // )
        // .set_column_compression(
        //     ColumnPath::from("collect_time"),
        //     Compression::ZSTD(ZstdLevel::default()),
        // )
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        //.set_statistics_enabled(EnabledStatistics::None)
        .set_max_statistics_size(1024)
        .build();

    df.write_parquet(
        args.output.as_str(),
        DataFrameWriteOptions::new()
            .with_overwrite(false)
            .with_single_file_output(false),
        Some(props),
    )
    .await?;

    Ok(())
}

fn run_df_sql_local(path: &str) -> Result<DataFrame> {
    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(true)
        .with_target_partitions(8)
        .with_information_schema(true)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_batch_size(6666);

    let ctx = SessionContext::new_with_config(config);

    task::block_on(ctx.register_parquet(
        "source_table",
        &format!("{path}"),
        ParquetReadOptions::default(),
    ))?;

    match task::block_on(ctx.sql("SELECT *  FROM source_table order by 1")) {
        Ok(df) => Ok(df),
        Err(e) => Err(e.into()),
    }
}
