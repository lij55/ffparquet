use std::collections::HashMap;
use std::fs;
use std::hash::Hash;

use std::sync::Arc;

use async_std::task;
use clap::Parser;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaBuilder, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use log::{debug, info, warn};
use parquet::schema;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
/// run sql with datafusion and write the result to a parquet file
pub struct Args {
    #[arg(short, help = "Path to config file")]
    config: String,
}

pub(crate) fn df_main(args: Args) -> eyre::Result<()> {
    let contents = fs::read_to_string(args.config).expect("Should have been able to read the file");

    let cfg: DFConfig = serde_yaml::from_str::<DFConfig>(&contents)?;

    debug!("{:?}", cfg);

    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(true)
        .with_target_partitions(8)
        .with_information_schema(true)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_batch_size(6666);

    let ctx = SessionContext::new_with_config(config);

    for src in cfg.source {
        match src.format.as_str() {
            "parquet" => {
                task::block_on(ctx.register_parquet(
                    src.name.as_str(),
                    &format!("{}", src.path[0]),
                    ParquetReadOptions::default(),
                ))?;
            }
            "csv" => {
                let mut opt = CsvReadOptions::default();
                let mut sbuilder = SchemaBuilder::new();


                for col_def in src.schema {
                    let f = build_fields(col_def);
                    sbuilder.push(f);
                }

                let csv_schema = Arc::new(sbuilder.finish());

                opt.schema = Option::from(csv_schema.as_ref());
                //println!("{}","Int32".parse::<DataType>());

                task::block_on(ctx.register_csv(
                    src.name.as_str(),
                    &format!("{}", src.path[0]),
                    opt,
                ))?;
            }
            v => {
                warn!("unknown source format {v}, skipping")
            }
        }

    }


    let df = task::block_on(ctx.sql(cfg.query[0].sql.as_str()))?;

    let mut props = WriterProperties::builder()
        // file settings
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_created_by("pp".to_owned())
        .set_write_batch_size(16 * 1024 * 1024)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        //.set_statistics_enabled(EnabledStatistics::None)
        .set_max_statistics_size(1024);

    let file_parameters = cfg.sink.parameters;

    if file_parameters.contains_key("max_group_size") {
        let grout_size = match file_parameters
            .get("max_group_size")
            .unwrap()
            .to_uppercase()
            .as_str()
            .parse::<usize>()
        {
            Ok(v) => v,
            _ => {
                warn!(
                    "invalid max_group_size: {}, use default size",
                    file_parameters.get("max_group_size").unwrap()
                );
                1000000
            }
        };
        props = props.set_max_row_group_size(grout_size);
    }

    if file_parameters.contains_key("encoding") {
        let encoding_type = get_encoding(&file_parameters);
        props = props.set_encoding(encoding_type);
    };

    if file_parameters.contains_key("compression") {
        let compression_type = get_compression(&file_parameters);
        props = props.set_compression(compression_type);
    }

    let comumn_parameters = cfg.sink.columns;
    for cp in comumn_parameters {
        let name = match cp.get("name") {
            Some(v) => v.as_str(),
            None => {
                warn!("column name not found, skip");
                continue;
            }
        };
        info!("{}", name);
        if cp.contains_key("compression") {
            let compression_type = get_compression(&cp);
            props = props.set_column_compression(ColumnPath::from(name), compression_type);
        }
        if cp.contains_key("encoding") {
            let encoding_type = get_encoding(&cp);
            props = props.set_column_encoding(ColumnPath::from(name), encoding_type);
        }
    }

    let props = props.build();

    task::block_on(
        df.write_parquet(
            cfg.sink.path.as_str(),
            DataFrameWriteOptions::new()
                .with_overwrite(false)
                .with_single_file_output(true),
            Some(props),
        ),
    )
    .expect("TODO: panic message");

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct DFConfig {
    source: Vec<Source>,
    sink: Sink,
    query: Vec<Query>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Source {
    name: String,
    format: String,
    path: Vec<String>,
    schema: Vec<HashMap<String, String>>
}
#[derive(Debug, Serialize, Deserialize)]
struct Sink {
    name: String,
    format: String,
    path: String,
    parameters: HashMap<String, String>,
    columns: Vec<HashMap<String, String>>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Query {
    name: String,
    sql: String,
}

fn get_encoding(parameters: &HashMap<String, String>) -> Encoding {
    match parameters.get("encoding").unwrap().to_uppercase().as_str() {
        "PLAIN" => Encoding::PLAIN,
        "PLAIN_DICTIONARY" => Encoding::PLAIN_DICTIONARY,
        "RLE" => Encoding::RLE,
        "DELTA_BINARY_PACKED" => Encoding::DELTA_BINARY_PACKED,
        "DELTA_BYTE_ARRAY" => Encoding::DELTA_BYTE_ARRAY,
        "DELTA_LENGTH_BYTE_ARRAY" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        _ => {
            warn!(
                "invalid encoding: {}, use PLAIN by default",
                parameters.get("encoding").unwrap()
            );
            Encoding::PLAIN
        }
    }
}

fn get_compression(parameters: &HashMap<String, String>) -> Compression {
    match parameters
        .get("compression")
        .unwrap()
        .to_uppercase()
        .as_str()
    {
        "ZSTD" => Compression::ZSTD(ZstdLevel::default()),
        "SNAPPY" => Compression::SNAPPY,
        "UNCOMPRESSED" => Compression::UNCOMPRESSED,
        "LZ4" => Compression::LZ4,
        _ => {
            warn!(
                "unknown compression type: {}, use ZSTD by default",
                parameters.get("compression").unwrap()
            );
            Compression::ZSTD(ZstdLevel::default())
        }
    }
}

fn build_fields(col: HashMap<String, String>) -> Field {
     let (name, datatype) = col.into_iter().next().unwrap();
    let arrow_type = match datatype.as_str() {
        "timestamp" => DataType::Timestamp(TimeUnit::Second, None),
        "decimal" => DataType::Decimal128(20,10),
        _ => DataType::Utf8,
    };
    Field::new(name, arrow_type, false)

}