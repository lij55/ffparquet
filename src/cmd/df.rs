use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use async_std::task;
use clap::Parser;
use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionConfig,
    SessionContext,
};
use eyre::Error;
use log::{debug, info, warn};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Parser, Debug)]
/// run sql with datafusion and write the result to a parquet file
pub struct Args {
    #[arg(short, help = "Path to config file")]
    config: String,

    #[arg(long, help = "query name to run")]
    query: Option<String>,

    #[arg(long, help = "source file")]
    source: Option<String>,

    #[arg(long, help = "target file")]
    sink: Option<String>,
}

pub(crate) fn df_main(args: Args) -> eyre::Result<()> {
    let contents = fs::read_to_string(args.config).expect("Should have been able to read the file");

    let cfg: DFConfig = serde_yaml::from_str::<DFConfig>(&contents)?;

    debug!("{:?}", cfg);

    if cfg.query.len() == 0 {
        //error!("no sql query provided");
        return Err(Error::msg("no sql query provided"));
    }

    if args.query.is_some() {
        if !cfg.query.contains_key(args.query.clone().unwrap().as_str()) {
            return Err(Error::msg("query not found in config"));
        }
    } else {
        if !cfg.query.contains_key("default") {
            return Err(Error::msg("no default query config nor arguments"));
        }
    }

    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(true)
        .with_target_partitions(8)
        .with_information_schema(true)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_batch_size(6666);

    let ctx = SessionContext::new_with_config(config);

    for (idx, src) in cfg.source.iter().enumerate() {
        let source_name = match idx {
            0 => args.source.clone().unwrap_or_else(|| src.path[0].clone()),
            _ => src.path[0].clone(),
        };

        match src.format.as_str() {
            "parquet" => {
                task::block_on(ctx.register_parquet(
                    source_name.as_str(),
                    &format!("{}", src.path[0]),
                    ParquetReadOptions::default(),
                ))?;
            }
            "csv" => {
                let mut opt = CsvReadOptions::default();
                let mut sbuilder = SchemaBuilder::new();

                opt.has_header = src.header.unwrap_or_else(|| false);

                for col_def in &src.schema {
                    let f = build_fields(col_def);
                    sbuilder.push(f);
                }

                let csv_schema = Arc::new(sbuilder.finish());

                opt.schema = Option::from(csv_schema.as_ref());
                //println!("{}","Int32".parse::<DataType>());

                task::block_on(ctx.register_csv(
                    source_name.as_str(),
                    &format!("{}", src.path[0]),
                    opt,
                ))?;
            }
            "json" => {
                task::block_on(ctx.register_json(
                    source_name.as_str(),
                    &format!("{}", src.path[0]),
                    NdJsonReadOptions::default(),
                ))?;
            }
            "avro" => {
                task::block_on(ctx.register_avro(
                    source_name.as_str(),
                    &format!("{}", src.path[0]),
                    AvroReadOptions::default(),
                ))?;
            }
            v => {
                warn!("unknown source format {v}, skipping")
            }
        }
    }

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
        // info!("{}", name);
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

    let bucket_name = "testdata";
    let s3 = AmazonS3Builder::new()
        .with_allow_http(true)
        .with_bucket_name(bucket_name)
        .with_region("test")
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_endpoint("http://127.0.0.1:9000")
        .build()?;

    let path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&path).unwrap();
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));

    // query search order: cmd, default
    let query_name = args.query.unwrap_or_else(|| format!("default"));

    let query = cfg.query.get(query_name.as_str()).unwrap();

    let df = task::block_on(ctx.sql(query.as_str()))?;

    let target_name = args.sink.unwrap_or_else(|| cfg.sink.path.clone());

    task::block_on(
        df.write_parquet(
            target_name.as_str(),
            DataFrameWriteOptions::new()
                .with_overwrite(false)
                .with_single_file_output(true),
            Some(props),
        ),
    )
    .expect(format!("writing parquet {} failed", cfg.sink.path).as_str());

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct DFConfig {
    source: Vec<Source>,
    sink: Sink,
    query: HashMap<String, String>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Source {
    name: String,
    format: String,
    header: Option<bool>,
    path: Vec<String>,
    schema: Vec<HashMap<String, String>>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Sink {
    name: String,
    format: String,
    path: String,
    parameters: HashMap<String, String>,
    columns: Vec<HashMap<String, String>>,
    s3: HashMap<String, String>,
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

fn build_fields(col: &HashMap<String, String>) -> Field {
    let (name, datatype) = col.into_iter().next().unwrap();
    let arrow_type = match datatype.as_str() {
        "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "decimal" => DataType::Decimal128(20, 10),
        _ => DataType::Utf8,
    };
    Field::new(name, arrow_type, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_compression() {
        let mut parameters = HashMap::new();
        parameters.insert("compression".to_owned(), "zstd".to_owned());
        assert_eq!(
            get_compression(&parameters),
            Compression::ZSTD(ZstdLevel::default())
        );
    }

    #[test]
    fn test_get_compression_bad_value() {
        let mut parameters = HashMap::new();
        parameters.insert("compression".to_owned(), "bad".to_owned());
        assert_eq!(
            get_compression(&parameters),
            Compression::ZSTD(ZstdLevel::default())
        )
    }

    #[test]
    fn test_get_encoding() {
        let mut parameters = HashMap::new();
        parameters.insert("encoding".to_owned(), "PLAIN".to_owned());
        assert_eq!(get_encoding(&parameters), Encoding::PLAIN);
    }

    #[test]
    fn test_get_encoding_bad_value() {
        let mut parameters = HashMap::new();
        parameters.insert("encoding".to_owned(), "bad".to_owned());
        assert_eq!(get_encoding(&parameters), Encoding::PLAIN)
    }
}
