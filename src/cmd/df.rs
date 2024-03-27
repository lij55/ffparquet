use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
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
use eyre::{Error, OptionExt};
use log::{debug, info, warn};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Parser, Debug)]
/// run sql with datafusion and write the result to a parquet file
pub struct Args {
    #[arg(short, help = "Path to config file")]
    config: String,

    #[arg(long)]
    /// query name to run, default is 'default'
    query: Option<String>,

    #[arg(short, long, help = "source file to replace in config file")]
    source: Vec<String>,

    #[arg(long, help = "target file")]
    sink: Option<String>,
}

pub(crate) fn df_main(args: Args) -> eyre::Result<()> {
    let contents = fs::read_to_string(args.config).expect("Should have been able to read the file");

    let cfg: DFConfig = serde_yaml::from_str::<DFConfig>(&contents)?;

    debug!("{:?}", cfg);

    // error if there is no query defined in config file
    if cfg.query.len() == 0 {
        return Err(Error::msg("no sql query provided"));
    }

    // error if the args- or default query not in config file
    if args.query.is_some() {
        if !cfg.query.contains_key(args.query.clone().unwrap().as_str()) {
            return Err(Error::msg("query not found in config"));
        }
    } else {
        if !cfg.query.contains_key("default") {
            return Err(Error::msg("no default query config nor arguments"));
        }
    }

    // // error if source number in args and config doesn't match.
    // if args.source.len() > 0 && args.source.len() != cfg.source.len() {
    //     return Err(Error::msg("source number in config and args not match"));
    // }

    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(true)
        .with_target_partitions(8)
        .with_information_schema(true)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true)
        .with_batch_size(6666);

    let ctx = SessionContext::new_with_config(config);

    for (idx, src) in cfg.source.iter().enumerate() {
        let path = match args.source.get(idx) {
            None => src.path.clone(),
            Some(v) => v.clone(),
        };

        match src.format.as_str() {
            "parquet" => {
                info!("reginster parquet {}", src.name.as_str());
                task::block_on(ctx.register_parquet(
                    src.name.as_str(),
                    &format!("{}", path),
                    ParquetReadOptions::default(),
                ))?;
            }
            "csv" => {
                let mut opt = CsvReadOptions::default();
                let mut sbuilder = SchemaBuilder::new();

                opt.has_header = src.header.unwrap_or_else(|| false);
                let schema = src.schema.clone().unwrap_or_default();

                for col_def in schema {
                    let f = build_fields(&col_def);
                    sbuilder.push(f);
                }

                let csv_schema = Arc::new(sbuilder.finish());

                opt.schema = Option::from(csv_schema.as_ref());
                //println!("{}","Int32".parse::<DataType>());

                task::block_on(ctx.register_csv(src.name.as_str(), &format!("{}", path), opt))?;
            }
            "json" => {
                task::block_on(ctx.register_json(
                    src.name.as_str(),
                    &format!("{}", path),
                    NdJsonReadOptions::default(),
                ))?;
            }
            "avro" => {
                task::block_on(ctx.register_avro(
                    src.name.as_str(),
                    &format!("{}", path),
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
        .set_max_statistics_size(1024);

    let file_parameters = cfg.sink.parameters;

    if file_parameters.contains_key("statistic") {
        let enable_statistic = file_parameters.get("statistic").unwrap().to_lowercase();
        props = match enable_statistic.as_str() {
            "false" => props.set_statistics_enabled(EnabledStatistics::None),
            "true" => props.set_statistics_enabled(EnabledStatistics::Chunk),
            _ => props,
        }
    }

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

        if cp.contains_key("compression") {
            let compression_type = get_compression(&cp);
            props = props.set_column_compression(ColumnPath::from(name), compression_type);
        }
        if cp.contains_key("encoding") {
            let encoding_type = get_encoding(&cp);
            props = props.set_column_encoding(ColumnPath::from(name), encoding_type);
        }
        if cp.contains_key("statistic") {
            let enable_statistic = cp.get("statistic").unwrap().to_lowercase();
            // info!("for sttics {}, {}", name, enable_statistic);
            match enable_statistic.as_str() {
                "false" => {
                    props = props.set_column_statistics_enabled(
                        ColumnPath::from(name),
                        EnabledStatistics::None,
                    )
                }
                "true" => {
                    props = props.set_column_statistics_enabled(
                        ColumnPath::from(name),
                        EnabledStatistics::Chunk,
                    )
                }
                _ => {
                    warn!("unknown statistic type {}, skip", enable_statistic);
                }
            }
        }
    }

    if cfg.sink.format == "s3" {
        let s3cfg = cfg.sink.s3.ok_or_eyre("s3 format without config")?;
        let bucket_name = s3cfg.get("bucket").ok_or_eyre(Error::msg("no buucket"))?;
        let region = s3cfg.get("region").ok_or_eyre(Error::msg("no region"))?;
        let key_id = s3cfg
            .get("access_id")
            .ok_or_eyre(Error::msg("no access_id"))?;
        let secret_key = s3cfg
            .get("secret_key")
            .ok_or_eyre(Error::msg("no secret_key"))?;
        let endpoint = s3cfg
            .get("endpoint")
            .ok_or_eyre(Error::msg("no endpoint"))?;

        let s3 = AmazonS3Builder::new()
            .with_allow_http(true)
            .with_bucket_name(bucket_name)
            .with_region(region)
            .with_access_key_id(key_id)
            .with_secret_access_key(secret_key)
            .with_endpoint(endpoint)
            .build()?;

        let path = format!("s3://{bucket_name}");
        let s3_url = Url::parse(&path).unwrap();
        ctx.runtime_env()
            .register_object_store(&s3_url, Arc::new(s3));
    }
    // query search order: cmd, default
    let query_name = args.query.unwrap_or_else(|| format!("default"));

    let query = cfg.query.get(query_name.as_str()).unwrap();

    let df = task::block_on(ctx.sql(query.as_str()))?;

    let target_name = args.sink.unwrap_or_else(|| cfg.sink.path.clone());

    let props = props.build();

    task::block_on(
        df.write_parquet(
            target_name.as_str(),
            DataFrameWriteOptions::new()
                .with_overwrite(false)
                .with_single_file_output(true),
            Some(props),
        ),
    )
    .expect(format!("writing parquet {} failed", target_name).as_str());

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
    path: String,
    schema: Option<Vec<HashMap<String, String>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sink {
    format: String,
    path: String,
    parameters: HashMap<String, String>,
    columns: Vec<HashMap<String, String>>,
    s3: Option<HashMap<String, String>>,
}

fn get_encoding(parameters: &HashMap<String, String>) -> Encoding {
    let encoding = match parameters.get("encoding") {
        Some(v) => String::from(v).to_uppercase(),
        None => String::from("PLAIN"),
    };
    Encoding::from_str(encoding.as_str()).unwrap_or_else(|_| {
        warn!("unknown encoding type: {}, use PLAIN by default", encoding);
        Encoding::PLAIN
    })
}

fn get_compression(parameters: &HashMap<String, String>) -> Compression {
    let compression = match parameters.get("compression") {
        Some(v) => String::from(v).to_uppercase(),
        None => String::from("ZSTD"),
    };

    Compression::from_str(compression.as_str()).unwrap_or_else(|_| {
        warn!(
            "unknown compression type: {}, use ZSTD by default",
            compression
        );
        Compression::ZSTD(ZstdLevel::default())
    })
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
