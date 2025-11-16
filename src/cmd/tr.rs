use std::fs::File;
use std::io::Read;

use arrow::array::RecordBatch;
use arrow::compute::{
    concat_batches, lexsort_to_indices, take, SortColumn, SortOptions, TakeOptions,
};
use clap::Parser;
use eyre::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde::Deserialize;

use crate::cmd::utils::open_file;

#[derive(Debug, Parser)]
/// Transcode a parquet file based on a yaml config
pub struct Args {
    /// Path to the yaml config file
    #[clap(short, long)]
    pub config: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: Vec<Source>,
    pub sink: Sink,
}

#[derive(Debug, Deserialize)]
pub struct Source {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct Sink {
    pub path: String,
    pub parameters: Parameters,
    pub columns: Vec<Column>,
    pub order: Option<Vec<OrderColumn>>,
}

#[derive(Debug, Deserialize)]
pub struct Parameters {
    pub compression: String,
    pub encoding: String,
    pub statistic: bool,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    pub compression: Option<String>,
    pub encoding: Option<String>,
    pub statistic: Option<bool>,
    pub dictionary: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct OrderColumn {
    pub name: String,
    pub descending: Option<bool>,
}

fn get_compression(compression: &str) -> Compression {
    match compression {
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(20).unwrap()),
        "gzip" => Compression::GZIP(GzipLevel::try_new(6).unwrap()),
        "brotli" => Compression::BROTLI(BrotliLevel::default()),
        "lz4" => Compression::LZ4_RAW,
        _ => Compression::UNCOMPRESSED,
    }
}

fn get_encoding(encoding: &str) -> Encoding {
    match encoding {
        "plain" => Encoding::PLAIN,
        "delta_binary_packed" => Encoding::DELTA_BINARY_PACKED,
        "delta_length_byte_array" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        "delta_byte_array" => Encoding::DELTA_BYTE_ARRAY,
        "rle" => Encoding::RLE,
        _ => Encoding::PLAIN,
    }
}

fn get_statistics(statistic: bool) -> EnabledStatistics {
    if statistic {
        EnabledStatistics::Chunk
    } else {
        EnabledStatistics::None
    }
}


pub fn run(args: Args) -> Result<()> {
    let mut file = File::open(args.config)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let config: Config = serde_yaml::from_str(&contents)?;
    // --- Reader ---
    let source_file = open_file(&config.source[0].path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(source_file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    let mut batches = vec![];
    for batch in reader {
        batches.push(batch?);
    }

    // --- Writer ---
    let sink_file = File::create(&config.sink.path)?;
    let mut props_builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(get_compression(&config.sink.parameters.compression))
        .set_encoding(get_encoding(&config.sink.parameters.encoding))
        .set_statistics_enabled(get_statistics(config.sink.parameters.statistic));

    if let Some(order_columns) = &config.sink.order {
        let sorting_columns = order_columns
            .iter()
            .map(|c| {
                let column_index = schema.index_of(&c.name).unwrap();
                let descending = c.descending.unwrap_or(false);
                parquet::format::SortingColumn::new(column_index as i32, descending, false)
            })
            .collect::<Vec<_>>();
        props_builder = props_builder.set_sorting_columns(Some(sorting_columns));
    }

    for column in &config.sink.columns {
        let compression = column
            .compression
            .as_deref()
            .unwrap_or(&config.sink.parameters.compression);
        let encoding = column
            .encoding
            .as_deref()
            .unwrap_or(&config.sink.parameters.encoding);
        let statistic = column.statistic.unwrap_or(config.sink.parameters.statistic);
        let dictionary_enabled = column.dictionary.unwrap_or(false);

        let path: ColumnPath = column.name.clone().into();
        props_builder = props_builder
            .set_column_compression(path.clone(), get_compression(compression))
            .set_column_encoding(path.clone(), get_encoding(encoding))
            .set_column_dictionary_enabled(path.clone(), dictionary_enabled)
            .set_column_statistics_enabled(path, get_statistics(statistic));
    }
    let props = props_builder.build();
    let mut writer = ArrowWriter::try_new(sink_file, schema.clone(), Some(props))?;

    // --- Transcode record batch by record batch ---
    if let Some(order_columns) = &config.sink.order {
        let batch = concat_batches(&schema, &batches)?;
        let sort_columns = order_columns
            .iter()
            .map(|c| {
                let descending = c.descending.unwrap_or(false);
                SortColumn {
                    values: batch.column_by_name(&c.name).unwrap().clone(),
                    options: Some(SortOptions {
                        descending,
                        nulls_first: false,
                    }),
                }
            })
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices(&sort_columns, None)?;
        let sorted_columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, Some(TakeOptions::default())))
            .collect::<Result<Vec<_>, _>>()?;
        let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)?;
        writer.write(&sorted_batch)?;
    } else {
        for batch in batches {
            writer.write(&batch)?;
        }
    }

    writer.close()?;

    Ok(())
}
