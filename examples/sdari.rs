use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow_csv::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;

#[derive(Parser, Debug)]
pub struct Args {
    output: String,

    #[arg(long)]
    col: Option<i32>,
}

fn main() {
    let bucket_name = "testdata";
    let region = Region::Custom {
        region: "".into(),
        endpoint: "http://localhost:9000".into(),
    };
    let credentials = Credentials::from_env().unwrap();
    let bucket = Bucket::new(bucket_name, region, credentials).unwrap().with_path_style();
    let mut path = "path";
    let result = bucket.list("".into(), None).unwrap();
    for i in result {
        println!("{i:?}")
    }
}

fn main2() {
    let args = Args::parse();
    // read csv from file
    let file = File::open("/dev/stdin").unwrap();

    let mut fields = vec![
        Field::new(
            "collect_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "create_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "update_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ];

    for i in 1..=args.col.unwrap_or(500) {
        fields.push(Field::new(
            format!("dp_{i:04}"),
            DataType::Decimal128(20, 10),
            false,
        ));
    }

    let csv_schema = Schema::new(fields);
    let mut reader = ReaderBuilder::new(Arc::new(csv_schema))
        .with_header(true)
        .build(file)
        .unwrap();

    let mut w = build_parquet_file_writer2(args.output.as_str(), reader.schema()).unwrap();
    loop {
        match reader.next() {
            Some(r) => match r {
                Ok(r) => {
                    w.write(&r);
                }
                Err(e) => {
                    println!("{e:?}");
                }
            },
            None => {
                // done
                break;
            }
        }
    }
    w.flush().unwrap();
    w.close();
    s3_upload();
}

fn build_parquet_file_writer2(path_str: &str, schema: SchemaRef) -> Option<ArrowWriter<File>> {
    let file = File::create(path_str).ok()?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(86400 / 4 / 6)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_created_by("op".into())
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .set_write_batch_size(16 * 1024 * 1024)
        .set_dictionary_enabled(false)
        .set_max_statistics_size(1024)
        .set_column_encoding(
            ColumnPath::from("collect_time"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_encoding(
            ColumnPath::from("create_time"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_encoding(
            ColumnPath::from("update_time"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .build();
    let writer = ArrowWriter::try_new(file, schema, Some(props)).ok()?;
    Some(writer)
}

fn s3_upload() {
    let bucket_name = "testdata";
    let region = Region::Custom {
        region: "".into(),
        endpoint: "http://localhost:9000".into(),
    };
    let credentials = Credentials::from_env().unwrap();
    let bucket = Bucket::new(bucket_name, region, credentials).unwrap().with_path_style();
    let mut path = "path";
    let test: Vec<u8> = (0..1000).map(|_| 42).collect();
    let mut file = open_file("test.parquet").unwrap();
    //file.write_all(&test).unwrap();
    //#[cfg(feature = "sync")]
    let status_code = bucket.put_object_stream(&mut file, "rust_s3_data_test.parquet");
    println!("{status_code:?}")
}

fn open_file<P: AsRef<Path>>(file_name: P) -> std::io::Result<File> {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    File::open(path)
}