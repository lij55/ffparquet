use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::format::SortingColumn;
use parquet::schema::types::{SchemaDescriptor, Type};
use std::fs::{read, File};
use std::io;
use std::path::Path;
use std::time::Duration;

use clap::Parser;
use std::fmt::Display;

use std::ops::Deref;
use std::{sync::mpsc, sync::Arc, sync::Mutex, thread};

use env_logger;
use log::{debug, error, info, warn};
use std::env;
const ERR_LOCK: &str = "cant acquire resource";

fn build_parquet_file_writer2(path_str: &str, schema: SchemaRef) -> Option<ArrowWriter<File>> {
    let file = File::create(path_str).ok()?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(1024 * 1024)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_created_by("ffparquet".into())
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let writer = ArrowWriter::try_new(file, schema, Some(props)).ok()?;
    Some(writer)
}

fn open_file<P: AsRef<Path>>(file_name: P) -> File {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    let file = File::open(path);

    file.unwrap()
}

/*
1个reader，message发给线程池
根据总行数，计算文件总个数
参数确定线程个数
 */

#[derive(clap::ValueEnum, PartialEq, Default, Clone, Debug)]
enum CompressionType {
    Uncompressed,
    #[default]
    Zstd,
    Snapy,
    Gzip,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        match *self {
            CompressionType::Uncompressed => write!(f, "uncompressed"),
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Snapy => write!(f, "snapy"),
            CompressionType::Gzip => write!(f, "gzip"),
        }
    }
}

#[derive(Parser)]
struct Args {
    /// Max row groups to create
    #[arg(short = 'g', long = "groups", default_value_t = 0)]
    max_row_group_count: u16,

    /// Size of each row group
    #[arg(short = 's', long = "size", default_value_t = 100000)]
    max_row_group_size: u32,

    /// jobs in parallel
    #[arg(short = 'j', long = "jobs", default_value_t = 4)]
    jobs: u8,

    /// output directoy, default is PWD
    #[arg(short = 'd', long = "dir")]
    output_dir: Option<String>,

    /// prefix
    output: Option<String>,

    /// compression method
    #[arg(short='c', long="compression", default_value_t = CompressionType::Zstd)]
    compression: CompressionType,

    /// input parquet file path
    #[arg(short = 'i', long = "input")]
    file: String,
}

enum Message {
    DONE,
    CONTENT(Box<RecordBatch>),
}

fn main() {
    env_logger::init(); // controlled by env var RUST_LOG
    let args = Args::parse();
    info!("hello, log");

    if args.output.is_none() {
        dump_parquet_info(args.file.as_str());
        return;
    }

    let target_prefix = args.output.unwrap();

    let file = File::open(args.file).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut total_rows = builder.metadata().file_metadata().num_rows() as u64;
    let schema_desc = builder.schema().clone();

    let mut reader = builder.with_batch_size(10000).build().unwrap();

    let row_limit_per_file: u64 =
        (args.max_row_group_count as u32 * args.max_row_group_size) as u64;
    let total_tasks: u64 = match row_limit_per_file {
        0 => {
            if total_rows < 10000 {
                1
            } else {
                args.jobs as u64
            }
        }
        _ => total_rows / row_limit_per_file + 1,
    };

    //let schema_desc = builder.schema().clone();
    let (tx, rx) = mpsc::sync_channel::<Message>(16);

    let (thread_tx, thread_rx) = mpsc::channel::<i8>();

    for i in 0..args.jobs {
        thread_tx.send(0);
    }

    let mut threads = vec![];

    let rxtf = Arc::new(Mutex::new(rx));

    thread::spawn(move || {
        while let Some(record_batch) = reader.next() {
            if record_batch.is_err() {
                break;
            }

            let data = Box::new(record_batch.unwrap());
            tx.send(Message::CONTENT(data)).unwrap();
            // w1.write(&data).expect("Writing batch");
            // w2.write(&data).expect("Writing batch");
        }
        for i in 0..total_tasks {
            tx.send(Message::DONE).unwrap();
        }

        //println!("send data done");
    });

    for i in 0..total_tasks {
        let _ = thread_rx.recv();
        let rx = rxtf.clone();
        let schema = schema_desc.clone();
        let local_thread_tx = thread_tx.clone();
        let outfile_name = format!("{target_prefix}_{i}.parquet");

        let t = thread::spawn(move || {
            let mut w1 = build_parquet_file_writer2(outfile_name.as_str(), schema).unwrap();

            loop {
                let msg = rx
                    .lock()
                    .expect(ERR_LOCK)
                    .recv_timeout(Duration::from_secs(10));

                match msg {
                    Ok(msg_data) => match msg_data {
                        Message::CONTENT(data) => {
                            //println!("get data from {i}");
                            w1.write(data.as_ref());
                        }
                        Message::DONE => {
                            println!("thread for part {i} done");
                            break;
                        }
                    },
                    Err(_) => break,
                }
            }
            w1.flush();

            // writer must be closed to write footer
            w1.close().unwrap();
            local_thread_tx.send(0);
        });
        threads.push(t);
    }

    while let Some(cur_thread) = threads.pop() {
        cur_thread.join().unwrap();
    }
}

fn dump_parquet_info(file: &str) {
    let file = File::open(file).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let c = parquet_reader.num_row_groups();
    let file_meta = parquet_reader.metadata().file_metadata();

    println!(
        "total number of columns: {}",
        file_meta.schema_descr().num_columns()
    );

    let columns = file_meta.schema_descr().columns();
    for i in 0..columns.len() {
        println!(
            "\t{}: {:?}",
            columns[i].name(),
            //columns[i].converted_type()
            columns[i].logical_type()
        );
    }

    println!("number of row groups: {}", c);
    for i in 0..c {
        let reader = parquet_reader.get_row_group(i).unwrap();
        let rg_metadata = reader.metadata();
        println!("\tRow group {i} has {} rows", rg_metadata.num_rows());
        for i in 0..rg_metadata.columns().len() {
            println!(
                "\t\tcolumn {i}: {} => {} by {}, encoding: {:?}",
                rg_metadata.column(i).uncompressed_size(),
                rg_metadata.column(i).compressed_size(),
                rg_metadata.column(i).compression(),
                rg_metadata.column(i).encodings()
            );
        }
    }

    println!("total number of rows: {}", file_meta.num_rows());

    //
}
