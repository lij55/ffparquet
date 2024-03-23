use std::fs::File;
use std::sync::Arc;

use clap::Parser;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::TypePtr;

use crate::cmd::utils::*;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    output: String,

    file: Vec<String>,
}

fn build_parquet_file_writer2(path_str: &str, schema: TypePtr) -> SerializedFileWriter<File> {

    let file = File::create(path_str).ok().unwrap();
    let props = WriterProperties::builder().build();
    let writer = SerializedFileWriter::new(file, schema, Arc::new(props))
        .ok()
        .unwrap();
    writer
}

pub fn merge_main(mut args: Args) -> eyre::Result<()> {
    //let props = Default::default();
    let mut writer = None;

    for file in args.file {
        let file = open_file(file)?;
        let reader = SerializedFileReader::new(file)?;

        if writer.is_none() {
            let schema = reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr()
                .root_schema_ptr();

            let props = WriterProperties::builder()
                // .set_max_row_group_size(max_rows_per_group)
                // .set_compression(Compression::ZSTD(ZstdLevel::default()))
                // .set_created_by("pp".into())
                // .set_statistics_enabled(EnabledStatistics::Chunk)
                // .set_sorting_columns(Option::from(vec![sorts]))
                .build();
            writer = Some(build_parquet_file_writer2(args.output.as_str(), schema));
        }

        let mut writer = writer.as_mut().unwrap();

        for rg_idx in 0..reader.num_row_groups() {
            let rg = reader.get_row_group(rg_idx)?;

            let mut row_group_writer = writer.next_row_group()?;

            for col in rg.get_column_reader()

            row_group_writer.close()?;
        }
    }

    Ok(())
}
