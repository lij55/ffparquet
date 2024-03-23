// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// from https://raw.githubusercontent.com/apache/arrow-rs/master/parquet/src/bin/parquet-concat.rs

use std::fs::File;
use std::sync::Arc;

use clap::Parser;
use eyre::Report;
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;

#[derive(Debug, Parser)]
#[clap(author, version)]
/// Concatenates one or more parquet files
pub struct Args {
    /// Path to output
    #[clap(short, long)]
    output: String,

    #[clap(short, long, default_value_t = 4)]
    groups: u32,

    /// Path to input files
    input: String,
}

pub fn split_main(args: Args) -> eyre::Result<()> {
    if args.input.is_empty() {
        return Err(Report::from(ParquetError::General(
            "Must provide one input file".into(),
        )));
    }

    let reader = File::open(args.input).unwrap();
    let metadata = parquet::file::footer::parse_metadata(&reader).unwrap();

    let props = Arc::new(WriterProperties::builder().build());
    let schema = metadata.file_metadata().schema_descr().root_schema_ptr();

    let mut output_idx = 0;
    let mut left = metadata.row_groups().len() as u32;
    let mut rg_iter = metadata.row_groups().into_iter();

    while left > 0 {
        let output = format!("{}_{:04}.parquet", args.output, output_idx);
        output_idx += 1;

        let output = File::create(output)?;
        let mut writer = SerializedFileWriter::new(output, schema.clone(), props.clone())?;

        for _ in 0..args.groups {
            match rg_iter.next() {
                Some(rg) => {
                    left -= 1;
                    let mut rg_out = writer.next_row_group()?;
                    for column in rg.columns() {
                        let result = ColumnCloseResult {
                            bytes_written: column.compressed_size() as _,
                            rows_written: rg.num_rows() as _,
                            metadata: column.clone(),
                            bloom_filter: None,
                            column_index: None,
                            offset_index: None,
                        };
                        rg_out.append_column(&reader, result)?;
                    }
                    rg_out.close()?;
                }
                None => {
                    break;
                }
            }
        }
        writer.close()?;
    }

    Ok(())
}
