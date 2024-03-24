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
/// merge parquet files by row groups
pub struct Args {
    /// Path to output
    output: String,

    /// Path to input files
    input: Vec<String>,
}

pub fn merge_main(args: Args) -> eyre::Result<()> {
    if args.input.is_empty() {
        return Err(Report::from(ParquetError::General(
            "Must provide at least one input file".into(),
        )));
    }

    let output = File::create(&args.output)?;

    let inputs = args
        .input
        .iter()
        .map(|x| {
            let reader = File::open(x).unwrap();
            let metadata = parquet::file::footer::parse_metadata(&reader).unwrap();
            (reader, metadata)
        })
        .collect::<Vec<_>>();

    let expected = inputs[0].1.file_metadata().schema();
    for (_, metadata) in inputs.iter().skip(1) {
        let actual = metadata.file_metadata().schema();
        if expected != actual {
            return Err(Report::from(ParquetError::General(format!(
                "inputs must have the same schema, {expected:#?} vs {actual:#?}"
            ))));
        }
    }

    let props = Arc::new(WriterProperties::builder().build());
    let schema = inputs[0].1.file_metadata().schema_descr().root_schema_ptr();
    let mut writer = SerializedFileWriter::new(output, schema, props)?;

    for (input, metadata) in inputs {
        for rg in metadata.row_groups() {
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
                rg_out.append_column(&input, result)?;
            }
            rg_out.close()?;
        }
    }

    writer.close()?;

    Ok(())
}
