// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use parquet::{arrow::ArrowWriter, errors::ParquetError};

const PREVIEW_MAX_VALUE_LEN: usize = 1000;
const PREVIEW_MAX_ROWS: usize = 5;

pub struct ParquetOutput {
    path: PathBuf,
    pub schema: SchemaRef,
    pub writer: ArrowWriter<File>,
    pub rows: usize,
    pub columns: usize,
    pub preview: Option<Vec<serde_json::Value>>,
}

impl ParquetOutput {
    pub fn try_new(batch: &RecordBatch) -> Result<Self, ParquetError> {
        let path = std::env::temp_dir().join(format!("{}.parquet", uuid::Uuid::new_v4()));
        let file = File::create(&path)?;
        let writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        Ok(Self {
            path,
            schema: batch.schema(),
            writer,
            rows: 0,
            columns: 0,
            preview: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.writer.write(batch)?;
        if self.preview.is_none() {
            self.preview = Some(Self::generate_preview(batch));
        }
        self.rows += batch.num_rows();
        self.columns = self.columns.max(batch.num_columns());
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), ParquetError> {
        self.writer.finish()?;
        Ok(())
    }

    fn generate_preview(batch: &RecordBatch) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        for row_idx in 0..batch.num_rows().min(PREVIEW_MAX_ROWS) {
            let mut row_map = serde_json::Map::new();
            let schema = batch.schema();
            for col_idx in 0..batch.num_columns() {
                let name = schema.field(col_idx).name();
                let val = if batch.column(col_idx).is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    let s =
                        arrow::util::display::array_value_to_string(batch.column(col_idx), row_idx)
                            .unwrap_or_default();
                    Self::truncate_preview_value(serde_json::Value::String(s))
                };
                row_map.insert(name.clone(), val);
            }
            rows.push(serde_json::Value::Object(row_map));
        }
        rows
    }

    fn truncate_preview_value(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::String(s) if s.len() > PREVIEW_MAX_VALUE_LEN => {
                serde_json::Value::String(format!(
                    "[TRUNCATED: value too large for preview ({} bytes)]",
                    s.len()
                ))
            }
            other => other,
        }
    }
}

impl Drop for ParquetOutput {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::reader::{FileReader as _, SerializedFileReader};
    use std::sync::Arc;

    #[test]
    fn columns_count_is_stable_when_writing_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
            .expect("valid record batch");
        let mut output = ParquetOutput::try_new(&batch).expect("valid Parquet output");

        output.write(&batch).expect("first batch should write");
        output.write(&batch).expect("second batch should write");

        assert_eq!(output.rows, 2);
        assert_eq!(output.columns, 1);
    }

    #[test]
    fn output_is_a_valid_parquet_file() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
            .expect("valid record batch");
        let mut output = ParquetOutput::try_new(&batch).expect("valid output");
        output.write(&batch).expect("write batch");
        output.finish().expect("finish output");

        let reader = SerializedFileReader::new(File::open(output.path()).unwrap());

        assert!(reader.is_ok(), "reader output must be Parquet");
        assert_eq!(reader.unwrap().metadata().file_metadata().num_rows(), 1);
    }
}
