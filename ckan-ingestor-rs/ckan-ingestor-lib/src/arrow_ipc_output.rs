use std::{
    cmp::max,
    fs::File,
    path::{Path, PathBuf},
};

use arrow::{array::RecordBatch, error::ArrowError};
use arrow_ipc::writer::FileWriter;

const PREVIEW_MAX_VALUE_LEN: usize = 1000;
const PREVIEW_MAX_ROWS: usize = 5;

pub struct ArrowIpcOutput {
    path: PathBuf,
    pub writer: FileWriter<File>,
    pub rows: usize,
    pub columns: usize,
    pub preview: Option<Vec<serde_json::Value>>,
}

impl ArrowIpcOutput {
    pub fn try_new(batch: &RecordBatch) -> Result<Self, ArrowError> {
        let path = std::env::temp_dir().join(format!("{}.arrow", uuid::Uuid::new_v4()));
        let file = File::create(&path)?;
        let writer = FileWriter::try_new(file, &batch.schema())?;
        Ok(Self {
            path,
            writer,
            rows: 0,
            columns: 0,
            preview: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.writer.write(batch)?;
        if self.preview.is_none() {
            self.preview = Some(Self::generate_preview(batch));
        }
        self.rows += batch.num_rows();
        self.columns += max(batch.num_columns(), self.columns);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), ArrowError> {
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
                    let s = duckdb::arrow::util::display::array_value_to_string(
                        batch.column(col_idx),
                        row_idx,
                    )
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

impl Drop for ArrowIpcOutput {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
