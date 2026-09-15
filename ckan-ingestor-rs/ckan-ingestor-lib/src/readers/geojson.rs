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

use std::sync::Arc;

use anyhow::{bail, Result};
use arrow::{
    array::ArrayRef,
    datatypes::{Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_json::reader::{infer_json_schema_from_iterator, ReaderBuilder};
use geo_types::Geometry;
use geoarrow_array::{builder::GeometryBuilder, GeoArrowArray};
use geoarrow_schema::GeometryType;
use geojson::FeatureCollection;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use serde_json::Value;

use crate::parquet_output::ParquetOutput;

pub(super) fn write_geoparquet(batch: RecordBatch) -> Result<ParquetOutput> {
    let encoder = GeoParquetRecordBatchEncoder::try_new(
        batch.schema().as_ref(),
        &GeoParquetWriterOptions::default(),
    )?;
    let mut output = ParquetOutput::try_new_geoparquet(&batch, encoder)?;
    output.write(&batch)?;
    output.finish()?;
    Ok(output)
}

pub(super) fn geojson_to_record_batch(collection: FeatureCollection) -> Result<RecordBatch> {
    let geometry_type = GeometryType::default();
    let mut geometries = GeometryBuilder::new(geometry_type.clone());
    let mut properties = Vec::with_capacity(collection.features.len());

    for feature in collection.features {
        // Uma geometria por linha, incluindo nulos.
        match feature.geometry {
            Some(geometry) => {
                let geometry: Geometry<f64> = geometry.value.try_into()?;
                geometries.push_geometry(Some(&geometry))?;
            }
            None => geometries.push_null(),
        }

        // properties: null vira uma linha com todos os atributos nulos.
        let props = feature.properties.unwrap_or_default();

        // Evita colisão com o nome da coluna espacial.
        if props.contains_key("geometry") {
            bail!("Uma propriedade já se chama 'geometry'");
        }

        properties.push(Value::Object(props));
    }

    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    if !properties.is_empty() {
        // Examina todas as linhas para descobrir nomes e tipos.
        let schema = infer_json_schema_from_iterator(properties.iter().map(Ok::<_, ArrowError>))?;

        // Também permite coleções sem nenhum atributo.
        if !schema.fields().is_empty() {
            let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder()?;

            decoder.serialize(&properties)?;

            let batch = decoder
                .flush()?
                .ok_or_else(|| anyhow::anyhow!("Nenhum batch de propriedades foi produzido"))?;

            fields.extend(batch.schema().fields().iter().map(|f| f.as_ref().clone()));
            columns.extend(batch.columns().iter().cloned());
        }
    }

    // O Field preserva os metadados da extensão GeoArrow.
    fields.push(geometry_type.to_field("geometry", true));
    columns.push(geometries.finish().into_array_ref());

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}
