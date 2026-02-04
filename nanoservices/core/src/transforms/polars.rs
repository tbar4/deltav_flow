use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use deltav_utils::{DeltavFlowResult, DeltavStream};
use crate::transforms::traits::Transform;
use arrow::array::BooleanArray;
use arrow::compute;
use arrow::record_batch::RecordBatch;

/// Drop rows containing any null values.
/// Uses Arrow compute kernels directly (no Polars interop needed).
pub struct DropNulls {
    input_name: String,
    output_name: String,
}

impl DropNulls {
    pub fn new(input: impl Into<String>, output: impl Into<String>) -> Self {
        Self {
            input_name: input.into(),
            output_name: output.into(),
        }
    }
}

#[async_trait]
impl Transform for DropNulls {
    fn name(&self) -> &str { "drop_nulls" }
    fn consumes(&self) -> Vec<&str> { vec![&self.input_name] }
    fn produces(&self) -> &str { &self.output_name }

    async fn transform(
        &self,
        inputs: HashMap<String, DeltavStream>,
    ) -> DeltavFlowResult<DeltavStream> {
        let stream = inputs.into_values().next().unwrap();
        let batches = stream.into_batches();

        let mut result_batches = Vec::new();
        for batch in batches {
            // Build a boolean mask: true where ALL columns are non-null
            let num_rows = batch.num_rows();
            let mut mask = vec![true; num_rows];

            for col in batch.columns() {
                if let Some(nulls) = col.nulls() {
                    for i in 0..num_rows {
                        if nulls.is_null(i) {
                            mask[i] = false;
                        }
                    }
                }
            }

            let predicate = BooleanArray::from(mask);
            let filtered = compute::filter_record_batch(&batch, &predicate)?;
            result_batches.push(filtered);
        }

        Ok(DeltavStream::new(result_batches))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn drop_nulls_removes_null_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        ).unwrap();

        let stream = DeltavStream::new(vec![batch]);
        let mut inputs = HashMap::new();
        inputs.insert("raw".to_string(), stream);

        let t = DropNulls::new("raw", "clean");
        let result = t.transform(inputs).await.unwrap();

        // Should have removed the row with null name
        assert_eq!(result.batches()[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn drop_nulls_keeps_all_when_no_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();

        let stream = DeltavStream::new(vec![batch]);
        let mut inputs = HashMap::new();
        inputs.insert("raw".to_string(), stream);

        let t = DropNulls::new("raw", "clean");
        let result = t.transform(inputs).await.unwrap();

        assert_eq!(result.batches()[0].num_rows(), 3);
    }
}
