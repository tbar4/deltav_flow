use async_trait::async_trait;
use deltav_utils::{DeltavFlowResult, DeltavStream};
use std::collections::HashMap;

/// A data transform that consumes one or more named inputs and produces one named output.
#[async_trait]
pub trait Transform: Send + Sync {
    fn name(&self) -> &str;

    /// Named inputs this transform requires
    fn consumes(&self) -> Vec<&str>;

    /// Named output this transform produces
    fn produces(&self) -> &str;

    /// Transform input streams into one output stream
    async fn transform(
        &self,
        inputs: HashMap<String, DeltavStream>,
    ) -> DeltavFlowResult<DeltavStream>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    struct PassthroughTransform;

    #[async_trait]
    impl Transform for PassthroughTransform {
        fn name(&self) -> &str { "passthrough" }
        fn consumes(&self) -> Vec<&str> { vec!["input_data"] }
        fn produces(&self) -> &str { "output_data" }
        async fn transform(
            &self,
            inputs: HashMap<String, DeltavStream>,
        ) -> DeltavFlowResult<DeltavStream> {
            Ok(inputs.into_values().next().unwrap())
        }
    }

    #[tokio::test]
    async fn transform_trait_works() {
        let t = PassthroughTransform;
        assert_eq!(t.name(), "passthrough");
        assert_eq!(t.consumes(), vec!["input_data"]);
        assert_eq!(t.produces(), "output_data");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();
        let stream = DeltavStream::new(vec![batch]);

        let mut inputs = HashMap::new();
        inputs.insert("input_data".to_string(), stream);

        let result = t.transform(inputs).await.unwrap();
        assert_eq!(result.batches()[0].num_rows(), 3);
    }
}
