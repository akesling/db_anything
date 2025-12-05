use std::any::Any;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

/// The haikus - each is [line1, line2, line3]
pub static HAIKUS: [[&str; 3]; 10] = [
    [
        "Logs, files, streams, and more—",
        "All become queryable.",
        "SQL for all.",
    ],
    ["Dusty CSV?", "Now a table you can join.", "Data alchemy."],
    ["Anything at all", "Can answer to a query—", "Just add SQL."],
    [
        "No schema needed.",
        "Point at chaos, ask questions.",
        "Order emerges.",
    ],
    [
        "Your filesystem",
        "Becomes rows and columns now.",
        "SELECT * FROM life.",
    ],
    [
        "Unstructured data",
        "Yearning to be understood—",
        "We give it a voice.",
    ],
    [
        "What was once opaque",
        "Now responds to your queries.",
        "Darkness becomes light.",
    ],
    [
        "Every file hides",
        "Tables waiting to be found.",
        "We set them all free.",
    ],
    [
        "Anything into",
        "A DB with one command.",
        "Power in your hands.",
    ],
    [
        "Raw bytes, scattered files—",
        "Through SQL's lens they shine.",
        "Meaning crystallized.",
    ],
];

static HAIKU_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut builder = SchemaBuilder::new();
    builder.push(Field::new("id", DataType::UInt32, false));
    builder.push(Field::new("line1", DataType::Utf8, false));
    builder.push(Field::new("line2", DataType::Utf8, false));
    builder.push(Field::new("line3", DataType::Utf8, false));
    Arc::new(builder.finish())
});

/// A TableProvider that exposes haikus as a queryable table
#[derive(Debug, Default)]
pub struct HaikuTableProvider {}

impl HaikuTableProvider {
    pub fn new() -> Self {
        Self {}
    }

    fn create_batch() -> Result<RecordBatch, DataFusionError> {
        let mut ids = Vec::with_capacity(HAIKUS.len());
        let mut line1s = Vec::with_capacity(HAIKUS.len());
        let mut line2s = Vec::with_capacity(HAIKUS.len());
        let mut line3s = Vec::with_capacity(HAIKUS.len());

        for (i, h) in HAIKUS.iter().enumerate() {
            ids.push(i as u32);
            line1s.push(h[0]);
            line2s.push(h[1]);
            line3s.push(h[2]);
        }

        RecordBatch::try_new(
            HAIKU_SCHEMA.clone(),
            vec![
                Arc::new(UInt32Array::from(ids)),
                Arc::new(StringArray::from(line1s)),
                Arc::new(StringArray::from(line2s)),
                Arc::new(StringArray::from(line3s)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

#[async_trait::async_trait]
impl TableProvider for HaikuTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        HAIKU_SCHEMA.clone()
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = Self::create_batch()?;
        let schema = if let Some(indices) = projection {
            if indices.is_empty() {
                self.schema()
            } else {
                Arc::new(self.schema().project(indices)?)
            }
        } else {
            self.schema()
        };

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?))
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
}

/// Scalar UDF that returns a random haiku
pub mod udf {
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use rand::seq::SliceRandom;
    use std::any::Any;
    use std::sync::Arc;

    use super::HAIKUS;

    #[derive(Debug)]
    pub struct RandomHaiku {
        signature: Signature,
    }

    impl Default for RandomHaiku {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RandomHaiku {
        pub fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }

        pub fn as_scalar_udf() -> ScalarUDF {
            ScalarUDF::from(Self::new())
        }
    }

    impl ScalarUDFImpl for RandomHaiku {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "random_haiku"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let num_rows = args.number_rows;
            let mut rng = rand::thread_rng();

            // Generate a random haiku for each row
            let haikus: Vec<String> = (0..num_rows)
                .map(|_| {
                    HAIKUS
                        .choose(&mut rng)
                        .map(|h| h.join("\n"))
                        .unwrap_or_default()
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(StringArray::from(haikus))))
        }
    }
}
