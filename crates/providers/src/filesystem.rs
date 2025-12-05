use std::any::Any;
use std::path;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use datafusion::arrow::array::RecordBatch;

use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{DisplayAs, PlanProperties};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

/// A table function that returns a table provider with the value as a single column
#[derive(Default, Debug)]
pub struct FilesystemListingFunction {}

impl TableFunctionImpl for FilesystemListingFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)))) = exprs.first() else {
            return datafusion::common::plan_err!("First argument must be a filesytem path string");
        };

        Ok(Arc::new(FilesystemTableProvider::new(path)))
    }
}

static LISTING_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut builder = SchemaBuilder::new();

    // Other things to add?
    // * Permissions
    // * owner/group

    // From std::fs::DirEntry
    builder.push(Field::new("path", DataType::Utf8, true));

    // From std::fs::Metadata
    builder.push(Field::new("is_dir", DataType::Boolean, true));
    builder.push(Field::new("is_file", DataType::Boolean, true));
    builder.push(Field::new("is_symlink", DataType::Boolean, true));
    builder.push(Field::new("size", DataType::UInt64, true));
    builder.push(Field::new(
        "created",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    ));
    builder.push(Field::new(
        "modified",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    ));
    builder.push(Field::new(
        "accessed",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    ));
    builder.push(Field::new("permissions", DataType::Utf8, true));
    builder.push(Field::new("contents", DataType::LargeBinary, true));

    Arc::new(builder.finish())
});

/// A TableProvider that provides access to a filesystem at a given root
#[derive(Debug)]
pub struct FilesystemTableProvider {
    root: path::PathBuf,
    schema: SchemaRef,
}

impl FilesystemTableProvider {
    pub fn new(root: impl Into<path::PathBuf>) -> Self {
        Self {
            root: root.into(),
            schema: LISTING_SCHEMA.clone(),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for FilesystemTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = if let Some(indices) = projection {
            if indices.is_empty() {
                self.schema.clone()
            } else {
                Arc::new(self.schema.project(indices)?)
            }
        } else {
            self.schema.clone()
        };

        return Ok(Arc::new(FilesystemExec::new(&self.root, schema)));
    }

    #[doc = "Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
}

#[derive(Clone, Debug)]
struct FilesystemExec {
    root: path::PathBuf,
    schema: SchemaRef,
    prop_cache: datafusion::physical_plan::PlanProperties,
}

impl FilesystemExec {
    pub fn new(root: impl Into<path::PathBuf>, schema: SchemaRef) -> Self {
        FilesystemExec {
            root: root.into(),
            schema: schema.clone(),
            prop_cache: Self::compute_properties(schema, 1),
        }
    }

    fn compute_properties(schema: SchemaRef, n_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(n_partitions),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl DisplayAs for FilesystemExec {
    fn fmt_as(
        &self,
        format_type: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        use datafusion::physical_plan::DisplayFormatType;
        match format_type {
            DisplayFormatType::Default => {
                write!(f, "{}", self.name())
            }
            DisplayFormatType::Verbose => {
                write!(f, "{}: root={}", self.name(), self.root.display())
            }
        }
    }
}

impl ExecutionPlan for FilesystemExec {
    fn name(&self) -> &'static str {
        "FilesystemExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.prop_cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(format!(
                "FilesystemExec does not support children, attempted to add {}",
                children.len()
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, DataFusionError> {
        use futures::StreamExt as _;

        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                concat!(
                    "Partition '{}' requested, but only one partition ",
                    "supported for filesystem listings"
                ),
                partition
            )));
        }

        let root = self.root.clone();
        let schema_ref = self.schema.clone();
        let s = futures::stream::once(async move {
            let listing: Vec<tokio::fs::DirEntry> =
                tokio_stream::wrappers::ReadDirStream::new(tokio::fs::read_dir(&root).await?)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<std::io::Result<_>>()
                    .map_err(|error| {
                        DataFusionError::Internal(format!(
                            "Error encountered while listing directory '{}': {}",
                            root.display(),
                            error
                        ))
                    })?;
            let metadatas = {
                let mut tmp = Vec::with_capacity(listing.len());
                for entry in &listing {
                    tmp.push(entry.metadata().await.map(Some).unwrap_or(None))
                }
                tmp
            };

            let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![];
            for f in &schema_ref.fields {
                match f.name().as_str() {
                    "path" => columns.push(Arc::new(arrow::array::StringArray::from_iter(
                        listing
                            .iter()
                            .map(|entry| Some(entry.path().display().to_string())),
                    ))),
                    "is_dir" => columns.push(Arc::new(arrow::array::BooleanArray::from_iter(
                        metadatas
                            .iter()
                            .map(|meta| meta.as_ref().map(|m| m.is_dir())),
                    ))),
                    "is_file" => columns.push(Arc::new(arrow::array::BooleanArray::from_iter(
                        metadatas
                            .iter()
                            .map(|meta| meta.as_ref().map(|m| m.is_file())),
                    ))),
                    "is_symlink" => columns.push(Arc::new(arrow::array::BooleanArray::from_iter(
                        metadatas
                            .iter()
                            .map(|meta| meta.as_ref().map(|m| m.is_symlink())),
                    ))),
                    "size" => columns.push(Arc::new(arrow::array::UInt64Array::from_iter(
                        metadatas.iter().map(|meta| meta.as_ref().map(|m| m.len())),
                    ))),
                    "created" => {
                        columns.push(Arc::new(arrow::array::TimestampSecondArray::from_iter(
                            metadatas.iter().map(|meta| {
                                meta.as_ref().map(|m| {
                                    let Ok(time) = m.created() else { return 0 };
                                    let Ok(dur) = time.duration_since(SystemTime::UNIX_EPOCH)
                                    else {
                                        return 0;
                                    };

                                    dur.as_secs() as i64
                                })
                            }),
                        )))
                    }
                    "modified" => {
                        columns.push(Arc::new(arrow::array::TimestampSecondArray::from_iter(
                            metadatas.iter().map(|meta| {
                                meta.as_ref().map(|m| {
                                    let Ok(time) = m.modified() else { return 0 };
                                    let Ok(dur) = time.duration_since(SystemTime::UNIX_EPOCH)
                                    else {
                                        return 0;
                                    };

                                    dur.as_secs() as i64
                                })
                            }),
                        )))
                    }
                    "accessed" => {
                        columns.push(Arc::new(arrow::array::TimestampSecondArray::from_iter(
                            metadatas.iter().map(|meta| {
                                meta.as_ref().map(|m| {
                                    let Ok(time) = m.accessed() else { return 0 };
                                    let Ok(dur) = time.duration_since(SystemTime::UNIX_EPOCH)
                                    else {
                                        return 0;
                                    };

                                    dur.as_secs() as i64
                                })
                            }),
                        )))
                    }
                    "permissions" => columns.push(Arc::new(arrow::array::StringArray::from_iter(
                        metadatas.iter().map(|meta| {
                            meta.as_ref().map(|m| {
                                use std::os::unix::fs::PermissionsExt as _;
                                symbolic_permissions(m.permissions().mode())
                            })
                        }),
                    ))),
                    "contents" => {
                        let mut contents: Vec<Vec<u8>> = Vec::with_capacity(listing.len());
                        for (entry, meta) in listing.iter().zip(metadatas.iter()) {
                            if let Some(meta) = meta {
                                if meta.is_dir() {
                                    contents.push(vec![]);
                                    continue;
                                }
                            } else {
                                contents.push(vec![]);
                                continue;
                            }

                            contents.push(tokio::fs::read(&entry.path()).await.unwrap_or(vec![]))
                        }
                        columns.push(Arc::new(arrow::array::LargeBinaryArray::from_vec(
                            contents.iter().map(Vec::as_slice).collect(),
                        )))
                    }
                    name => Err(DataFusionError::Internal(format!(
                        "Unrecognized field {name}"
                    )))?,
                }
            }

            let batch = RecordBatch::try_new(schema_ref, columns)?;
            Ok(batch)
        });
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.schema.clone(),
                s,
            ),
        ))
    }
}

// TODO(akesling): Actually digest this function wholly wrought from the bowels of ChatGPT and fix
// what dragons lurk within....
/// Convert a `u32` (Unix mode bits) into a symbolic "rwxr-xr-x" style string.
fn symbolic_permissions(mode: u32) -> String {
    // Handle special bits (setuid, setgid, sticky)
    let setuid = mode & 0o4000 != 0;
    let setgid = mode & 0o2000 != 0;
    let sticky = mode & 0o1000 != 0;

    // Extract bits for user/group/other:
    let ur = if mode & 0o400 != 0 { 'r' } else { '-' };
    let uw = if mode & 0o200 != 0 { 'w' } else { '-' };
    let ux = if mode & 0o100 != 0 { 'x' } else { '-' };

    let gr = if mode & 0o040 != 0 { 'r' } else { '-' };
    let gw = if mode & 0o020 != 0 { 'w' } else { '-' };
    let gx = if mode & 0o010 != 0 { 'x' } else { '-' };

    let or = if mode & 0o004 != 0 { 'r' } else { '-' };
    let ow = if mode & 0o002 != 0 { 'w' } else { '-' };
    let ox = if mode & 0o001 != 0 { 'x' } else { '-' };

    // Adjust x bits according to setuid, setgid, and sticky bits:
    let ux = match (ux, setuid) {
        ('x', true) => 's', // setuid + user-exec
        ('-', true) => 'S', // setuid without user-exec
        (c, false) => c,
        _ => todo!(),
    };
    let gx = match (gx, setgid) {
        ('x', true) => 's', // setgid + group-exec
        ('-', true) => 'S',
        (c, false) => c,
        _ => todo!(),
    };
    let ox = match (ox, sticky) {
        ('x', true) => 't', // sticky + other-exec
        ('-', true) => 'T',
        (c, false) => c,
        _ => todo!(),
    };

    // Combine into one string
    format!("{}{}{}{}{}{}{}{}{}", ur, uw, ux, gr, gw, gx, or, ow, ox)
}
