use std::path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use datafusion::{execution::SendableRecordBatchStream, prelude::SessionContext};
use itertools::Itertools as _;
use url::Url;

pub struct Core {
    context: SessionContext,
}

impl Core {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(memory_limit_bytes: usize) -> anyhow::Result<Core> {
        use datafusion::prelude::*;

        let session_config = SessionConfig::new().with_information_schema(true);
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(memory_limit_bytes),
            ))
            .build_arc()?;

        let context = SessionContext::new_with_config_rt(session_config, runtime_env);
        Ok(Core { context })
    }

    pub async fn add_direct_csv_table(
        self,
        name: &str,
        sources: &[String],
    ) -> anyhow::Result<Core> {
        if sources.is_empty() {
            bail!("No table sources provided");
        }

        let csv_format = datafusion::datasource::file_format::csv::CsvFormat::default();
        let listing_options =
            datafusion::datasource::listing::ListingOptions::new(Arc::new(csv_format))
                .with_file_extension(".csv");

        let table_paths: Vec<_> = sources
            .iter()
            .map(datafusion::datasource::listing::ListingTableUrl::parse)
            .collect::<Result<_, _>>()
            .map_err(|err| anyhow!("{err}"))?;

        // This is a bit of a hack where we create a new object store for each recognized HTTP URL
        for store_url in table_paths
            .iter()
            .map(datafusion::datasource::listing::ListingTableUrl::object_store)
            .unique()
            .filter(|s_url| s_url.as_str().starts_with("http"))
        {
            let store_url = store_url.as_str();
            let http_store = Arc::new(
                object_store::http::HttpBuilder::new()
                    .with_url(store_url)
                    .build()
                    .context("HTTP Store failed to build")?,
            );
            self.context.runtime_env().register_object_store(
                &Url::parse(store_url).context("Failed to parse object store base URL")?,
                http_store,
            );
        }

        let resolved_schema = listing_options
            .infer_schema(&self.context.state(), &table_paths[0])
            .await?;
        let config =
            datafusion::datasource::listing::ListingTableConfig::new_with_multi_paths(table_paths)
                .with_listing_options(listing_options)
                .with_schema(resolved_schema);
        let listing_table = datafusion::datasource::listing::ListingTable::try_new(config)?;

        self.context.register_table(name, Arc::new(listing_table))?;

        Ok(self)
    }

    pub async fn add_fs_table(self, name: &str, path: &path::Path) -> anyhow::Result<Core> {
        let fs_table = providers::filesystem::FilesystemTableProvider::new(path);
        self.context.register_table(name, Arc::new(fs_table))?;
        Ok(self)
    }

    pub async fn add_fs_table_func(self) -> anyhow::Result<Core> {
        self.context.register_udtf(
            "fs",
            Arc::new(providers::filesystem::FilesystemListingFunction {}),
        );
        Ok(self)
    }

    pub async fn execute(&mut self, query: &str) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self.context.sql(query).await?.execute_stream().await?)
    }

    pub async fn serve_pg(
        &mut self,
        serve_address: &str,
        enable_fs: bool,
    ) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
        log::debug!("Starting up a new server");

        // Register haiku table and UDF
        self.context.register_table(
            "haikus",
            Arc::new(providers::haiku::HaikuTableProvider::new()),
        )?;
        self.context
            .register_udf(providers::haiku::udf::RandomHaiku::as_scalar_udf());

        // Register fs() table function if enabled
        if enable_fs {
            self.context.register_udtf(
                "fs",
                Arc::new(providers::filesystem::FilesystemListingFunction {}),
            );
        }

        let listener = tokio::net::TcpListener::bind(serve_address)
            .await
            .context(format!("run_server failed to bind to {serve_address}"))?;

        log::debug!("Listening to {}", serve_address);

        let factory = Arc::new(datafusion_postgres::HandlerFactory(Arc::new(
            // TODO(akesling): wrap Engine instead of using DfSessionService over the context.
            // This will give us more control and provide a better stack of abstractions.
            datafusion_postgres::DfSessionService::new(self.context.clone()),
        )));

        Ok(tokio::spawn(async move {
            loop {
                let incoming_socket = listener.accept().await.context(
                    "run_server listener failed when attempting to open incoming socket",
                )?;
                let factory_ref = factory.clone();

                tokio::spawn(async move {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    log::debug!(
                        "Starting new session for incoming socket connection. (time={now})"
                    );
                    let result =
                        pgwire::tokio::process_socket(incoming_socket.0, None, factory_ref).await;
                    log::debug!("No longer listening on socket (start-time={now})");
                    result
                });
            }
        }))
    }
}
