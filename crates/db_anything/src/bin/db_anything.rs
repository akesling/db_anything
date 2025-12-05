use std::collections::BTreeMap;
use std::path;

use anyhow::{bail, Context as _, Result};
use clap::Parser;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// Log level pairs of the form <MODULE>:<LEVEL>.
    #[arg(long)]
    log_levels: Option<Vec<String>>,

    /// Max number of bytes available for memory pool, defaults to 10GiB
    #[arg(long, default_value_t = 10737418240)]
    memory_pool_bytes: usize,
}

struct GlobalOptions {
    memory_pool_bytes: usize,
}

#[derive(clap::Parser, Debug)]
struct HaikuOptions {
    /// Print all haikus
    #[arg(long)]
    all: bool,
}

async fn haiku(_context: &GlobalOptions, options: &HaikuOptions) -> anyhow::Result<()> {
    db_anything::print_haiku(options.all)
}

#[derive(clap::Parser, Debug)]
struct FsExecOptions {
    /// Source CSV files for command
    #[arg(long)]
    path: path::PathBuf,

    // TODO(alex): Allow arbitrarily binding csv files to tables so we can have multiple tables
    // surfaced.
    /// Name for virtual table
    #[arg(long, default_value = "tbl")]
    table_name: String,

    /// The query to execute on the virtual table `tbl`
    #[arg()]
    query: String,
}

#[derive(clap::Parser, Debug)]
struct FsTableFuncOptions {
    /// The query to execute on the virtual table `tbl`
    #[arg()]
    query: String,
}

#[derive(clap::Parser, Debug)]
struct ExecOptions {
    /// Source CSV files for command
    #[arg(long)]
    csv: Vec<String>,

    // TODO(alex): Allow arbitrarily binding csv files to tables so we can have multiple tables
    // surfaced.
    /// Name for virtual table
    #[arg(long, default_value = "tbl")]
    table_name: String,

    /// The query to execute on the virtual table `tbl`
    #[arg()]
    query: String,
}

async fn exec(context: &GlobalOptions, options: &ExecOptions) -> anyhow::Result<()> {
    db_anything::run_cmd(
        &db_anything::CmdOptions {
            memory_limit_bytes: context.memory_pool_bytes,
        },
        &options.csv,
        &options.table_name,
        &options.query,
    )
    .await
}

async fn fs_exec(context: &GlobalOptions, options: &FsExecOptions) -> anyhow::Result<()> {
    db_anything::run_fs(
        &db_anything::CmdOptions {
            memory_limit_bytes: context.memory_pool_bytes,
        },
        &options.path,
        &options.table_name,
        &options.query,
    )
    .await
}

async fn fs_table_func(
    context: &GlobalOptions,
    options: &FsTableFuncOptions,
) -> anyhow::Result<()> {
    db_anything::run_fs_table_func(
        &db_anything::CmdOptions {
            memory_limit_bytes: context.memory_pool_bytes,
        },
        &options.query,
    )
    .await
}

#[derive(clap::Parser, Debug)]
struct ServePgOptions {
    /// Source CSV files for server
    #[arg(long)]
    csv: Vec<String>,

    // TODO(alex): Allow arbitrarily binding csv files to tables so we can have multiple tables
    // surfaced.
    /// Name for virtual table
    #[arg(long, default_value = "tbl")]
    table_name: String,

    /// Serving address
    #[arg(default_value = "127.0.0.1:5432")]
    address: String,
}

async fn serve_pg(context: &GlobalOptions, options: &ServePgOptions) -> Result<()> {
    if options.csv.is_empty() {
        bail!("No sources provided when running command")
    }

    let mut engine = db_anything::engine::Core::new(context.memory_pool_bytes)?
        .add_direct_csv_table(&options.table_name, &options.csv)
        .await?;
    let join_handle = engine.serve_pg(&options.address).await?;
    join_handle.await?
}

/// Convert a series of <MODULE>:<LEVEL> pairs into actionable `(module, LevelFilter)` pairs
fn as_level_pairs(config: &[String]) -> Result<Vec<(&str, simplelog::LevelFilter)>> {
    let mut pairs = Vec::with_capacity(config.len());
    for c in config {
        let tokens: Vec<&str> = c.split(":").collect();
        if tokens.len() != 2 {
            bail!("Flag config pair was not of the form <MODULE>:<LEVEL>: '{c}'");
        }
        pairs.push((
            tokens[0],
            match tokens[1].to_lowercase().as_str() {
                "trace" => simplelog::LevelFilter::Trace,
                "debug" => simplelog::LevelFilter::Debug,
                "info" => simplelog::LevelFilter::Info,
                "warn" => simplelog::LevelFilter::Warn,
                "error" => simplelog::LevelFilter::Error,
                _ => bail!("Unrecognized level name in '{c}'"),
            },
        ))
    }

    Ok(pairs)
}

fn initialize_logging(
    module_path_filters: &[(&str, simplelog::LevelFilter)],
) -> anyhow::Result<()> {
    simplelog::CombinedLogger::init(
        module_path_filters
            .iter()
            .map(|(module_path_filter, level)| {
                simplelog::TermLogger::new(
                    *level,
                    simplelog::ConfigBuilder::new()
                        .add_filter_allow(module_path_filter.to_string())
                        .build(),
                    simplelog::TerminalMode::Mixed,
                    simplelog::ColorChoice::Auto,
                ) as Box<dyn simplelog::SharedLogger>
            })
            .collect(),
    )
    .map_err(|e| e.into())
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Print a random haiku
    Haiku(HaikuOptions),
    /// Execute SQL over CSV files
    Exec(ExecOptions),
    /// Execute SQL over a filesystem location
    FsExec(FsExecOptions),
    /// Execute SQL with a fs() function for filesystem ops
    FsTableFunc(FsTableFuncOptions),
    /// Serve PostgreSQL wire protocol server over CSV files
    ServePg(ServePgOptions),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let levels_arg: Option<Vec<String>> = args.log_levels;

    let log_levels: Vec<(&str, simplelog::LevelFilter)> = {
        let mut log_levels = BTreeMap::from([
            // ("db_anything", simplelog::LevelFilter::Debug),
            // If compiled via Bazel, "db_anything" binary crate module will be named "bin"
            //("bin", simplelog::LevelFilter::Debug),
        ]);

        for (module, level) in levels_arg
            .as_deref()
            .map(as_level_pairs)
            .unwrap_or(Ok(vec![]))
            .context("Log level override parsing failed")?
        {
            log_levels.insert(module, level);
        }
        log_levels.into_iter().collect()
    };
    let _ = initialize_logging(&log_levels[..]);
    log::trace!("Logging initialized, commands parsed...");

    let context = GlobalOptions {
        memory_pool_bytes: args.memory_pool_bytes,
    };
    match args.command {
        Command::Haiku(options) => haiku(&context, &options).await?,
        Command::Exec(options) => exec(&context, &options).await?,
        Command::FsExec(options) => fs_exec(&context, &options).await?,
        Command::FsTableFunc(options) => fs_table_func(&context, &options).await?,
        Command::ServePg(options) => serve_pg(&context, &options).await?,
    }
    Ok(())
}
