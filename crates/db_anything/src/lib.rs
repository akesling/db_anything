use std::path;

use anyhow::{anyhow, bail};

pub mod engine;

/// Print a random project-related haiku
pub fn print_haiku(print_all: bool) -> anyhow::Result<()> {
    use providers::haiku::HAIKUS;
    use rand::seq::SliceRandom as _;

    if print_all {
        for h in HAIKUS {
            println!("{}", h.join(":"))
        }
    } else {
        let mut rng = rand::thread_rng();
        println!(
            "{}",
            HAIKUS
                .choose(&mut rng)
                .ok_or(anyhow!("at least one haiku"))?
                .join("\n")
        )
    }
    Ok(())
}

pub struct CmdOptions {
    /// The number of bytes the command memory pool should be limited to
    pub memory_limit_bytes: usize,
}

pub async fn run_cmd(
    options: &CmdOptions,
    sources: &[String],
    table_name: &str,
    sql: &str,
) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    if sources.is_empty() {
        bail!("No sources provided when running command")
    }

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::Core::new(options.memory_limit_bytes)?
        .add_direct_csv_table(table_name, sources)
        .await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}

pub async fn run_fs(
    options: &CmdOptions,
    path: &path::Path,
    table_name: &str,
    sql: &str,
) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::Core::new(options.memory_limit_bytes)?
        .add_fs_table(table_name, path)
        .await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}

pub async fn run_fs_table_func(options: &CmdOptions, sql: &str) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::Core::new(options.memory_limit_bytes)?
        .add_fs_table_func()
        .await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}
