mod address_names;
mod automatic_projection_replay;
mod children;
mod cli;
mod commands;
mod execution;
mod healthcheck;
mod inspect;
mod manifest_drift;
mod name_current;
mod permissions;
mod primary_name;
mod projection_apply;
mod projection_json;
mod raw_facts;
mod record_inventory;
mod replay;
mod resolver;
mod runtime;

#[cfg(test)]
mod main_tests;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    runtime::init_tracing("bigname-worker", cli.writes_machine_json());
    commands::dispatch(cli.command).await
}
