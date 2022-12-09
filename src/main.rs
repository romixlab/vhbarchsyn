mod util;
mod syncer_util;
mod archive;

use anyhow::{Context, Result};
use clap::Parser;
use path_clean::PathClean;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use toml;
use tracing::{Level};
use tracing_subscriber::FmtSubscriber;
use crate::archive::archive_local;
use crate::util::remove_trailing_slash;

#[derive(Deserialize)]
struct Config {
    #[serde(default = "default_date_format")]
    date_format: String,
    local_working_dir: PathBuf,
    local_archive: PathBuf,
    exclude: PathBuf
}

fn default_date_format() -> String {
    "%b%d_%Y_%H%M%S%z".to_owned()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    action: String,
    config: String,
}

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::TRACE).compact().finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args: Args = Args::parse();

    let config_path = PathBuf::from(args.config).clean();
    let input = fs::read_to_string(config_path.clone())
        .context(format!("unable to open {:?}", config_path))?;
    let mut config: Config = toml::from_str(input.as_str())?;

    // remove trailing slashes and add later only if needed
    remove_trailing_slash(&mut config.local_archive);
    remove_trailing_slash(&mut  config.local_working_dir);

    // let temp_dir = tempdir()?;
    // let exclude_filename = temp_dir.path().join("exclude.txt");
    // let mut exclude_file = File::create(exclude_filename.clone())?;
    // for exclude_pattern in &config.exclude {
    //     exclude_file.write_all(exclude_pattern.as_str().as_bytes())?;
    //     exclude_file.write_all("\n".as_bytes())?;
    // }
    // exclude_file.sync_data()?;

    if args.action == "archive" {
        archive_local(&config.local_working_dir, &config.local_archive, &config.exclude, config.date_format.as_str())?;
    }

    Ok(())
}
