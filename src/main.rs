mod util;
mod syncer_util;

use anyhow::{Context, Result};
use clap::Parser;
use path_clean::PathClean;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use chrono::Local;
use tempfile::{tempdir};
use toml;
use tracing::{debug, info, Level};
use tracing_subscriber::FmtSubscriber;
use crate::syncer_util::{count_timestamp_named_folders, rsync_extract_diff, latest_timestamp_named_dir, rsync_apply_diff, RsyncDirection};
use crate::util::{CpMvMode, fs_copy, fs_move, remove_trailing_slash, ssh_execute_remote};

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
    config: String,
}

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let ls = ssh_execute_remote("roman", "10.211.55.6", 22, "ls -l")?;
    info!("{ls}");
    return Ok(());

    let args: Args = Args::parse();

    let config_path = PathBuf::from(args.config).clean();
    let input = fs::read_to_string(config_path.clone())
        .context(format!("unable to open {:?}", config_path))?;
    let mut config: Config = toml::from_str(input.as_str())?;

    // remove trailing slashes and add later only if needed
    remove_trailing_slash(&mut config.local_archive);
    remove_trailing_slash(&mut  config.local_working_dir);

    let temp_dir = tempdir()?;
    // let exclude_filename = temp_dir.path().join("exclude.txt");
    // let mut exclude_file = File::create(exclude_filename.clone())?;
    // for exclude_pattern in &config.exclude {
    //     exclude_file.write_all(exclude_pattern.as_str().as_bytes())?;
    //     exclude_file.write_all("\n".as_bytes())?;
    // }
    // exclude_file.sync_data()?;

    let latest_archived_timestamp = latest_timestamp_named_dir(&config.local_archive, config.date_format.as_str())?;
    info!("Latest archived: {:?}", latest_archived_timestamp);

    let (latest_archived_path, mut is_fast_forward) = match latest_archived_timestamp {
        Some(latest_datetime) => {
            let is_today = latest_datetime.date_naive() == Local::now().date_naive();
            let path = config.local_archive.join(latest_datetime.format(config.date_format.as_str()).to_string());
            (path, is_today)
        }
        None => {
            let now = Local::now().format(config.date_format.as_str()).to_string();
            let path = config.local_archive.join(now);
            info!("empty archive folder, create first empty folder");
            fs::create_dir(path.clone())?;
            sleep(Duration::new(2, 0)); // needed hack, otherwise this folder will be changed below
            (path, false)
        }
    };
    // do not fast forward if only one archived folder exists, otherwise it will be lost
    is_fast_forward = if count_timestamp_named_folders(&config.local_archive, config.date_format.as_str())? == 1 {
        false
    } else {
        is_fast_forward
    };

    let rsync_dir = RsyncDirection::LocalToLocal {
        from: config.local_working_dir.clone(),
        to: latest_archived_path.clone()
    };
    let now = Local::now().format(config.date_format.as_str()).to_string();
    let diff_filename = now.clone() + ".diff";
    let diff_filepath = config.local_archive.join(diff_filename);
    let diff = rsync_extract_diff(rsync_dir, &diff_filepath, &config.exclude)?;
    match diff {
        Some(mut changed) => {
            info!("changed raw: {changed:?}");
            changed.extract_moves(&latest_archived_path, &config.local_working_dir);
            info!("try find moved files: {changed:?}");
            if is_fast_forward {
                info!("fast-forwarding by renaming latest archived folder");
                fs_move(&latest_archived_path, &config.local_archive, CpMvMode::FolderRename(now.clone()))?;
            } else {
                info!("copying latest archived folder");
                fs_copy(&latest_archived_path, &config.local_archive, CpMvMode::FolderRename(now.clone()))?;
            }

            let new_latest_archived = config.local_archive.join(now);
            rsync_apply_diff(&new_latest_archived, &diff_filepath, &config.exclude)?;
        }
        None => {
            info!("no changes")
        }
    }

    Ok(())
}
