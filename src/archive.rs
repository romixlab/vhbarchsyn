use std::fs;
use std::path::Path;
use anyhow::{Context, Result};
use chrono::{Duration, Local};
use tracing::info;
use crate::syncer_util::{count_timestamp_named_folders, latest_timestamp_named_dir, rsync_apply_diff, rsync_extract_diff, RsyncDirection};
use crate::util::{CpMvMode, fs_copy, fs_move};

pub fn archive_local(working_dir: &Path, local_archive: &Path, exclude_file: &Path, date_format: &str) -> Result<()> {
    let latest_archived_timestamp = latest_timestamp_named_dir(local_archive, date_format)?;
    info!("Latest archived: {:?}", latest_archived_timestamp);

    let (latest_archived_path, mut is_fast_forward) = match latest_archived_timestamp {
        Some(latest_datetime) => {
            let is_today = latest_datetime.date_naive() == Local::now().date_naive();
            let path = local_archive.join(latest_datetime.format(date_format).to_string());
            (path, is_today)
        }
        None => {
            let now = (Local::now() - Duration::seconds(1)).format(date_format).to_string();
            let path = local_archive.join(now);
            info!("empty archive folder, create first empty folder");
            fs::create_dir(path.clone())?;
            (path, false)
        }
    };
    // do not fast forward if only one archived folder exists, otherwise it will be lost
    is_fast_forward = if count_timestamp_named_folders(local_archive, date_format)? == 1 {
        false
    } else {
        is_fast_forward
    };

    let rsync_dir = RsyncDirection::LocalToLocal {
        from: working_dir.to_path_buf(),
        to: latest_archived_path.clone()
    };
    let now = Local::now().format(date_format).to_string();
    let diff_filename = now.clone() + ".diff";
    let diff_filepath = local_archive.join(diff_filename);
    let diff = rsync_extract_diff(rsync_dir, &diff_filepath, exclude_file)?;
    match diff {
        Some(mut changed) => {
            info!("changed raw: {changed:?}");
            changed.extract_moves(&latest_archived_path, working_dir);
            info!("try find moved files: {changed:?}");
            if is_fast_forward {
                info!("fast-forwarding by renaming latest archived folder");
                fs_move(&latest_archived_path, local_archive, CpMvMode::FolderRename(now.clone()))?;
            } else {
                info!("copying latest archived folder");
                fs_copy(&latest_archived_path, local_archive, CpMvMode::FolderRename(now.clone()))?;
            }

            let new_latest_archived = local_archive.join(now.clone());
            info!("applying diff file");
            rsync_apply_diff(&new_latest_archived, &diff_filepath, exclude_file)?;

            info!("saving change list");
            let changed_json = serde_json::to_string(&changed).context("serializing change list")?;
            fs::write(local_archive.join(format!("{}.changes", now)), changed_json).context("writing change list")?;
        }
        None => {
            info!("no changes")
        }
    }
    Ok(())
}