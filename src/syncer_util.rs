use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use chrono::{DateTime, FixedOffset};
use anyhow::{anyhow, Context, Result};
use pathsearch::find_executable_in_path;
use subprocess::{Exec, Redirection};
use tracing::{debug, instrument, trace, warn};
use crate::util::{add_trailing_slash, concat_str_path, path_to_str};

pub fn latest_timestamp_named_dir(p: &Path, date_format: &str) -> Result<Option<DateTime<FixedOffset>>> {
    let mut latest: Option<DateTime<FixedOffset>> = None;
    let paths = fs::read_dir(p).context("unable to read local archive")?;
    for p in paths {
        let p = p?;
        if p.metadata()?.is_dir() {
            // println!("{p:?}");
            let timestamp = DateTime::parse_from_str(
                p.path()
                    .file_name()
                    .ok_or(anyhow!("wrong archive folder name"))?
                    .to_str()
                    .ok_or(anyhow!("convert dir name to str"))?,
                date_format,
            );
            let timestamp = match timestamp {
                Ok(t) => t,
                Err(_) => {
                    warn!("strange folder, only timestamped names are expected: {:?}", p.path());
                    continue;
                }
            };

            latest = match latest {
                None => Some(timestamp),
                Some(old_dt) => {
                    if timestamp > old_dt {
                        Some(timestamp)
                    } else {
                        Some(old_dt)
                    }
                }
            };
        }
    }
    Ok(latest)
}

pub fn count_timestamp_named_folders(in_folder: &Path, date_format: &str) -> Result<usize> {
    let mut count = 0;
     let paths = fs::read_dir(in_folder).context("unable to read local archive")?;
    for p in paths {
        let p = p?;
        if p.metadata()?.is_dir() {
            match DateTime::parse_from_str(
                p.path()
                    .file_name()
                    .ok_or(anyhow!("wrong archive folder name"))?
                    .to_str()
                    .ok_or(anyhow!("convert dir name to str"))?,
                date_format,
            ) {
                Ok(_) => {
                    count += 1;
                }
                Err(_) => {}
            }
        }
    }
    Ok(count)
}

#[derive(Debug)]
pub struct SshPath {
    pub server: String,
    pub username: String,
    pub port: u16,
    pub path: PathBuf,
}

impl SshPath {
    pub fn to_args_header(&self) -> Vec<OsString> {
        let mut args = Vec::new();
        args.push(OsString::from("-e"));
        args.push(OsString::from(format!("'ssh -p {}'", self.port)));
        args
    }

    pub fn to_args_path(&self, trailing_slash: bool) -> Result<OsString> {
        let path = if trailing_slash {
            add_trailing_slash(self.path.clone())
        } else {
            self.path.clone()
        };
        let arg = OsString::from(format!("{}@{}:{}", self.username, self.server, path_to_str(&path)?));
        Ok(arg)
    }
}

#[derive(Debug)]
pub enum RsyncDirection {
    LocalToLocal {
        from: PathBuf,
        to: PathBuf
    },
    LocalToRemote {
        from: PathBuf,
        to: SshPath
    },
    RemoteToLocal {
        from: SshPath,
        to: PathBuf
    }
}

impl RsyncDirection {
    pub fn to_args(&self) -> Result<Vec<OsString>> {
        let mut args = Vec::new();
        match self {
            RsyncDirection::LocalToLocal { from, to } => {
                let from = add_trailing_slash(from.clone());
                args.push(from.as_os_str().to_os_string());
                args.push(to.as_os_str().to_os_string());
            }
            RsyncDirection::LocalToRemote { from, to } => {
                let from = add_trailing_slash(from.clone());
                args.extend_from_slice(&to.to_args_header());
                args.push(from.as_os_str().to_os_string());
                args.push(to.to_args_path(false)?);
            }
            RsyncDirection::RemoteToLocal { from, to } => {
                args.extend_from_slice(&from.to_args_header());
                args.push(from.to_args_path(true)?);
                args.push(to.as_os_str().to_os_string());
            }
        }
        Ok(args)
    }
}

/// Creates rsync patch file and return Ok(Some(path)) if there are differences, Ok(None) otherwise.
/// Return an error if rsync is absent or other os related stuff happened.
/// Runs:
/// rsync -avz --exclude-from 'temp_sync_exclude.txt' --only-write-batch=/temp/diff --delete --out-format='changed-file:%o;%n'
#[instrument]
pub fn rsync_extract_diff(rsync_dir: RsyncDirection, diff_file: &Path, exclude_file: &Path) -> Result<Option<()>> {
    trace!("working");
    let rsync_path =
        find_executable_in_path("rsync").context("Failed to find rsync in PATH")?;
    let rsync_exec = Exec::cmd(rsync_path)
        .arg("-avz")
        .arg("--exclude-from")
        .arg(exclude_file)
        .arg(concat_str_path("--only-write-batch=", &diff_file)?)
        .args(&["--delete", "--out-format='changed-file:%o;%n'"])
        .args(&rsync_dir.to_args()?)
        .stdout(Redirection::Pipe);
    debug!("{rsync_exec:?}");
    let rsync_exec = rsync_exec.capture().context("Failed to run rsync")?;
    if !rsync_exec.exit_status.success() {
        return Err(anyhow!("rsync exited with an error"));
    }

    let rsync_output = rsync_exec.stdout_str();
    println!("rsync out: {rsync_output}");
    let is_changed = rsync_output.contains("changed-file");
    let deleted_and_moved = if is_changed {
        Some(())
    } else {
        None
    };
    Ok(deleted_and_moved)
}

/// Runs:
/// rsync -avz --read-batch=diff_file --delete --out-format='changed-file:%o;%n'
#[instrument]
pub fn rsync_apply_diff(dst_folder: &Path, diff_file: &Path, exclude_file: &Path) -> Result<()> {
    trace!("working");
    let rsync_path =
        find_executable_in_path("rsync").context("Failed to find rsync in PATH")?;
    let rsync_exec = Exec::cmd(rsync_path)
        .arg("-avz")
        .arg("--exclude-from")
        .arg(exclude_file)
        .arg(concat_str_path("--read-batch=", &diff_file)?)
        .args(&["--delete", "--out-format='changed-file:%o;%n'"])
        .arg(dst_folder);
    debug!("{rsync_exec:?}");
    let rsync_exec = rsync_exec
        .join()
        .context("rsync read batch")?;
    if !rsync_exec.success() {
        return Err(anyhow!("cp exited with an error"));
    }
    Ok(())
}