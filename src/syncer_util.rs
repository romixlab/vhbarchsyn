use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use chrono::{DateTime, FixedOffset};
use anyhow::{anyhow, Context, Result};
use pathsearch::find_executable_in_path;
use subprocess::{Exec, Redirection};
use tracing::{debug, error, info, instrument, trace, warn};
use crate::util::{add_trailing_slash, concat_str_path, path_to_str, remove_trailing_slash};

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

#[derive(Debug, Clone)]
pub enum FsEntity {
    Folder(PathBuf),
    File(PathBuf),
}

#[derive(Debug)]
pub struct ChangeList {
    deleted: Vec<FsEntity>,
    changed: Vec<FsEntity>,
    moved: Vec<(FsEntity, PathBuf)>,
}

impl ChangeList {
    pub fn collect<S: AsRef<str>>(s: S) -> Option<Self> {
        let mut deleted = Vec::new();
        let mut changed = Vec::new();
        let mut lines = s.as_ref().lines();
        const DEL_PREFIX: &'static str = "'changed-file:del.;";
        const SEND_PREFIX: &'static str = "'changed-file:send;";
        for line in lines {
            let (path, is_deletion) = if line.starts_with(DEL_PREFIX) {
                (&line[DEL_PREFIX.len() .. line.len() - 1], true)
            } else if line.starts_with(SEND_PREFIX) {
                (&line[DEL_PREFIX.len() .. line.len() - 1], false)
            } else {
                continue
            };
            let is_folder = path.ends_with("/");
            let entity = if is_folder {
                FsEntity::Folder(PathBuf::from(&path[..path.len() - 1]))
            } else {
                FsEntity::File(PathBuf::from(path))
            };
            if is_deletion {
                deleted.push(entity);
            } else {
                changed.push(entity);
            }
        }
        if deleted.is_empty() && changed.is_empty() {
            return None;
        }
        Some(ChangeList {
            deleted,
            changed,
            moved: vec![]
        })
    }

    pub fn extract_moves(&mut self, archived_dir: &Path, working_dir: &Path) -> Vec<FsEntity> {
        let mut moved = Vec::new();
        let mut deletions_to_keep = vec![];
        for deleted in &self.deleted {
            match deleted {
                FsEntity::Folder(_) => {
                    deletions_to_keep.push(true);
                },
                FsEntity::File(deleted_path) => {
                    let deleted_filename = match deleted_path.file_name() {
                        Some(filename) => filename,
                        None => {
                            deletions_to_keep.push(true);
                            continue
                        }
                    };
                    // debug!("del_filename: {deleted_filename:?}");
                    // debug!("del file in archive: {:?}", archived_dir.join(deleted_path));
                    let deleted_file_size = match fs::metadata(archived_dir.join(deleted_path)) {
                        Ok(metadata) => {
                            metadata.len()
                        },
                        Err(_) => {
                            deletions_to_keep.push(true);
                            continue
                        }
                    };
                    // debug!("fsize: {deleted_file_size}");
                    let same_filenames = self.changed.iter().fold(Vec::new(), |mut paths, entity| {
                        match entity {
                            FsEntity::Folder(_) => {}
                            FsEntity::File(changed_path) => {
                                match changed_path.file_name() {
                                    Some(changed_filename) => {
                                        if changed_filename == deleted_filename {
                                            paths.push(changed_path);
                                        }
                                    }
                                    None => {}
                                }
                            }
                        }
                        paths
                    });
                    // debug!("same filenames changed: {same_filenames:?}");
                    for candidate in same_filenames {
                        match fs::metadata(working_dir.join(candidate)) {
                            Ok(metadata) => {
                                // debug!("candidate meta ok, size: {}", metadata.len());
                                if deleted_file_size == metadata.len() { // TODO: check file hash as well?
                                    debug!("found a move for {deleted_path:?}");
                                    deletions_to_keep.push(false);
                                    self.moved.push((deleted.clone(), candidate.to_path_buf()));
                                    continue;
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    deletions_to_keep.push(true);
                }
            }
        }
        let mut keep_iter = deletions_to_keep.iter();
        self.deleted.retain(|_| *keep_iter.next().unwrap());
        moved
    }
}

/// Creates rsync patch file and return Ok(Some(path)) if there are differences, Ok(None) otherwise.
/// Return an error if rsync is absent or other os related stuff happened.
/// Runs:
/// rsync -avz --exclude-from 'temp_sync_exclude.txt' --only-write-batch=/temp/diff --delete --out-format='changed-file:%o;%n'
#[instrument]
pub fn rsync_extract_diff(rsync_dir: RsyncDirection, diff_file: &Path, exclude_file: &Path) -> Result<Option<ChangeList>> {
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

    if rsync_output.contains("No batched update for") {
        error!("sad news, rsync failed (no batched update for)");
        return Err(anyhow!("rsync failure"));
    }

    let delete_and_move = ChangeList::collect(rsync_output);
    Ok(delete_and_move)
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
        .stdout(Redirection::Pipe)
        .capture()
        .context("rsync read batch")?;

    if !rsync_exec.exit_status.success() {
        return Err(anyhow!("rsync exited with an error"));
    }
    let rsync_output = rsync_exec.stdout_str();
    println!("rsync out: {rsync_output}");

    if rsync_output.contains("No batched update for") {
        error!("sad news, rsync failed");
        return Err(anyhow!("rsync failure"));
    }

    Ok(())
}