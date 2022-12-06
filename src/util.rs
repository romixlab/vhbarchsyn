use std::{env, io};
use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use path_clean::PathClean;
use anyhow::{anyhow, Context, Result};
use pathsearch::find_executable_in_path;
use subprocess::Exec;
use tracing::{debug, instrument, trace};

#[allow(dead_code)]
pub fn absolute_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref();

    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    }.clean();

    Ok(absolute_path)
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum CpMvMode {
    File,
    FileRename(String),
    Folder,
    FolderRename(String)
}

#[instrument]
pub fn fs_copy(src_path: &Path, dst_folder: &Path, mode: CpMvMode) -> Result<()> {
    trace!("copying");
    let cp_path =
        find_executable_in_path("cp").context("Failed to find cp in PATH")?;
    let cp_run = Exec::cmd(cp_path);
    let cp_run = match mode.clone() {
        CpMvMode::File | CpMvMode::Folder => {
            let dst_folder = add_trailing_slash(dst_folder.to_path_buf());
            if mode == CpMvMode::Folder {
                cp_run.arg("-r").args(&[src_path, &dst_folder])
            } else {
                cp_run.args(&[src_path, &dst_folder])
            }
        },
        CpMvMode::FileRename(to) | CpMvMode::FolderRename(to) => {
            let dst_path = dst_folder.join(to);
            if matches!(mode, CpMvMode::FolderRename(_)) {
                cp_run.arg("-r").args(&[src_path, &dst_path])
            } else {
                cp_run.args(&[src_path, &dst_path])
            }
        }
    };
    debug!("{cp_run:?}");
    let cp_run = cp_run
        .join()
        .context("Failed to run cp")?;
    if !cp_run.success() {
        return Err(anyhow!("cp exited with an error"));
    }
    Ok(())
}

#[instrument]
pub fn fs_move(src_path: &Path, dst_folder: &Path, mode: CpMvMode) -> Result<()> {
    trace!("moving");
    let mv_path =
        find_executable_in_path("mv").context("Failed to find mv in PATH")?;
    let dst = match mode {
        CpMvMode::File | CpMvMode::Folder => {
            add_trailing_slash(dst_folder.to_path_buf())
        }
        CpMvMode::FileRename(to) | CpMvMode::FolderRename(to) => {
            dst_folder.join(to)
        }
    };
    let mv_run = Exec::cmd(mv_path)
        .args(&[src_path, &dst])
        .join()
        .context("Failed to run mv")?;
    if !mv_run.success() {
        return Err(anyhow!("mv exited with an error"));
    }
    Ok(())
}

#[cfg(windows)]
fn has_trailing_slash(p: &Path) -> bool {
    let last = p.as_os_str().encode_wide().last();
    last == Some(b'\\' as u16) || last == Some(b'/' as u16)
}
#[cfg(unix)]
fn has_trailing_slash(p: &Path) -> bool {
    p.as_os_str().as_bytes().last() == Some(&b'/')
}

#[allow(dead_code)]
pub fn add_trailing_slash(mut p: PathBuf) -> PathBuf {
    let dirname = if let Some(fname) = p.file_name() {
        let mut s = OsString::with_capacity(fname.len() + 1);
        s.push(fname);
        if cfg!(windows) {
            s.push("\\");
        } else {
            s.push("/");
        }
        s
    } else {
        OsString::new()
    };

    if p.pop() {
        p.push(dirname);
    }
    p
}

// see https://www.reddit.com/r/rust/comments/ooh5wn/damn_trailing_slash/ for more fun
pub fn remove_trailing_slash(p: &mut PathBuf) {
    if has_trailing_slash(&p) {
        let dirname = if let Some(fname) = p.file_name() {
            match fname.to_str() {
                Some(fname) => {
                    let mut s = OsString::with_capacity(fname.len() - 1);
                    s.push(OsString::from_str(&fname[..fname.len() - 2]).expect(""));
                    s
                }
                None => OsString::new()
            }
        } else {
            OsString::new()
        };

        if p.pop() {
            p.push(dirname);
        }
    }
}

pub fn path_to_str(p: &Path) -> Result<&str> {
    p.to_str().ok_or(anyhow!("Path::to_str() failed, non-unicode symbols in path?"))
}

pub fn enclose_path_in(p: &Path, symbol: char) -> Result<String> {
    let p = path_to_str(p)?;
    let mut s = String::with_capacity(p.len() + 2);
    s.push(symbol);
    s.push_str(p);
    s.push(symbol);
    Ok(s)
}

pub fn concat_str_path<S: AsRef<str>>(s: S, p: &Path) -> Result<String> {
    let p = path_to_str(p)?;
    let mut c = String::with_capacity(s.as_ref().len() + p.len());
    c.push_str(s.as_ref());
    c.push_str(p);
    Ok(c)
}