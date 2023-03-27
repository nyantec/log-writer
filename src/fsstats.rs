use std::ffi::CString;
use std::io::{Error, ErrorKind, Result};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// `FsStats` contains some common stats about a file system.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FsStats {
    pub free_space: u64,
    pub available_space: u64,
    pub total_space: u64,
    pub allocation_granularity: u64,
}

pub(crate) fn statvfs(path: &Path) -> Result<FsStats> {
    let cstr = match CString::new(path.as_os_str().as_bytes()) {
        Ok(cstr) => cstr,
        Err(..) => return Err(Error::new(ErrorKind::InvalidInput, "path contained a null")),
    };

    let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::zeroed();

    if unsafe { libc::statvfs(cstr.as_ptr() as *const _, stat.as_mut_ptr()) } != 0 {
        Err(Error::last_os_error())
    } else {
        let stat = unsafe { stat.assume_init() };
        Ok(FsStats {
            free_space: stat.f_frsize as u64 * stat.f_bfree as u64,
            available_space: stat.f_frsize as u64 * stat.f_bavail as u64,
            total_space: stat.f_frsize as u64 * stat.f_blocks as u64,
            allocation_granularity: stat.f_frsize as u64,
        })
    }
}
