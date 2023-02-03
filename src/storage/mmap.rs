//! 内存映射相关

use std::fs::File;
use memmap2::{MmapMut, MmapOptions};
use crate::cust_error::{MmapError, panic};


/// 创建 MmapMut
pub fn mmap_mut_create(file: &File, mmap_len: u64) -> MmapMut {
    if let Err(err) = file.set_len(mmap_len) {
        let err = MmapError::SetLenErr(err.to_string());
        panic(err.to_string().as_str())
    }

    unsafe {
        match MmapOptions::new().map_mut(file) {
            Ok(result) => result,
            Err(err) => panic(
                MmapError::MmapErr(err.to_string())
                    .to_string().as_str(),
            ),
        }
    }
}
