//! 内存映射相关

use std::fs::{File};
use memmap2::{MmapMut, MmapOptions};
use crate::cust_error::{MmapError, panic};
use crate::file_util::sorted_commit_log_files;


pub struct MmapWriter {
    /// 保存上次写的位置，以便追加写入，初始从 start_offset 文件中读取
    pub prev_write_size: usize,
    pub file_name: String,
    pub writer: MmapMut,
}
impl MmapWriter {
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

    /// 初始化写文件的名称
    pub fn file_name_create(init_file_name: &str, dir_name: &str) -> String {
        sorted_commit_log_files(dir_name).iter()
            .map(|file| file.file_name().to_str().unwrap().to_string())
            .last()
            .unwrap_or(init_file_name.to_string())
    }
}


