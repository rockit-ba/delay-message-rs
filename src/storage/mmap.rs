//! 内存映射相关

use crate::cust_error::{panic, MmapError};
use crate::file_util::{file_path, sorted_commit_log_files};
use log::{error, info, warn};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Seek, SeekFrom};
use std::ops::DerefMut;
use byteorder::{LittleEndian, ReadBytesExt};

pub struct MmapWriter {
    /// 保存上次写的位置，以便追加写入，初始从 start_offset 文件中读取
    pub prev_write_size: usize,
    pub file_name: String,
    pub writer: MmapMut,
}
impl MmapWriter {
    /// None 用于程序启动是自动初始化
    ///
    /// Some 用于程序运行过程中创建新的写文件
    pub fn new(
        file_name: Option<&str>,
        init_file_name: &str,
        dir_name: &str,
        offset: Option<usize>,
        mmap_len: u64,
    ) -> Self {
        let file_name_ = match file_name {
            None => Self::file_name_create(init_file_name, dir_name),
            Some(file_name) => String::from(file_name),
        };
        info!("当前 write file name：{file_name_}");

        let path = file_path(dir_name).join(file_name_.as_str());
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(file) => {
                let mut writer = Self::mmap_mut_create(&file, mmap_len);
                let offset = Self::start_offset_process(offset, &mut writer);
                info!("读取 START_OFFSET：{}", offset);
                Self {
                    prev_write_size: offset,
                    file_name: file_name_,
                    writer,
                }
            }
            Err(err) => {
                let err = MmapError::OpenErr(err.to_string());
                panic(err.to_string().as_str())
            }
        }
    }

    fn start_offset_process(offset: Option<usize>, writer: &mut MmapMut) -> usize {
        let offset = match offset {
            None => {
                // 表明是consume_queue已经存在的文件，从文件末尾8个字节读出offset
                warn!("consume_queue 文件 开始获取 start_offset");
                let mut reader = Cursor::new(writer.deref_mut());
                reader.seek(SeekFrom::End(-8)).unwrap();
                let offset = reader.read_u64::<LittleEndian>().unwrap_or_else(|err| {
                    error!("读取 start_offset 错误 \n{:?},返回默认 0", err);
                    0_u64
                });
                offset as usize
            }
            Some(ele) => { ele }
        };
        offset
    }
    /// 创建 MmapMut
    pub fn mmap_mut_create(file: &File, mmap_len: u64) -> MmapMut {
        if let Err(err) = file.set_len(mmap_len) {
            let err = MmapError::SetLenErr(err.to_string());
            panic(err.to_string().as_str())
        }

        unsafe {
            match MmapOptions::new().map_mut(file) {
                Ok(result) => result,
                Err(err) => panic(MmapError::MmapErr(err.to_string()).to_string().as_str()),
            }
        }
    }

    /// 初始化写文件的名称
    pub fn file_name_create(init_file_name: &str, dir_name: &str) -> String {
        sorted_commit_log_files(dir_name)
            .iter()
            .map(|file| file.file_name().to_str().unwrap().to_string())
            .last()
            .unwrap_or(init_file_name.to_string())
    }

    /// // 还原当前文件参数
    pub fn new_writer_create(&mut self, new_name: &str, new_writer: Self) {
        self.prev_write_size = 0;
        self.file_name = String::from(new_name);
        self.writer = new_writer.writer;
    }
}
