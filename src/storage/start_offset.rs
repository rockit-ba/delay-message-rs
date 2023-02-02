//! 持久化 start_offset

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use log::{error};
use std::fs::{OpenOptions};
pub use std::io::{Write};
use std::ops::{DerefMut};
use std::path::PathBuf;
use memmap2::{MmapMut, MmapOptions};
use crate::cust_error::{CommitLogError, panic};


/// 持久化间隔，单位秒
const INTERVAL: u64 = 5;
/// 存储文件名
const START_OFFSET_FILE: &str = "start_offset";

static mut FILE: Option<MmapMut> = None;

fn get_file() -> &'static mut MmapMut {
    unsafe {
        if FILE.is_none() {
            let path: PathBuf = PathBuf::from(START_OFFSET_FILE);
            let file = OpenOptions::new()
                    .create(true).write(true).read(true)
                    .open(path).expect("打开 start_offset 存储文件失败");
            file.set_len(8).unwrap();
            FILE = Some(match MmapOptions::new().map_mut(&file) {
                Ok(result) => result,
                Err(err) => panic(
                    CommitLogError::MmapErr(err.to_string())
                        .to_string()
                        .as_str(),
                ),
            });
        }
        FILE.as_mut().unwrap()
    }
}


/// 持久化 start_offset
pub fn write(offset: u64) {
    // todo 由writer主动通知写checkpoint文件
    get_file().deref_mut().write_u64::<LittleEndian>(offset)
        .unwrap_or_else(|err| {
            error!("持久化 start_offset 文件错误 {:?}", err);
        });
    get_file().flush().unwrap();
}

/// 获取文件存储的 start_offset
pub fn read() -> usize {
    let mut reader = std::io::Cursor::new(get_file().deref_mut());
    let offset = reader.read_u64::<LittleEndian>().unwrap_or_else(|err| {
        error!("读取 start_offset 文件错误 {:?},返回默认 0", err);
        0_u64
    });
    offset as usize
}

#[cfg(test)]

mod tests {
    use crate::common::log_util::log_init;
    use crate::storage::start_offset::{read};
    use log::info;


    #[test]
    fn test_start_offset_read() {
        log_init();
        let i = read();
        info!("{i}");
    }
}
