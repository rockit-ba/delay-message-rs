//! 持久化 start_offset

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use log::{error};
use std::fs::{OpenOptions};
pub use std::io::{Write};
use std::io::Cursor;
use std::ops::{DerefMut};

use memmap2::{MmapMut};
use crate::storage::mmap::MmapWriter;


/// 持久化间隔，单位秒
const INTERVAL: u64 = 5;
/// 存储文件名
const START_OFFSET_FILE: &str = "start_offset";
/// 存储映射引用
static mut START_OFFSET: Option<MmapMut> = None;

/// 单例获取 START_OFFSET
fn instance() -> &'static mut MmapMut {
    unsafe {
        if START_OFFSET.is_none() {
            let file = OpenOptions::new()
                .create(true).write(true).read(true)
                .open(START_OFFSET_FILE)
                .expect("打开 start_offset 存储文件失败");
            START_OFFSET = Some(MmapWriter::mmap_mut_create(&file, 8));
        }
        START_OFFSET.as_mut().unwrap()
    }
}


/// 持久化 start_offset
pub fn write(offset: u64) {
    instance().deref_mut().write_u64::<LittleEndian>(offset)
        .unwrap_or_else(|err| {
            error!("持久化 start_offset 文件错误 \n{:?}", err);
        });
}

/// 获取文件存储的 start_offset
pub fn read() -> usize {
    let mut reader = Cursor::new(instance().deref_mut());
    let offset = reader.read_u64::<LittleEndian>().unwrap_or_else(|err| {
        error!("读取 start_offset 文件错误 \n{:?},返回默认 0", err);
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
