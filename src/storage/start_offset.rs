//! 持久化 start_offset

use crate::storage::commit_log::START_OFFSET;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use log::{error, info};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::Mutex;
use tokio::time::interval;

/// 持久化间隔，单位秒
const INTERVAL: u64 = 5;
/// 存储文件名
const START_OFFSET_FILE: &str = "start_offset";

lazy_static! {
    /// start_offset 文件引用
    static ref FILE: Mutex<File> = {
        let path: PathBuf = PathBuf::from(START_OFFSET_FILE);
        let file = OpenOptions::new()
        .create(true).append(true).read(true)
        .open(path).expect("打开 start_offset 存储文件失败");
        Mutex::new(file)
    };
}

/// 定时持久化 start_offset
async fn write_schedule() {
    // 每隔1秒执行一次
    let mut interval = interval(std::time::Duration::from_secs(INTERVAL));
    loop {
        interval.tick().await;
        write().await;
        info!("持久化 start_offset 成功");
    }
}

/// 持久化 start_offset
async fn write() {
    let offset = unsafe { START_OFFSET as u64 };
    {
        let mut file = FILE.lock().unwrap();
        file.write_u64::<LittleEndian>(offset)
            .unwrap_or_else(|err| {
                error!("持久化 start_offset 文件错误 {:?}", err);
            });
    }
}

/// 获取文件存储的 start_offset
pub fn read() -> usize {
    {
        let mut file = FILE.lock().unwrap();
        let offset = file.read_u64::<LittleEndian>().unwrap_or_else(|err| {
            error!("读取 start_offset 文件错误 {:?},返回默认 0", err);
            0_u64
        });
        offset as usize
    }
}

#[cfg(test)]

mod tests {
    use crate::common::log_util::log_init;
    use crate::storage::start_offset::{read, write};
    use log::info;

    #[tokio::test]
    async fn test_start_offset_write() {
        log_init();
        write().await;
    }

    #[test]
    fn test_start_offset_read() {
        log_init();
        let i = read();
        info!("{i}");
    }
}
