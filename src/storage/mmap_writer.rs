//! 写文件

use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;

/// 存储文件初始化大小
const FILE_SIZE: u64 = 1024;
/// 记录服务正在运行的 mmap 的开始写入的 offset
static START_OFFSET: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    static ref MMAP: Mutex<MmapMut> = {
        let path: PathBuf = PathBuf::from("00000");
        let file = OpenOptions::new()
            .create(true)
            .read(true).write(true)
            .open(path).expect("打开文件失败");

        file.set_len(FILE_SIZE).unwrap();
        // 此offset 需要加载文件的时候计算
        // 不同于 START_OFFSET，这里的代表磁盘上开始的写入位置
        let offset = 3;
        START_OFFSET.store(offset, Ordering::SeqCst);
        Mutex::new(unsafe { MmapOptions::new().map_mut(&file).unwrap() })
    };
}

/// 写数据
pub fn write(data: &[u8]) {
    {
        let mut m_map = MMAP.lock().unwrap();
        let start = START_OFFSET.fetch_add(data.len(), Ordering::SeqCst);
        (&mut m_map[start..]).write_all(data).unwrap();
        m_map.flush_async().unwrap();
    }
}

#[cfg(test)]

mod tests {
    use crate::common::log_util::log_init;
    use crate::storage::mmap_writer::write;

    #[test]
    fn test_01() {
        log_init();
        write(b"abc");
    }
    #[test]
    fn test_02() {
        log_init();
        write(b"efg");
        write(b"hij");
    }
}