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
/// 第一个存储文件的名称
static INIT_LOG_FILE_NAME: &str = "0";

lazy_static! {
    /// 内存映射可变引用
    static ref MMAP_WRITER: Mutex<MmapMut> = {
        let path: PathBuf = PathBuf::from(INIT_LOG_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true).write(true)
            .open(path).expect("打开文件失败");

        file.set_len(FILE_SIZE).unwrap();
        // 此offset 需要加载文件的时候计算
        // 不同于 START_OFFSET，这里的代表磁盘上开始的写入位置
        let offset = 0;
        START_OFFSET.store(offset, Ordering::SeqCst);
        Mutex::new(unsafe { MmapOptions::new().map_mut(&file).unwrap() })
    };
}

/// 写数据
pub fn write(data: &[u8]) {
    {
        let mut m_map = MMAP_WRITER.lock().unwrap();
        let start = START_OFFSET.fetch_add(data.len(), Ordering::SeqCst);
        (&mut m_map[start..]).write_all(data).unwrap();
        m_map.flush_async().unwrap();
    }
}

#[cfg(test)]

mod tests {
    use crate::common::log_util::log_init;
    use crate::storage::message::Message;
    use crate::storage::mmap_writer::write;
    use std::fs::OpenOptions;
    use std::path::PathBuf;

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

    #[test]
    fn test_01_write_message() {
        log_init();
        let json = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"此情可待成追忆\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json).serialize_binary();
        let x = message.as_slice();
        write(x);

        let json2 = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"只是当时已惘然\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message2 = Message::deserialize_json(&json2).serialize_binary();
        let x2 = message2.as_slice();
        write(x2);
    }

    #[test]
    fn test_01_read_message() {
        let path: PathBuf = PathBuf::from("0");
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("打开文件失败");
        let message = Message::deserialize_binary(&mut file);
        println!("总大小 {}-{:?}", message.msg_len(), &message);

        let message2 = Message::deserialize_binary(&mut file);
        println!(
            "总大小 {}-{:?}",
            message.msg_len() + message2.msg_len(),
            &message2
        );
    }
}
