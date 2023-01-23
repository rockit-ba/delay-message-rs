//! commit_log 文件模块

use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{RwLock};
use byteorder::{LittleEndian, ReadBytesExt};

use crate::storage::message::Message;
use crate::storage::start_offset;
use lazy_static::lazy_static;
use log::{error, info};

/// 存储文件初始化大小
const FILE_SIZE: u64 = 1024;
/// 记录服务正在运行的 mmap 的开始写入的 offset
pub static mut START_OFFSET: usize = 0;
/// 第一个存储文件的名称
static INIT_LOG_FILE_NAME: &str = "0";

lazy_static! {
    /// 内存映射可变引用
    static ref MMAP_WRITER: RwLock<MmapMut> = {
        let path: PathBuf = PathBuf::from(INIT_LOG_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true).write(true)
            .open(path).expect("打开log文件失败");

        file.set_len(FILE_SIZE).expect("文件初始化设置异常");
        // 此offset 需要加载文件的时候计算
        // 不同于 START_OFFSET，这里的代表磁盘上开始的写入位置
        let offset = start_offset::read();
        info!("从 start_offset 文件读取 START_OFFSET：{}", offset);
        let offset = start_offset_init(&mut file, offset);
        info!("从 log 文件重新计算 START_OFFSET：{}", offset);

        unsafe {START_OFFSET = offset};
        RwLock::new(unsafe { MmapOptions::new().map_mut(&file).expect("虚拟内存映射初始化异常") })
    };
}

/// 初始化log 文件开始写的位置，
///
/// 此方法用于
fn start_offset_init(file: &File, offset: usize) -> usize {
    let mut offset = offset;
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(offset as u64)).unwrap();
    // 注意，这里已经游标走出4个
    while let Ok(size) = reader.read_u32::<LittleEndian>() {
        // 固定长度 40，因此不可能小于40
        if size < 40 { break }
        let mut data = vec![0u8; size as usize];
        reader.read_exact(&mut data).unwrap();
        info!("{}",data.len());
        if let Some(msg) = Message::deserialize_binary(&mut data, size) {
            info!("解析消息：{:?}", &msg);
            offset += msg.msg_len() as usize;
        };
    }
    offset
}

/// offset 读取的位置
///
/// size 读取的长度
pub fn read(offset: u64, size: u32) -> Vec<u8>{
    let start = offset as usize;
    let len = (offset + size as u64) as usize;
    {
        let m_map = MMAP_WRITER.read().unwrap();
        let data = &m_map[start..len];
        data.to_vec()
    }
}

/// 写数据
pub fn write(data: &[u8]) {
    {
        let mut m_map = MMAP_WRITER.write().unwrap();
        unsafe {
            (&mut m_map[START_OFFSET..]).write_all(data).unwrap();
            START_OFFSET += data.len();
        }
        if let Err(err) = m_map.flush_async() {
            error!("log文件 flush_async 异常：{:?}", err);
        }
    }
}

#[cfg(test)]

mod tests {
    use log::info;
    use crate::common::log_util::log_init;
    use crate::storage::message::Message;
    use crate::storage::commit_log::write;

    #[test]
    fn test_01_write_message() {
        log_init();
        let json = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"此情可待成追忆\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json).serialize_binary();
        let x = message.as_slice();
        info!("{}", x.len());
        write(x);

        let json2 = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"只是当时已惘然\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message2 = Message::deserialize_json(&json2).serialize_binary();
        let x2 = message2.as_slice();
        info!("{}", x.len());
        write(x2);
    }
}
