//! commit_log 文件模块
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::str::FromStr;
use std::sync::{RwLock};
use byteorder::{LittleEndian, ReadBytesExt};

use crate::storage::message::Message;
use crate::storage::start_offset;
use crate::cust_error::{CommitLogError, panic};
use crate::file_util;

use lazy_static::lazy_static;
use log::{error, info};

/// 存储文件初始化大小
const FILE_SIZE: u64 = 1024;
/// 记录服务正在运行的 mmap 的开始写入的 offset
pub static mut START_OFFSET: usize = 0;
/// 第一个存储文件的名称
const INIT_LOG_FILE_NAME: u64 = 0;

const DIR_NAME: &str = "store/commit_log";


lazy_static! {
    /// 内存映射可变引用
    static ref MMAP_WRITER: MmapWriter = {
        let file_name = MmapWriter::init_file_name();
        let path = std::env::current_dir().expect("获取应用程序目录异常")
            .join(DIR_NAME).join(file_name.as_str());
        match OpenOptions::new().create(true).read(true).write(true).open(path) {
            Ok(file) => {
                let writer = init_mmap_writer(&file);
                MmapWriter {file_name, writer}
            },
            Err(err) => {
                let err = CommitLogError::OpenErr(err.to_string());
                panic(err.to_string().as_str())
            }
        }
    };
}

/// commit_log 写对象
struct MmapWriter {
    file_name: String,
    writer: RwLock<MmapMut>,
}
impl MmapWriter {
    /// 初始化写文件的名称
    fn init_file_name() -> String {
        let path = std::env::current_dir().expect("获取应用程序目录异常")
            .join(DIR_NAME);
        let sort = file_util::get_all_files(&path)
            .iter()
            .map(|ele| u64::from_str(ele.file_name().to_str().unwrap()).unwrap())
            .collect::<Vec<_>>();
        sort.last().copied().unwrap_or(INIT_LOG_FILE_NAME).to_string()
    }
}

fn init_mmap_writer(file: &File) -> RwLock<MmapMut>{
    if let Err(err) = file.set_len(FILE_SIZE) {
        let err = CommitLogError::SetLenErr(err.to_string());
        panic(err.to_string().as_str())
    }

    let offset = start_offset::read();
    info!("从 start_offset 文件读取 START_OFFSET：{}", offset);

    // 为了防止异常情况下最后的offset值没有写入 start_offset  文件，增量继续获取真实的offset
    let offset = real_start_offset(file, offset);
    info!("从 log 文件重新计算 START_OFFSET：{}", offset);
    unsafe {START_OFFSET = offset};

    RwLock::new(unsafe {
        match MmapOptions::new().map_mut(file) {
            Ok(result) => result,
            Err(err) => {
                panic(CommitLogError::MmapErr(err.to_string()).to_string().as_str())
            }
        }
    })
}

/// 初始化log 文件真实的开始写的位置
/// stored_offset：从start_offset文件获取的最后写入的值
fn real_start_offset(file: &File, stored_offset: usize) -> usize {
    let mut real_offset = stored_offset;
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(real_offset as u64)).unwrap();
    // 注意，这里已经游标走出4个
    while let Ok(size) = reader.read_u32::<LittleEndian>() {
        if size < Message::fix_len() { break }

        let mut data = vec![0u8; size as usize];
        reader.read_exact(&mut data).unwrap();
        info!("{}",data.len());
        if let Some(msg) = Message::deserialize_binary(&mut data, size) {
            info!("解析消息：{:?}", &msg);
            real_offset += msg.msg_len() as usize;
        };
    }
    real_offset
}


/// 根据queue_consume 读取一个消息
///
/// offset  读取的位置
///
/// size    读取的长度
pub fn read(offset: u64, size: u32) -> Vec<u8>{
    let start = offset as usize;
    let len = (offset + size as u64) as usize;
    {
        let m_map = MMAP_WRITER.writer.read().unwrap();
        let data = &m_map[start..len];
        data.to_vec()
    }
}

/// 写数据
pub fn write(data: &[u8]) {
    {
        let mut m_map = MMAP_WRITER.writer.write().unwrap();
        unsafe {
            let mut buf = &mut m_map[START_OFFSET..];
            if buf.len() < data.len() {
                info!("当前commit_log文件已满，开始创建新的文件");
                // todo

                return;
            }
            buf.write_all(data).unwrap();
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

    #[test]
    fn sys_root_test() {
        log_init();
        let string = format!("{:?}", std::env::current_dir().expect("获取应用程序目录异常").as_os_str());
        info!("{string}");
    }
}
