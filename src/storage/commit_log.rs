//! commit_log 文件模块

use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::mem;
use std::ops::{DerefMut};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use crossbeam::atomic::AtomicCell;

use crate::cust_error::{panic, CommitLogError};
use crate::file_util;
use crate::storage::message::Message;
use crate::storage::start_offset;

use lazy_static::lazy_static;
use log::{error, info};

/// 存储文件初始化大小
const FILE_SIZE: u64 = 200;
/// 第一个存储文件的名称
const INIT_LOG_FILE_NAME: &str = "00000000000000000000";
/// 文件存储目录
const DIR_NAME: &str = "store/commit_log";

lazy_static! {
    /// writer
    pub static ref MMAP_WRITER: MmapWriter = MmapWriter::new(None);
    static ref MMAP_READERS: Vec<MmapReader> = MmapReader::init_readers();
}

/// commit_log 写对象
pub struct MmapWriter {
    // 记录服务正在运行的 mmap 的开始写入的 offset
    start_offset: AtomicUsize,
    file_name: AtomicCell<String>,
    writer: RwLock<MmapMut>,
}
impl MmapWriter {
    /// 创建实例
    ///
    /// None 用于程序启动是自动初始化
    ///
    /// Some 用于程序运行过程中创建新的写文件
    fn new(file_name: Option<&str>) -> MmapWriter {
        let file_name_ = match file_name {
            None => MmapWriter::file_name_create(),
            Some(file_name) => String::from(file_name),
        };
        info!("当前 write file name：{file_name_}");

        let path = file_path().join(file_name_.as_str());

        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(file) => {
                let (writer, start_offset) = MmapWriter::writer_create(&file);
                MmapWriter {
                    start_offset,
                    file_name: AtomicCell::new(file_name_),
                    writer,
                }
            }
            Err(err) => {
                let err = CommitLogError::OpenErr(err.to_string());
                panic(err.to_string().as_str())
            }
        }
    }

    /// 初始化写文件的名称
    fn file_name_create() -> String {
        let files = sorted_commit_log_files();
        files.iter()
            .map(|file| file.file_name().to_str().unwrap().to_string())
            .last()
            .unwrap_or(INIT_LOG_FILE_NAME.to_string())
    }

    /// 创建 MmapWriter#writer
    fn writer_create(file: &File) -> (RwLock<MmapMut>, AtomicUsize) {
        if let Err(err) = file.set_len(FILE_SIZE) {
            let err = CommitLogError::SetLenErr(err.to_string());
            panic(err.to_string().as_str())
        }

        let offset = start_offset::read();
        info!("从 start_offset 文件读取 START_OFFSET：{}", offset);

        // 为了防止异常情况下最后的offset值没有写入 start_offset  文件，增量继续获取真实的offset
        let offset = Self::real_start_offset(file, offset);
        info!("从 log 文件重新计算 START_OFFSET：{}", offset);

        (
            RwLock::new(unsafe {
                match MmapOptions::new().map_mut(file) {
                    Ok(result) => result,
                    Err(err) => panic(
                        CommitLogError::MmapErr(err.to_string())
                            .to_string()
                            .as_str(),
                    ),
                }
            }),
            AtomicUsize::new(offset),
        )
    }

    /// 获取 writer 的 start_offset
    pub fn start_offset(&self) -> usize {
        self.start_offset.load(Ordering::SeqCst)
    }
    /// 写数据
    pub fn write(&self, data: &[u8]) {
        {
            let mut m_map = self.writer.write().unwrap();
            let mut buf = &mut m_map[self.start_offset.load(Ordering::SeqCst)..];

            info!("当前文件剩余：{},当前数据大小：{}", buf.len(), data.len());
            if buf.len() > data.len() {
                buf.write_all(data).unwrap();
                self.start_offset.fetch_add(data.len(), Ordering::SeqCst);
                if let Err(err) = m_map.flush_async() {
                    error!("log文件 flush_async 异常：{:?}", err);
                }
                return;
            }
        }

        self.new_writer_create();
        self.write(data);
    }

    /// 当前commit_log文件已满，开始创建新的文件
    fn new_writer_create(&self) {
        self.start_offset.store(0, Ordering::SeqCst);
        let full_file = self.file_name.take();
        let curr = u64::from_str(full_file.as_str()).unwrap();
        info!("当前commit_log文件[{}]已满，开始创建新的文件", full_file);

        let new_name = format!("{number:>0width$}", number = curr + FILE_SIZE, width = 20);
        let new_writer = Self::new(Some(new_name.as_str()));
        self.file_name.store(new_writer.file_name.take());

        {
            let mut old = self.writer.write().unwrap();
            let mut new = new_writer.writer.write().unwrap();
            mem::swap(old.deref_mut(), new.deref_mut());
        }
    }

    /// 初始化log 文件真实的开始写的位置
    /// stored_offset：从start_offset文件获取的最后写入的值
    fn real_start_offset(file: &File, stored_offset: usize) -> usize {
        let mut real_offset = stored_offset;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(real_offset as u64)).unwrap();
        // 注意，这里已经游标走出4个
        while let Ok(size) = reader.read_u32::<LittleEndian>() {
            if size < Message::mix_len() {
                break;
            }

            let mut data = vec![0u8; size as usize];
            reader.read_exact(&mut data).unwrap();
            if let Some(msg) = Message::deserialize_binary(&mut data, size) {
                info!("重新计算解析消息：{:?}", &msg);
                real_offset += msg.msg_len() as usize;
            };
        }
        real_offset
    }
}


/// commit_log read 对象
pub struct MmapReader {
    file_name: String,
    reader: Mmap,
}
impl MmapReader {
    /// 创建
    fn new(file_name: &str, reader: Mmap) -> MmapReader {
        MmapReader { file_name: file_name.to_string(), reader }
    }
    /// 初始化所有 commit_log 文件的读取对象
    fn init_readers() -> Vec<MmapReader> {
        let log_files = sorted_commit_log_files();
        let mut vec = Vec::<MmapReader>::new();
        if log_files.is_empty() {
            Self::empty_reader_process(&mut vec);
        }
        else {
            Self::not_empty_reader_process(log_files, &mut vec);
        }
        vec
    }

    /// 存在 log 文件的处理方式
    fn not_empty_reader_process(log_files: Vec<DirEntry>, vec: &mut Vec<MmapReader>) {
        log_files.iter().for_each(|ele| {
            let path = file_path().join(ele.file_name().to_str().unwrap());
            match OpenOptions::new().read(true).open(path)
            {
                Ok(file) => {
                    let ele = Self::new(ele.file_name().to_str().unwrap(),
                                        unsafe { MmapOptions::new().map(&file).unwrap() });
                    vec.push(ele);
                }
                Err(err) => {
                    let err = CommitLogError::OpenErr(err.to_string());
                    panic(err.to_string().as_str())
                }
            }
        });
    }

    /// 如果目录中log 文件为空时的处理
    fn empty_reader_process(vec: &mut Vec<MmapReader>) {
        let path = file_path().join(INIT_LOG_FILE_NAME);
        match OpenOptions::new().create(true).read(true).open(path)
        {
            Ok(file) => {
                vec.push(Self::new(INIT_LOG_FILE_NAME,
                                   unsafe { MmapOptions::new().map(&file).unwrap() }
                ));
            }
            Err(err) => {
                let err = CommitLogError::OpenErr(err.to_string());
                panic(err.to_string().as_str())
            }
        }
    }

    /// 根据queue_consume 读取一个消息
    ///
    /// offset  log 文件物理位置偏移
    ///
    /// size    读取的长度
    pub fn read(offset: u64, size: u32) -> Vec<u8> {
        // commit log 文件索引
        let index = (offset / FILE_SIZE) as usize ;
        let reader = MMAP_READERS.get(index).unwrap();

        let start = offset as usize;
        let len = (offset + size as u64) as usize;
        let data = &reader.reader[start..len];
        data.to_vec()
    }
}

fn file_path() -> PathBuf {
    std::env::current_dir()
        .expect("获取应用目录异常")
        .join(DIR_NAME)
}

/// 获取 排序后的 commit_log files
fn sorted_commit_log_files() -> Vec<DirEntry> {
    let mut files = file_util::get_all_files(&file_path());
    files.sort_by_key(|file| file.file_name());
    files
}



#[cfg(test)]

mod tests {
    use crossbeam::atomic::AtomicCell;
    use crate::common::log_util::log_init;
    use crate::storage::commit_log::MMAP_WRITER;
    use crate::storage::message::Message;

    #[test]
    fn test_01_write_message() {
        log_init();
        let json = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"此情可待成追忆\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json).serialize_binary();
        let x = message.as_slice();
        MMAP_WRITER.write(x);

        let json2 = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"只是当时已茫然\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message2 = Message::deserialize_json(&json2).serialize_binary();
        let x2 = message2.as_slice();
        MMAP_WRITER.write(x2);
    }

    #[test]
    fn sys_root_test() {
        let name = AtomicCell::new(String::from("000000"));
        name.swap(String::new());
    }
}
