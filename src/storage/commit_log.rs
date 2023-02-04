//! commit_log 文件模块


use memmap2::{Mmap, MmapOptions};
use std::fs::{DirEntry, OpenOptions};
use std::io::{Write};
use std::str::FromStr;
use crate::cust_error::{panic, MmapError};
use crate::storage::start_offset;

use lazy_static::lazy_static;
use log::{info};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use crate::common::config::CONFIG;
use crate::file_util::{file_path, sorted_commit_log_files};
use crate::storage::message::Message;
use crate::storage::mmap::{MmapWriter};

/// 第一个存储文件的名称
const INIT_LOG_FILE_NAME: &str = "00000000000000000000";
/// 文件存储目录
const DIR_NAME: &str = "store/commit_log";
/// 存储映射引用
static mut MMAP_WRITER: Option<CommitLogWriter> = None;

lazy_static! {
    static ref MMAP_READERS: Vec<MmapReader> = MmapReader::init_readers();
}

/// 创建 mpsc 写入通道，返回发送者
pub fn mpsc_channel() -> UnboundedSender<Message>{
    let (tx,mut rx) = mpsc::unbounded_channel::<Message>();
    tokio::spawn(async move {
        info!("commit_log write 监听初始化");
        while let Some(ele) = rx.recv().await {
            info!("收到 写入消息 {ele:?}");
            // 返回请求成功
            CommitLogWriter::instance().write(ele.serialize_binary().as_slice());
            // todo 发送到consume_queue进行索引存储
        }
    });
    tx
}

/// commit_log 写对象
///
/// 此对象利用mpsc进行操作，因为避免写入时使用锁竞争
type CommitLogWriter = MmapWriter;

impl CommitLogWriter {
    /// 单例获取 START_OFFSET
    pub fn instance() -> &'static mut CommitLogWriter {
        unsafe {
            if MMAP_WRITER.is_none() {
                MMAP_WRITER = Some(Self::self_new(None));
            }
            MMAP_WRITER.as_mut().unwrap()
        }
    }
    /// 创建实例
    ///
    fn self_new(file_name: Option<&str>) -> Self{
        Self::new(
            file_name,
            INIT_LOG_FILE_NAME,
            DIR_NAME,
            start_offset::read(),
            CONFIG.commit_log_file_size
        )
    }

    /// 写数据
    pub fn write(&mut self, data: &[u8]) {
        let mut m_mut = &mut self.writer[self.prev_write_size..];

        info!("当前文件[{}]剩余：{},当前数据大小：{}", self.file_name, m_mut.len(), data.len());
        if m_mut.len() > data.len() {
            m_mut.write_all(data).unwrap();
            self.prev_write_size += data.len();
            start_offset::write(self.prev_write_size as u64);
            return;
        }

        self.new_writer_create();
        self.write(data);
    }

    /// 当前commit_log文件已满，开始创建新的文件
    fn new_writer_create(&mut self) {
        let curr = u64::from_str(self.file_name.as_str()).unwrap();
        info!("当前commit_log文件[{}]已满，开始创建新的文件", self.file_name);
        // TODO 新文件还原为0，这里要可能需要兼容
        start_offset::write(0);

        let new_name = format!("{number:>0width$}", number = curr + CONFIG.commit_log_file_size, width = 20);
        let new_writer = Self::self_new(Some(new_name.as_str()));
        // 还原当前文件参数
        self.prev_write_size = 0;
        self.file_name = new_name;
        self.writer = new_writer.writer;

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
        let log_files = sorted_commit_log_files(DIR_NAME);
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
            let path = file_path(DIR_NAME).join(ele.file_name().to_str().unwrap());
            match OpenOptions::new().read(true).open(path)
            {
                Ok(file) => {
                    let ele = Self::new(ele.file_name().to_str().unwrap(),
                                        unsafe { MmapOptions::new().map(&file).unwrap() });
                    vec.push(ele);
                }
                Err(err) => {
                    let err = MmapError::OpenErr(err.to_string());
                    panic(err.to_string().as_str())
                }
            }
        });
    }

    /// 如果目录中log 文件为空时的处理
    fn empty_reader_process(vec: &mut Vec<MmapReader>) {
        let path = file_path(DIR_NAME).join(INIT_LOG_FILE_NAME);
        match OpenOptions::new().create(true).read(true).open(path)
        {
            Ok(file) => {
                vec.push(Self::new(INIT_LOG_FILE_NAME,
                                   unsafe { MmapOptions::new().map(&file).unwrap() }
                ));
            }
            Err(err) => {
                let err = MmapError::OpenErr(err.to_string());
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
        let index = (offset / CONFIG.commit_log_file_size) as usize ;
        let reader = MMAP_READERS.get(index).unwrap();

        let start = offset as usize;
        let len = (offset + size as u64) as usize;
        let data = &reader.reader[start..len];
        data.to_vec()
    }
}







#[cfg(test)]

mod tests {
    use crossbeam::atomic::AtomicCell;
    use crate::common::log_util::log_init;
    use crate::storage::commit_log::CommitLogWriter;
    use crate::storage::message::Message;

    #[test]
    fn test_01_write_message() {
        log_init();
        let writer = CommitLogWriter::instance();
        let json = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"此情可待成追忆\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json).serialize_binary();
        let x = message.as_slice();
        writer.write(x);

        let json2 = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"只是当时已茫然\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message2 = Message::deserialize_json(&json2).serialize_binary();
        let x2 = message2.as_slice();
        writer.write(x2);
    }

    #[test]
    fn sys_root_test() {
        let name = AtomicCell::new(String::from("000000"));
        name.swap(String::new());
    }
}
