//! 用于构建 commit_log 数据管理,加快消息消费

use crate::common::config::CONFIG;
use crate::data_process_util::hashcode;
use crate::file_util::{file_path, get_all_dirs};
use crate::storage::message::Message;
use crate::storage::mmap::MmapWriter;
use lazy_static::lazy_static;
use log::{info, warn};
use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration;
use byteorder::{LittleEndian, WriteBytesExt};
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::{watch, RwLock};
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;

/// 第一个存储文件的名称
const INIT_LOG_FILE_NAME: &str = "00000000000000000000";
/// 文件存储目录，最终的目录还需要拼接对应topic的名称
///
/// |consume_queue
///     |topic_test
///         |filename
const BASE_DIR_NAME: &str = "store/consume_queue";

lazy_static! {
    /// topic区分的writer key 就是 topic
    static ref WRITERS: RwLock<HashMap<String, ConsumeQueueWriter>> = {
        RwLock::new(writers_init())
    };
    /// 存放延迟消息的队列
    static ref DELAY_QUEUE: RwLock<DelayQueue<QueueMessage>> = {
        let mut queue = DelayQueue::<QueueMessage>::with_capacity(1024);
        let (block,duration) = QueueMessage::block_message();
        // 设置阻塞元素
        queue.insert(block, duration);
        RwLock::new(queue)
    };

    /// 传递过期消息的 channel
    static ref ESCAPE_CHANNEL: Sender<QueueMessage> = {
        let (tx, mut rx) = watch::channel(QueueMessage::default());
        // TODO 根据tag_hashcode 创建对应的 接收者，然后将消息存放到对应的队列
        let mut r2 = rx.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                info!("topic 1 收到到期消息 ： {:?}", *rx.borrow());
            }
        });

        tokio::spawn(async move {
            while r2.changed().await.is_ok() {
                info!("topic 2 收到到期消息 ： {:?}", *r2.borrow());
            }
        });
        tx
    };

}

type ConsumeQueueWriter = MmapWriter;
impl ConsumeQueueWriter {
    /// 创建当前的实例
    /// dir_name 是base_dir_name/topic
    fn consume_queue_new(file_name: Option<&str>, dir_name: &str) -> Self {
        Self::new(
            file_name,
            INIT_LOG_FILE_NAME,
            dir_name,
            None,
            CONFIG.consume_queue_file_size,
        )
    }

    /// 写数据
    fn consume_queue_write(&mut self, data: &[u8]) {
        let mut buf = &mut self.writer[self.prev_write_size..];

        info!(
            "当前 consume_queue 文件[{}]剩余：{},当前数据大小：{}",
            self.file_name,
            buf.len(),
            data.len()
        );
        // 这里-8的原因是最后8个字节存储当前写入的位置
        if buf.len()-8 < data.len() {
            self.consume_queue_new_writer_create();
            self.consume_queue_write(data);
            return;
        }
        buf.write_all(data).unwrap();
        self.prev_write_size += data.len();
        // 存储写入的位置
        let end = buf.len()-8;
        let mut start_offset_buf = &mut buf[end..];
        start_offset_buf.write_u64::<LittleEndian>(self.prev_write_size as u64).unwrap();
    }

    /// 当前commit_log文件已满，开始创建新的文件
    fn consume_queue_new_writer_create(&mut self) {
        let curr = u64::from_str(self.file_name.as_str()).unwrap();
        info!(
            "当前 consume_queue 文件[{}]已满，开始创建新的文件",
            self.file_name
        );

        let new_name = format!(
            "{number:>0width$}",
            number = curr + CONFIG.consume_queue_file_size,
            width = 20
        );
        let new_writer = Self::consume_queue_new(Some(new_name.as_str()),"");
        self.new_writer_create(&new_name, new_writer);
    }
}

fn writers_init() -> HashMap<String, ConsumeQueueWriter> {
    let mut map = HashMap::<String, ConsumeQueueWriter>::with_capacity(1024);
    let path = file_path(BASE_DIR_NAME);
    get_all_dirs(&path).iter().for_each(|ele| {
        let key = ele.file_name().to_str().unwrap().to_string();
        let dir_name = format!("{BASE_DIR_NAME}/{key}");
        let writer = ConsumeQueueWriter::consume_queue_new(None, &dir_name);
        info!("构建 consume_queue_writer：{:?}", writer);
        map.insert(key, writer);
    });
    map
}
// pub fn send(message: &Message){
//     let (tx, rx) = spmc_channel();
//     //
//
// }

/// 最优的可能是spsc,但是那样可能会相对复杂，
///
/// 把消息是否处理放在了发送端，因此每次发送消息的时候需要找到对应的发送者
///
/// spmc简单一些，因为事实上不会有过多的topic
fn spmc_channel() -> (Sender<Message>, Receiver<Message>) {
    // 注意，这里的
    let (tx, rx) = watch::channel(Message::default());
    let b = rx.clone();
    // 根据消息topic 查询是否有对应的receiver，如果没有则创建
    let mut a = rx.clone();
    tokio::spawn(async move {
        while a.changed().await.is_ok() {
            info!(
                "topic xx 收到持久化 consume_queue的消息 ： {:?}",
                *rx.borrow()
            );
        }
    });
    (tx, b)
}

/// commit_log 索引数据
#[derive(Debug, Clone, Default)]
pub struct QueueMessage {
    // commit_log 物理偏移量 8
    physical_offset: u64,
    // 数据大小 4
    size: u32,
    // tag  的hash_code 8
    tag_hashcode: u64,
    // 最长支持一年  31_536_000  秒 4
    pub delay_time: u32,
}
impl QueueMessage {
    /// 定长长度 1G 内存可以存储 4473_9242条数据
    pub fn len() -> u16 {
        24_u16
    }
    /// 根据 commit_log message 构建一个 QueueMessage
    pub fn from_message(message: &Message) -> (Self, Duration) {
        let prop = message.prop.clone();
        let delay_time = prop.split('-').collect::<Vec<_>>();
        QueueMessage::new(
            message.physical_offset,
            message.msg_len(),
            &message.topic,
            u32::from_str(delay_time.get(1).unwrap()).unwrap(),
        )
    }

    ///  创建消息
    pub fn new(physical_offset: u64, size: u32, tag: &str, delay_time: u32) -> (Self, Duration) {
        let message = QueueMessage {
            physical_offset,
            size,
            tag_hashcode: hashcode(&tag),
            delay_time,
        };
        let time = message.duration();
        (message, time)
    }

    /// 无效的延迟消息，用于阻塞循环
    fn block_message() -> (Self, Duration) {
        let message = QueueMessage {
            physical_offset: 0,
            size: 0,
            tag_hashcode: 0,
            delay_time: CONFIG.max_delay_time,
        };
        let time = message.duration();
        (message, time)
    }

    /// 是否是阻塞的无效消息
    fn is_block_message(&self) -> bool {
        self.size == 0
    }

    fn duration(&self) -> Duration {
        let n = self.delay_time;
        Duration::from_secs(n as u64)
    }
}

/// 初始化延迟消息
pub async fn init() {
    init_message().await;
    process_message().await;
}
/// 从磁盘反序列化出 queue_message ，初始化到延迟队列
async fn init_message() {
    for r in 1..10 {
        let (task_01, duration1) = QueueMessage::new(r as u64, r as u32, "", r as u32);
        {
            DELAY_QUEUE.write().await.insert(task_01, duration1);
        }
    }
}

/// 处理所有的延迟消息
async fn process_message() {
    tokio::spawn(async move {
        loop {
            let mut queue = DELAY_QUEUE.write().await;
            if let Some(ele) = queue.next().await {
                // 在这里处理取出的元素
                let msg = ele.get_ref();
                if msg.is_block_message() {
                    let (block, duration) = QueueMessage::block_message();
                    warn!("无效阻塞消息消费：{msg:?}，将重新赋值：{block:?}");
                    queue.insert(block, duration);
                    return;
                }
                info!("消息过期：{msg:?}");

                ESCAPE_CHANNEL.send(ele.into_inner()).unwrap();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use crate::consume_queue::writers_init;
    use crate::log_util::log_init;

    #[tokio::test]
    async fn delay_queue() {}

    #[test]
    fn test_init_writers() {
        log_init();
        writers_init();
    }
}
