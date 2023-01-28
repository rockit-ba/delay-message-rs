//! 用于构建 commit_log 数据管理,加快消息消费

use std::time::Duration;
use lazy_static::lazy_static;
use log::{info, warn};
use tokio::sync::{RwLock, watch};
use tokio::sync::watch::{Sender};
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;
use crate::data_process_util::hashcode;

lazy_static! {
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
        // TODO 根据topic 创建对应的 接收者，然后将消息存放到对应的队列
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                info!("收到到期消息 ： {:?}", *rx.borrow());
            }
        });
        tx
    };

}

/// commit_log 索引数据
#[derive(Debug,Clone,Default)]
pub struct QueueMessage {
    // commit_log 物理偏移量
    physical_offset: u64,
    // 数据大小
    size: u32,
    // tag  的hash_code
    tag_hashcode: u64,
    // 最长支持一年  31_536_000  秒
    pub delay_time : u32,
}
impl QueueMessage {
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
            delay_time: 31_536_000,
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
    tokio::spawn(async move{
        loop {
            let mut queue = DELAY_QUEUE.write().await;
            if let Some(ele) = queue.next().await {
                // 在这里处理取出的元素
                let msg = ele.get_ref();
                if msg.is_block_message() {
                    let (block,duration) = QueueMessage::block_message();
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

    #[tokio::test]
    async fn delay_queue() {

    }
}
