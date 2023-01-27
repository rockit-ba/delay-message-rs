//! 用于构建 commit_log 数据管理,加快消息消费

use crossbeam::channel::{Sender,Receiver,unbounded};
use std::ops::Deref;
use lazy_static::lazy_static;
use log::info;
use tokio::time::{Instant,sleep_until, Duration};
use crate::data_process_util::hashcode;

lazy_static! {
    pub static ref NOW: Instant = Instant::now();
    pub static ref MSG_PROCESS_LISTNER: (Sender<QueueMessage>, Receiver<QueueMessage>) = unbounded();
}

/// commit_log 索引数据
#[derive(Debug,Clone)]
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
    pub fn new(physical_offset: u64, size: u32, tag: &str, delay_time: u32) -> Self {
        QueueMessage {
            physical_offset,
            size,
            tag_hashcode: hashcode(&tag),
            delay_time,
        }
    }

    /// 生成 阻塞时间
    pub async fn sleep_until(&self) {
        let copy = self.clone();
        let (s, _) = MSG_PROCESS_LISTNER.deref();
        tokio::spawn( async move{
            sleep_until(*NOW + Duration::from_secs(copy.delay_time as u64)).await;
            s.send(copy).unwrap();
        });
    }
}

/// 初始化延迟消息
pub async fn init() {
    init_message().await;
    process_message().await;
}
/// 从磁盘反序列化出 queue_message ，并发送处理消息
async fn init_message() {
    for i in 0..1_00 {
        let task_01 = QueueMessage::new(i as u64, i as u32, "", i as u32);
        task_01.sleep_until().await;
    }
}

/// 处理所有的延迟消息
async fn process_message() {
    tokio::spawn(async move {
        let (_, r) = MSG_PROCESS_LISTNER.deref();
        loop {
            if let Ok(ele) = r.recv() {
                info!("{ele:?}");
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
