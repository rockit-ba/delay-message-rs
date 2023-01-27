//! 用于构建 commit_log 数据管理,加快消息消费

use crossbeam::channel::{Sender,Receiver,bounded};
use std::cmp::Ordering;
use std::ops::Deref;
use lazy_static::lazy_static;
use log::info;
use rayon::prelude::ParallelSliceMut;
use tokio::time::{Instant, Sleep, sleep_until, Duration};
use crate::data_process_util::hashcode;

lazy_static! {
    pub static ref NOW: Instant = Instant::now();
    pub static ref MSG_PROCESS_LISTNER: (Sender<()>, Receiver<()>) = bounded(1);
}

/// commit_log 索引数据
#[derive(Debug)]
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
    pub fn sleep_until(&self) -> Sleep {
        sleep_until(*NOW + Duration::from_secs(self.delay_time as u64))
    }

    /// 根据 延迟时间倒叙排序
    pub fn delay_time_desc() -> fn(&QueueMessage, &QueueMessage) -> Ordering {
        |a, b| b.delay_time.partial_cmp(&a.delay_time).unwrap()
    }
}

/// 初始化延迟消息
pub async fn init() {
    let list = init_message();
    process_message(list).await;
}
/// 从磁盘反序列化出 queue_message ，并发送处理消息
fn init_message() -> Vec<QueueMessage> {
    let mut list = Vec::<QueueMessage>::with_capacity(5);
    let task_01 = QueueMessage::new(1_u64, 1_u32, "task_01", 1_u32);
    let task_02 = QueueMessage::new(2_u64, 2_u32, "task_02", 2_u32);
    let task_03 = QueueMessage::new(3_u64, 3_u32, "task_03", 3_u32);
    let task_04 = QueueMessage::new(4_u64, 4_u32, "task_04", 4_u32);
    let task_05 = QueueMessage::new(5_u64, 5_u32, "task_05", 5_u32);
    list.push(task_05);
    list.push(task_01);
    list.push(task_04);
    list.push(task_02);
    list.push(task_03);
    // 数量过多应该分散到多个list中分别处理
    list.par_sort_by(QueueMessage::delay_time_desc());
    let (s, _) = MSG_PROCESS_LISTNER.deref();
    s.send(()).unwrap();
    list
}

/// 处理所有的延迟消息
async fn process_message(mut list: Vec<QueueMessage>) {
    tokio::spawn(async move {
        let (_, r) = MSG_PROCESS_LISTNER.deref();
        loop {
            if r.recv().is_ok() {
                while let Some(ele) = list.pop() {
                    ele.sleep_until().await;
                    info!("{ele:?}");
                }
            }
        }
    }).await.unwrap();
}




#[cfg(test)]
mod tests {
    use crate::consume_queue::QueueMessage;

    #[tokio::test]
    async fn delay_queue() {
        let mut list = Vec::<QueueMessage>::with_capacity(5);
        let task_01 = QueueMessage::new(1_u64,1_u32,"task_01", 1_u32);
        let task_02 = QueueMessage::new(2_u64,2_u32,"task_02", 2_u32);
        let task_03 = QueueMessage::new(3_u64,3_u32,"task_03", 3_u32);
        let task_04 = QueueMessage::new(4_u64,4_u32,"task_04", 4_u32);
        let task_05 = QueueMessage::new(5_u64,5_u32,"task_05", 5_u32);
        list.push(task_05);
        list.push(task_01);
        list.push(task_04);
        list.push(task_02);
        list.push(task_03);
        list.sort_by_key(|ele| ele.delay_time);
        list.iter().for_each(|ele| {
            println!("{ele:?}");
        });

    }
}
