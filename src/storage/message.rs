//! 消息对象

use serde::{Serialize, Deserialize};
/// 从文件中获取一条消息的方式：
///
/// 根据 读取 一个 u32 的msg_len，然后 读取msg_len长度字节的数据
///
/// 定位一个消息在文件中的起始位置：physical_offset
#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    /// 消息总长度 4
    msg_len: u32,
    /// 校验和 4
    body_crc: u32,
    /// 在log 文件中的偏移量，物理偏移量 8
    physical_offset: u64,
    /// 消息在客户端发送的时间戳 8
    send_timestamp: u64,
    /// 消息在服务端存储的时间戳 8
    store_timestamp: u64,
    /// 消息体的长度 4
    body_len: u32,
    /// 消息体内容
    body: String,
    /// topic的长度 2
    topic_len: u16,
    /// topic
    topic: String,
    /// 消息属性长度 2
    prop_len: u16,
    /// 消息属性
    prop: String,
}

impl Message {
    /// 消息固定长度大小
    pub fn fix_len() -> u32{
        40
    }

    /// 序列化为 JSON
    pub fn serialize_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// 反序列化为 message
    pub fn deserialize_json(json: &str) -> Self {
        serde_json::from_str::<Message>(json).unwrap()
    }
}

#[cfg(test)]
mod tests {

    use log::{info};
    use crate::common::log_util::{log_init};
    use crate::storage::message::Message;

    #[test]
    fn test_json() {
        log_init();
        let json = String::from("{\"msg_len\":40,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":0,\"body\":\"\",\"topic_len\":0,\"topic\":\"\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json);
        info!("{:?}", message);

        let string = message.serialize_json();
        info!("{:?}", string);
    }

    #[test]
    fn test_word_len() {
        log_init();
        let str = "topic_oms";
        info!("长度-{}", str.as_bytes().len());
    }

    #[test]
    fn test_byte() {
        log_init();
        let json = String::from("{\"msg_len\":40,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"此情可待成追忆\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json);
        let serialized = bincode::serialize(&message).unwrap();
        info!("长度：{}", serialized.len());
    }
}