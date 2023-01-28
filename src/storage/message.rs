//! 消息对象

use crate::common::data_process_util::{crc32, crc_check};
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{BufReader, Read};
use std::time::SystemTime;

/// 从文件中获取一条消息的方式：
///
/// 根据 读取 一个 u32 的msg_len，然后 读取msg_len长度字节的数据
///
/// 定位一个消息在文件中的起始位置：physical_offset
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    /// 消息总长度 4，不包括自身的4字节
    msg_len: u32,
    /// 校验和 4
    body_crc: u32,
    /// 在log 文件中的偏移量，物理偏移量 8
    pub physical_offset: u64,
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
    pub topic: String,
    /// 消息属性长度 2
    prop_len: u16,
    /// 消息属性 <_delay-10>
    pub prop: String,
}

impl Message {
    /// 消息固定长度大小
    pub fn mix_len() -> u32 {
        40
    }

    /// 消息总大小
    pub fn msg_len(&self) -> u32 {
        self.msg_len + 4
    }

    /// 序列化为 JSON
    pub fn serialize_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// 将客户端网络传输的JSON 反序列化为 message
    pub fn deserialize_json(json: &str) -> Self {
        serde_json::from_str::<Message>(json).unwrap()
    }

    /// 将对象序列化为文件存储的字节编码,使用小端序列化
    pub fn serialize_binary(&self) -> Vec<u8> {
        let mut v = Vec::<u8>::new();
        v.extend(self.msg_len.to_le_bytes());
        // 设置check_sum
        v.extend(crc32(self.body.as_bytes()).to_le_bytes());
        v.extend(self.physical_offset.to_le_bytes());
        v.extend(self.send_timestamp.to_le_bytes());
        v.extend(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().to_le_bytes());

        v.extend(self.body_len.to_le_bytes());
        v.extend(self.body.as_bytes());

        v.extend(self.topic_len.to_le_bytes());
        v.extend(self.topic.as_bytes());

        v.extend(self.prop_len.to_le_bytes());
        v.extend(self.prop.as_bytes());
        v
    }

    /// 从文件夹中读取一个message出来
    pub fn deserialize_binary(data: &mut Vec<u8>, msg_len: u32) -> Option<Message> {
        let mut reader = BufReader::new(data.as_slice());
        let body_crc = reader.read_u32::<LittleEndian>().unwrap();
        let physical_offset = reader.read_u64::<LittleEndian>().unwrap();
        let send_timestamp = reader.read_u64::<LittleEndian>().unwrap();
        let store_timestamp = reader.read_u64::<LittleEndian>().unwrap();
        let (body_len, body) = Self::deserialize_binary_body(&mut reader, body_crc);
        let (topic_len, topic) = Self::deserialize_binary_topic(&mut reader);
        let (prop_len, prop) = Self::deserialize_binary_prop(&mut reader);

        Some(Message {
            msg_len,
            body_crc,
            physical_offset,
            send_timestamp,
            store_timestamp,
            body_len,
            body,
            topic_len,
            topic,
            prop_len,
            prop,
        })
    }

    // body 处理
    fn deserialize_binary_body(reader: &mut BufReader<&[u8]>, body_crc: u32) -> (u32, String) {
        let body_len = reader.read_u32::<LittleEndian>().unwrap();
        let mut body = vec![0u8; body_len as usize];
        reader.read_exact(&mut body).unwrap();
        crc_check(body_crc, body.as_slice());
        let body = String::from_utf8_lossy(body.as_slice()).to_string();
        (body_len, body)
    }

    // topic 处理
    fn deserialize_binary_topic(reader: &mut BufReader<&[u8]>) -> (u16, String) {
        let topic_len = reader.read_u16::<LittleEndian>().unwrap();
        let mut topic = vec![0u8; topic_len as usize];
        reader.read_exact(&mut topic).unwrap();
        let topic = String::from_utf8_lossy(topic.as_slice()).to_string();
        (topic_len, topic)
    }

    // prop 处理
    fn deserialize_binary_prop(reader: &mut BufReader<&[u8]>) -> (u16, String) {
        let prop_len = reader.read_u16::<LittleEndian>().unwrap();
        let mut prop = vec![0u8; prop_len as usize];
        reader.read_exact(&mut prop).unwrap();
        let prop = String::from_utf8_lossy(prop.as_slice()).to_string();
        (prop_len, prop)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;
    use crate::common::log_util::log_init;
    use crate::storage::message::Message;
    use log::info;

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
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        println!("timestamp: {timestamp}");
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
