//! 消息对象

use crate::common::crc_check_util::{crc32, crc_check};
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;

/// 从文件中获取一条消息的方式：
///
/// 根据 读取 一个 u32 的msg_len，然后 读取msg_len长度字节的数据
///
/// 定位一个消息在文件中的起始位置：physical_offset
#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    /// 消息总长度 4，不包括自己
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
    pub fn fix_len() -> u32 {
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
        let mut msg = serde_json::from_str::<Message>(json).unwrap();
        // 设置check_sum
        msg.body_crc = crc32(msg.body.as_bytes());
        msg
    }

    /// 将对象序列化为文件存储的字节编码,使用小端序列化
    pub fn serialize_binary(&self) -> Vec<u8> {
        let mut v = Vec::<u8>::new();
        v.extend(self.msg_len.to_le_bytes());
        v.extend(self.body_crc.to_le_bytes());
        v.extend(self.physical_offset.to_le_bytes());
        v.extend(self.send_timestamp.to_le_bytes());
        v.extend(self.store_timestamp.to_le_bytes());

        v.extend(self.body_len.to_le_bytes());
        v.extend(self.body.as_bytes());

        v.extend(self.topic_len.to_le_bytes());
        v.extend(self.topic.as_bytes());

        v.extend(self.prop_len.to_le_bytes());
        v.extend(self.prop.as_bytes());
        v
    }

    /// 从文件夹中读取一个message出来
    pub fn deserialize_binary(file: &mut File) -> Option<Message> {
        let msg_len = file.read_u32::<LittleEndian>().expect("文件内容非法");
        if msg_len < Message::fix_len() {
            return None;
        }
        let mut buf = Vec::<u8>::with_capacity(msg_len as usize);
        {
            file.by_ref()
                .take(msg_len as u64)
                .read_to_end(&mut buf)
                .unwrap();
        }
        let (body_crc, rest) = buf.split_at(4);
        let body_crc = u32::from_le_bytes(body_crc.try_into().unwrap());

        let (physical_offset, rest) = rest.split_at(8);
        let physical_offset = u64::from_le_bytes(physical_offset.try_into().unwrap());

        let (send_timestamp, rest) = rest.split_at(8);
        let send_timestamp = u64::from_le_bytes(send_timestamp.try_into().unwrap());

        let (store_timestamp, rest) = rest.split_at(8);
        let store_timestamp = u64::from_le_bytes(store_timestamp.try_into().unwrap());

        let (body_len, rest) = rest.split_at(4);
        let body_len = u32::from_le_bytes(body_len.try_into().unwrap());

        let (body, rest) = rest.split_at(body_len as usize);
        crc_check(body_crc, body);
        let body = String::from_utf8_lossy(body).to_string();

        let (topic_len, rest) = rest.split_at(2);
        let topic_len = u16::from_le_bytes(topic_len.try_into().unwrap());

        let (topic, rest) = rest.split_at(topic_len as usize);
        let topic = String::from_utf8_lossy(topic).to_string();

        let (prop_len, rest) = rest.split_at(2);
        let prop_len = u16::from_le_bytes(prop_len.try_into().unwrap());

        let (prop, _) = rest.split_at(prop_len as usize);
        let prop = String::from_utf8_lossy(prop).to_string();

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
}

#[cfg(test)]
mod tests {

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
