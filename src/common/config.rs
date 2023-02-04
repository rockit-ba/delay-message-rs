//! 配置文件

use std::fs::File;
use lazy_static::lazy_static;
use serde::{Serialize, Deserialize};

/// 配置文件路径
const CONF_PATH: &str = "conf.yaml";

lazy_static! {
    /// 配置文件
    pub static ref CONFIG: Config = Config::new();
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// commit_log 每个file的大小
    pub commit_log_file_size: u64,
    /// 最大延迟时间
    pub max_delay_time: u32,
    /// consume_queue 每个file的大小
    pub consume_queue_file_size: u64,
}

impl Config {
    pub fn new() -> Self{
        let file = File::options().read(true).open(CONF_PATH).unwrap();
        serde_yaml::from_reader(&file).expect("初始化配置文件失败")
    }

}

#[cfg(test)]
mod tests {
    use crate::common::config::Config;

    #[test]
    fn test_config() {
        let config = Config::new();
        println!("{config:?}");
    }
}