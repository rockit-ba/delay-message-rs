//! 自定义异常
use log::error;
use thiserror::Error;

const COMMIT_LOG: &str = "commit_log";

pub fn panic(err: &str) -> ! {
    error!("{err}");
    panic!("{err}")
}

#[derive(Error, Debug)]
pub enum MmapError {
    // #[error("Invalid header (expected {expected:?}, got {found:?})")]
    // InvalidHeader {
    //     expected: String,
    //     found: String,
    // },
    // #[error("Missing attribute: {0}")]
    // MissingAttribute(String),
    #[error("文件open失败: {0}")]
    OpenErr(String),

    #[error("文件初始大小设置异常: {0}")]
    SetLenErr(String),

    #[error("虚拟内存映射初始化异常: {0}")]
    MmapErr(String),
}
