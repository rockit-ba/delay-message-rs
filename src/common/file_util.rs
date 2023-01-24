//! 操作file 的快捷工具类

use std::fs::{read_dir, DirEntry};
use std::path::PathBuf;

/// 获取指定 PathBuf 下的所有文件
///
/// return Vec<DirEntry>
pub fn get_all_files(dir: &PathBuf) -> Vec<DirEntry> {
    read_dir(dir)
        .unwrap()
        .map(|f| f.unwrap())
        .collect::<Vec<_>>()
}

#[cfg(test)]

mod tests {
    use crate::file_util;
    use std::str::FromStr;

    #[test]
    fn test_get_all_files() {
        let path = std::env::current_dir()
            .expect("获取应用程序目录异常")
            .join("store/commit_log");
        let sort = file_util::get_all_files(&path)
            .iter()
            .map(|ele| u64::from_str(ele.file_name().to_str().unwrap()).unwrap())
            .collect::<Vec<_>>();
        if !sort.is_empty() {
            println!("{}", sort.last().unwrap());
        }
    }

    #[test]
    fn trans_test() {
        let i = u64::from_str("0").unwrap();
        println!("{i}")
    }
}
