//! 操作file 的快捷工具类

use crate::cust_error::panic;
use log::error;
use std::fs::{create_dir_all, read_dir, DirEntry};
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

/// 获取指定 PathBuf 下的所有文件夹
///
/// return Vec<DirEntry>
pub fn get_all_dirs(dir: &PathBuf) -> Vec<DirEntry> {
    read_dir(dir)
        .unwrap()
        .map(|f| f.unwrap())
        .filter(|f| f.metadata().unwrap().is_dir())
        .collect::<Vec<_>>()
}

/// 获取工作目录的文件夹
pub fn file_path(dir_name: &str) -> PathBuf {
    let path = std::env::current_dir()
        .expect("获取应用目录异常")
        .join(dir_name);
    if !path.exists() {
        if let Err(e) = create_dir_all(&path) {
            error!("创建文件路径失败：{:?}", e);
            panic(e.to_string().as_str())
        }
    }
    path
}

/// 获取 排序后的 files
pub fn sorted_commit_log_files(dir_name: &str) -> Vec<DirEntry> {
    let mut files = get_all_files(&file_path(dir_name));
    files.sort_by_key(|file| file.file_name());
    files
}

#[cfg(test)]

mod tests {
    use crate::file_util;
    use crate::file_util::get_all_dirs;
    use std::path::PathBuf;
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
        let path = PathBuf::from_str("store/consume_queue").unwrap();
        get_all_dirs(&path).iter().for_each(|e| {
            println!("{:?}", e.file_name().as_os_str());
        });
    }
}
