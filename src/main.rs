use std::fs::{OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use memmap2::{MmapMut};

fn main() {
    let path: PathBuf = PathBuf::from("111111");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    // 从指定位置写入
    let mut x = &mut mmap[3..];
    x.write_all(b"abc").unwrap();
    x.write_all(b"abc").unwrap();
    mmap.flush().unwrap();
}
