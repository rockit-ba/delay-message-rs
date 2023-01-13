

#[cfg(test)]

mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use memmap2::{MmapMut};

    #[test]
    fn test_01() {
        let file = File::create("111111").unwrap();
        file.set_len(1024).unwrap();
    }

    #[test]
    fn test_02() {
        let path: PathBuf = PathBuf::from("111111");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let mut x = &mut mmap[..];
        x.write_all(b"abc").unwrap();
    }
    #[test]
    fn test_03() {
        let path: PathBuf = PathBuf::from("111111");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path).unwrap();
        let mut map = unsafe { MmapMut::map_mut(&file).unwrap() };
        // 从指定位置写入
        let mut x = &mut map[3..];
        x.write_all(b"efg").unwrap();
        x.write_all(b"hijk").unwrap();
        map.flush().unwrap();
    }
}