//! crc 32 校验和工具

use crc::{Crc, CRC_32_CKSUM};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const CRC_CKSUM: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);

/// 数据正确性校验
pub fn crc_check(save_crc: u32, data: &[u8]) {
    let ck_sum = CRC_CKSUM.checksum(data);
    if ck_sum != save_crc {
        panic!("CRC check failed: curr: {ck_sum}, old: {save_crc}");
    }
}

/// 获取数据的crc
pub fn crc32(bytes: &[u8]) -> u32 {
    CRC_CKSUM.checksum(bytes)
}

/// 获取hash_code
pub fn hashcode<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
