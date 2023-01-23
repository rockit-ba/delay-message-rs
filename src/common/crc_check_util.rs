//! crc 32 校验和工具

use crc::{Crc, CRC_32_CKSUM};

const CRC_CKSUM: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);

/// 数据正确性校验
pub fn crc_check(save_crc: u32, data: &[u8]) {
    let cksum = CRC_CKSUM.checksum(data);
    if cksum != save_crc {
        panic!("CRC check failed: curr: {cksum}, old: {save_crc}");
    }
}

/// 获取数据的crc
pub fn crc32(bytes: &[u8]) -> u32 {
    CRC_CKSUM.checksum(bytes)
}
