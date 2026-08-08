type PgIdx = U40;

type PgByteIdx = U24;

#[repr(C, packed)]
struct U40([u8; 5]);

impl U40 {
    pub fn get(&self) -> u64 {
        let mut buf = [0u8; size_of::<u64>()];
        buf[..5].copy_from_slice(&self.0);
        u64::from_le_bytes(buf)
    }

    pub fn set(&mut self, value: u64) {
        self.0
            .copy_from_slice(&value.to_le_bytes()[..size_of::<Self>()]);
    }
}

#[repr(C, packed)]
struct U24([u8; 3]);

impl U24 {
    pub fn get(&self) -> u32 {
        let mut buf = [0u8; size_of::<u32>()];
        buf[..3].copy_from_slice(&self.0);
        u32::from_le_bytes(buf)
    }

    pub fn set(&mut self, value: u32) {
        self.0
            .copy_from_slice(&value.to_le_bytes()[..size_of::<Self>()]);
    }
}
