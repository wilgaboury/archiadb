pub(crate) type PgIdx = u64;
pub(crate) type InPgIdx = u64;
pub(crate) type PgIdxDisk = U40;
pub(crate) type InPgIdxDisk = U24;

#[repr(C, packed)]
pub(crate) struct U64(u64);

impl U64 {
    pub(crate) fn new(value: u64) -> Self {
        let mut ret = Self(0);
        ret.set(value);
        ret
    }

    pub(crate) fn get(&self) -> u64 {
        u64::from_le(self.0) as u64
    }

    pub(crate) fn set(&mut self, value: u64) {
        self.0 = u64::from_le(value);
    }
}

#[repr(C, packed)]
pub(crate) struct U40([u8; 5]);

impl U40 {
    pub(crate) fn new(value: u64) -> Self {
        let mut ret = Self([0u8; size_of::<Self>()]);
        ret.set(value);
        ret
    }

    pub(crate) fn get(&self) -> u64 {
        let mut buf = [0u8; size_of::<u64>()];
        buf[..size_of::<Self>()].copy_from_slice(&self.0);
        u64::from_le_bytes(buf)
    }

    pub(crate) fn set(&mut self, value: u64) {
        self.0
            .copy_from_slice(&value.to_le_bytes()[..size_of::<Self>()]);
    }
}

#[repr(C, packed)]
pub(crate) struct U32(u32);

impl U32 {
    pub(crate) fn new(value: u64) -> Self {
        let mut ret = Self(0);
        ret.set(value);
        ret
    }

    pub(crate) fn get(&self) -> u64 {
        u32::from_le(self.0) as u64
    }

    pub(crate) fn set(&mut self, value: u64) {
        self.0 = u32::from_le(value as u32);
    }
}

#[repr(C, packed)]
pub(crate) struct U24([u8; 3]);

impl U24 {
    pub(crate) fn new(value: u64) -> Self {
        let mut ret = Self([0u8; size_of::<Self>()]);
        ret.set(value);
        ret
    }

    pub(crate) fn get(&self) -> u64 {
        let mut buf = [0u8; size_of::<u64>()];
        buf[..size_of::<Self>()].copy_from_slice(&self.0);
        u64::from_le_bytes(buf)
    }

    pub(crate) fn set(&mut self, value: u64) {
        self.0
            .copy_from_slice(&value.to_le_bytes()[..size_of::<Self>()]);
    }
}

#[repr(C, packed)]
pub(crate) struct U16(u16);

impl U16 {
    pub(crate) fn new(value: u64) -> Self {
        let mut ret = Self(0);
        ret.set(value);
        ret
    }

    pub(crate) fn get(&self) -> u64 {
        u16::from_le(self.0) as u64
    }

    pub(crate) fn set(&mut self, value: u64) {
        self.0 = u16::from_le(value as u16);
    }
}

#[cfg(test)]
mod test {
    use crate::uint::{U16, U24, U32, U40, U64};

    #[test]
    fn u64() {
        let value = 0x2211FFEEDDCCBBAAu64;
        let mut test = U64::new(0);
        assert_eq!(0, test.get());
        test.set(value);
        assert_eq!(value, test.get());
    }

    #[test]
    fn u40() {
        let value = 0x000000EEDDCCBBAAu64;
        let mut test = U40::new(0);
        assert_eq!(0, test.get());
        test.set(value);
        assert_eq!(value, test.get());
    }

    #[test]
    fn u32() {
        let value = 0x00000000DDCCBBAAu64;
        let mut test = U32::new(0);
        assert_eq!(0, test.get());
        test.set(value);
        assert_eq!(value, test.get());
    }

    #[test]
    fn u24() {
        let value = 0x000CCBBAAu64;
        let mut test = U24::new(0);
        assert_eq!(0, test.get());
        test.set(value);
        assert_eq!(value, test.get());
    }

    #[test]
    fn u16() {
        let value = 0x000000000000BBAAu64;
        let mut test = U16::new(0);
        assert_eq!(0, test.get());
        test.set(value);
        assert_eq!(value, test.get());
    }
}
