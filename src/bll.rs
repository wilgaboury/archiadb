use anyhow::Result;

use crate::{
    block::BlockIo,
    flux::Flux,
    lalloc::{LallocCtx, lalloc},
    uint::{PgIdx, PgIdxDisk},
    util::{ChecksumDisk, from_bytes, from_bytes_mut},
};

pub(crate) async fn create<'a, 'b, B: BlockIo>(
    ctx: &mut LallocCtx<'a, 'b, B>,
    flux: &mut Flux<B>,
    value: &[u8],
) -> Result<u64> {
    let chunk_size =
        ctx.meta.page_size() as usize - size_of::<PgIdxDisk>() - size_of::<ChecksumDisk>();
    let mut prev_pg_idx: PgIdx = 0;
    for chunk in value.chunks(chunk_size).rev() {
        let mut buf = flux.buf();
        let mbuf = buf.as_mut();
        let len = chunk.len();
        mbuf[..len].copy_from_slice(chunk);
        from_bytes_mut::<PgIdxDisk>(&mut mbuf[len..len + size_of::<PgIdxDisk>()]).set(prev_pg_idx);
        let pg_idx = flux.write(buf, || lalloc(ctx)).await?;
        prev_pg_idx = pg_idx;
    }
    Ok(prev_pg_idx)
}

pub(crate) async fn read<B: BlockIo>(
    flux: &mut Flux<B>,
    mut pg_idx: u64,
    buf: &mut [u8],
) -> Result<()> {
    let mut empty = buf.len();
    while empty > 0 {
        let pg = flux.read(pg_idx).await?;
        let pg_buf = pg.as_ref();
        let len = pg_buf.len() - size_of::<PgIdxDisk>() - size_of::<ChecksumDisk>();
        let cp_len = std::cmp::min(len, empty);
        let cp_start = buf.len() - empty;
        buf[cp_start..cp_start + cp_len].copy_from_slice(&pg_buf[0..cp_len]);
        empty -= cp_len;
        pg_idx = from_bytes::<PgIdxDisk>(&pg_buf[len..len + size_of::<PgIdxDisk>()]).get();
    }
    Ok(())
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use std::f32::MIN;

    use anyhow::Result;

    use crate::{bio::MIN_PAGE_SIZE, block::BlockIoMemory, lalloc::LallocCtx};

    #[named]
    #[tokio::test]
    async fn test_linked_list() -> Result<()> {
        let bio = BlockIoMemory::new(MIN_PAGE_SIZE);

        let dir = TmpDir::new(function_name!())?;
        let db = dir.db().await?;
        let mut txn = db.txn().begin().await;
        let value_len = txn.db.meta.page_size() as usize * 2.5 as usize;
        let value = vec![1u8; value_len];
        let mut value_test = vec![0u8; value_len];
        let pg_idx = txn.create_value_linked_list(&value).await?;
        txn.read_value_linked_list(pg_idx, &mut value_test).await?;
        assert_eq!(value, value_test);
        Ok(())
    }
}
