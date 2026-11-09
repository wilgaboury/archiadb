use anyhow::Result;

use crate::{
    bio::BlockIo,
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
    use anyhow::Result;
    use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};

    use crate::{
        bio::{BlockIoMemory, BlockIoRapidHash},
        bll,
        defer::Defer,
        flux::Flux,
        test::init_basic_mem_db,
        uint::PgIdx,
        uring::MIN_PAGE_SIZE,
    };

    #[tokio::test]
    async fn fuzz_block_linked_list() -> Result<()> {
        const BIO_SIZE: PgIdx = 2000;
        const ITERS: usize = 100;
        const MIN_VAL_SIZE: PgIdx = 2 * MIN_PAGE_SIZE;
        const MAX_VAL_SIZE: PgIdx = 8 * MIN_PAGE_SIZE;

        let mut rng = StdRng::seed_from_u64(88369992);

        for _ in 0..ITERS {
            let bio = BlockIoRapidHash::new(BlockIoMemory::new(MIN_PAGE_SIZE));
            let defer = Defer::new();
            let mut test_ctx = init_basic_mem_db(&bio, BIO_SIZE, &defer).await?;
            let mut flux = Flux::new(bio.clone());

            let size = rng.gen_range(MIN_VAL_SIZE..=MAX_VAL_SIZE);
            let mut buffer = vec![0u8; size as usize];
            rng.fill_bytes(&mut buffer);

            let mut ctx = test_ctx.ctx();
            let pg_idx = bll::create(&mut ctx, &mut flux, &buffer).await?;
            let mut read_buffer = vec![0u8; size as usize];
            bll::read(&mut flux, pg_idx, &mut read_buffer).await?;
            assert_eq!(buffer, read_buffer);
        }

        Ok(())
    }
}
