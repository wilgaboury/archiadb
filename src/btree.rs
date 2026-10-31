use std::cmp::Ordering;

use anyhow::{Result, bail};

use crate::{
    block::BlockIo,
    bnode::{
        BNodeBuf, BNodeHeader, BNodeKind, BNodeRootHeader, LeafValue, LeafValueGetResult,
        SearchResult, SlotDisk, get_key_inner, get_key_leaf, get_page_ptr, get_value_leaf,
        in_pg_len, in_pg_size_of, insert_at_inner, insert_at_leaf, remaining_bytes, remove_at_leaf,
        root_to_inner, search_inner, search_leaf, write_page_ptr,
    },
    db::{BRootInfo, Txn},
    flux::{Flux, FluxBuf},
    lalloc::lalloc,
    uint::{InPgIdx, PgIdx, PgIdxDisk},
    util::{ChecksumDisk, from_bytes, from_bytes_mut},
};

enum InsertResult<B: BlockIo> {
    Single(FluxBuf<B>),
    Split(u64, Box<[u8]>, u64),
}

#[derive(Debug)]
pub(crate) enum BtreeGetResult<B: BlockIo> {
    Btree {
        pg_idx_1: PgIdx,
        pg_idx_2: PgIdx,
    },
    ValueEmbedded {
        buf: FluxBuf<B>,
        loc: usize,
        len: usize,
    },
    ValueLinkedList {
        pg_idx: PgIdx,
        len: PgIdx,
    },
}

async fn get<B: BlockIo>(
    key: &[u8],
    node: &FluxBuf<B>,
    flux: &mut Flux<B>,
) -> Result<Option<BtreeGetResult<B>>> {
    assert!(node.as_ref().header().kind == BNodeKind::Root);

    let search = search_inner(node.as_ref(), key);
    let child_pg_idx = if node.as_ref().header().len() > 0 {
        get_page_ptr(node.as_ref(), search.pg_idx())
    } else {
        bail!("empty root node")
    };
    let child_pg = flux.read(child_pg_idx).await?;
    get_inner(key, child_pg, flux).await
}

async fn get_inner<B: BlockIo>(
    key: &[u8],
    node: FluxBuf<B>,
    flux: &mut Flux<B>,
) -> Result<Option<BtreeGetResult<B>>> {
    match node.as_ref().header().kind {
        BNodeKind::Leaf => {
            let search = search_leaf(node.as_ref(), key);
            if let SearchResult::Exact(idx) = search {
                let value = get_value_leaf(node.as_ref(), idx);
                Ok(Some(value.with_page(node)))
            } else {
                Ok(None)
            }
        }
        BNodeKind::Root | BNodeKind::Inner => {
            let search = search_inner(node.as_ref(), key);
            let child_pg_idx = if node.as_ref().header().len() > 0 {
                get_page_ptr(node.as_ref(), search.pg_idx())
            } else {
                bail!("empty inner node")
            };
            let child_pg = flux.read(child_pg_idx).await?;
            Box::pin(get_inner(key, child_pg, flux)).await
        }
    }
}

pub(crate) struct UpsertCtx<'a> {
    bruh: &'a mut BRootInfo,
}

impl<'a, B: BlockIo> Txn<'a, B> {
    async fn btree_upsert(
        &mut self,
        dirty: &mut BRootInfo,
        key: &[u8],
        value: &[u8],
        root: FluxBuf<B>,
    ) -> Result<FluxBuf<B>> {
        match self
            .btree_upsert_inner(dirty, key, LeafValue::Value(value), root)
            .await?
        {
            InsertResult::Single(mut page_buf) => {
                let header = from_bytes_mut::<BNodeRootHeader>(page_buf.as_mut());
                header.version.set(header.version.get() + 1);
                Ok(page_buf)
            }
            InsertResult::Split(left, split, right) => {
                let mut pg_buf = self.flux.buf();
                let header = from_bytes_mut::<BNodeRootHeader>(pg_buf.as_mut());
                header.init();
                header.header.len.set(1);
                header.version.set(header.version.get() + 1);
                insert_at_inner(pg_buf.as_mut(), 0, left, &split, right);

                Ok(pg_buf)
            }
        }
    }

    async fn btree_upsert_inner(
        &mut self,
        dirty: &mut BRootInfo,
        key: &[u8],
        value: LeafValue<'_>,
        mut pg: FluxBuf<B>,
    ) -> Result<InsertResult<B>> {
        let header = from_bytes::<BNodeHeader>(pg.as_ref());
        if header.kind == BNodeKind::Leaf {
            self.btree_upsert_leaf(dirty, key, value, pg).await
        } else {
            let search = search_inner(pg.as_ref(), key).idx();
            let child_pg = if pg.as_ref().header().len() > 0 {
                let child_pg_idx = get_page_ptr(pg.as_ref(), search);
                self.free.push(child_pg_idx);
                self.flux.read(child_pg_idx).await?
            } else {
                pg.as_mut().header_mut().set_len(1);
                let mut child_pg = self.flux.buf();
                child_pg.as_mut().header_mut().init(BNodeKind::Leaf);
                child_pg
            };
            let insert = Box::pin(self.btree_upsert_inner(dirty, key, value, child_pg)).await?;

            match insert {
                InsertResult::Single(child_pg) => {
                    let (mut ctx, flux) = self.to_lalloc_ctx_and_flux(dirty);
                    let child_pg_idx = flux.write(child_pg, || lalloc(&mut ctx)).await?;
                    write_page_ptr(pg.as_mut(), search, child_pg_idx);
                    Ok(InsertResult::Single(pg))
                }
                InsertResult::Split(left, split, right) => {
                    let can_insert = remaining_bytes(pg.as_ref())
                        > in_pg_size_of::<PgIdxDisk>()
                            + in_pg_len(&split)
                            + in_pg_size_of::<SlotDisk>();
                    if can_insert {
                        insert_at_inner(pg.as_mut(), search, left, &split, right);
                        Ok(InsertResult::Single(pg))
                    } else {
                        if pg.as_ref().header().kind == BNodeKind::Root {
                            root_to_inner(pg.as_mut());
                        }
                        self.btree_split_inner(dirty, left, split, right, pg).await
                    }
                }
            }
        }
    }

    async fn btree_split_inner(
        &mut self,
        dirty: &mut BRootInfo,
        left_idx: u64,
        split: Box<[u8]>,
        right_idx: u64,
        pg: FluxBuf<B>,
    ) -> Result<InsertResult<B>> {
        let mut left = pg;
        let mut right = self.flux.buf();

        right.as_mut().header_mut().init(BNodeKind::Inner);

        let len = left.as_ref().header().len(); // children
        let mid = len / 2;
        // move slots [mid .. len-1] and their [ptr|key] blobs to right
        for idx in mid..len {
            let key = get_key_inner(left.as_ref(), idx);
            let left_ptr = get_page_ptr(left.as_ref(), idx);
            let right_ptr = get_page_ptr(left.as_ref(), idx + 1);
            insert_at_inner(right.as_mut(), idx - mid, left_ptr, key, right_ptr);
        }
        left.as_mut().header_mut().set_len(mid as u64);

        {
            let insert = if split.as_ref().cmp(get_key_inner(right.as_ref(), 0)) == Ordering::Less {
                &mut left
            } else {
                &mut right
            };

            let search = search_inner(insert.as_ref(), &split);
            match search {
                SearchResult::Exact(_) => bail!("exact match value should never be promoted"),
                SearchResult::Insert(idx) => {
                    insert_at_inner(insert.as_mut(), idx, left_idx, &split, right_idx);
                }
            }
        }

        let key = get_key_leaf(right.as_ref(), 0).to_vec().into_boxed_slice();

        let (mut ctx, flux) = self.to_lalloc_ctx_and_flux(dirty);
        let ret_left_idx = flux.write(left, || lalloc(&mut ctx)).await?;
        let ret_right_idx = flux.write(right, || lalloc(&mut ctx)).await?;

        Ok(InsertResult::Split(ret_left_idx, key, ret_right_idx))
    }

    async fn btree_upsert_leaf(
        &mut self,
        dirty: &mut BRootInfo,
        key: &[u8],
        value: LeafValue<'_>,
        mut pg: FluxBuf<B>,
    ) -> Result<InsertResult<B>> {
        let encoded_value = value.encode(self).await?;

        let can_insert = remaining_bytes(pg.as_ref())
            > in_pg_len(key) + encoded_value.len() + in_pg_size_of::<SlotDisk>();
        if can_insert {
            let search = search_leaf(pg.as_ref(), key);
            if let SearchResult::Exact(idx) = search
                && pg.as_ref().header().len() > 0
            {
                // ignoring underflow during upsert because it's less complicated
                self.btree_leaf_remove_at(dirty, pg.as_mut(), idx).await?;
            }
            insert_at_leaf(pg.as_mut(), search.idx(), key, &encoded_value);
            Ok(InsertResult::Single(pg))
        } else {
            let mut left = pg;
            let mut right = self.flux.buf();

            right.as_mut().header_mut().init(BNodeKind::Leaf);

            let len = left.as_ref().header().len();
            let start = len / 2;
            for idx in start..len {
                let key = get_key_leaf(left.as_ref(), idx);
                let value = get_value_leaf(left.as_ref(), idx).encode(left.as_ref());
                insert_at_leaf(right.as_mut(), idx - start, key, &value);
            }
            left.as_mut().header_mut().set_len(start as u64);

            {
                let insert = if key.cmp(get_key_leaf(right.as_ref(), 0)) == Ordering::Less {
                    &mut left
                } else {
                    &mut right
                };

                let search = search_leaf(insert.as_ref(), key);
                if let SearchResult::Exact(idx) = search {
                    // ignoring underflow during upsert because it's less complicated
                    self.btree_leaf_remove_at(dirty, insert.as_mut(), idx)
                        .await?;
                }
                insert_at_leaf(insert.as_mut(), search.idx(), key, &encoded_value);
            }

            let key = get_key_leaf(right.as_ref(), 0).to_vec().into_boxed_slice();

            let (mut ctx, flux) = self.to_lalloc_ctx_and_flux(dirty);
            let left_idx = flux.write(left, || lalloc(&mut ctx)).await?;
            let right_idx = flux.write(right, || lalloc(&mut ctx)).await?;

            Ok(InsertResult::Split(left_idx, key, right_idx))
        }
    }

    async fn btree_leaf_remove_at(
        &mut self,
        _dirty: &mut BRootInfo,
        buf: &mut [u8],
        idx: InPgIdx,
    ) -> Result<()> {
        let value = get_value_leaf(buf, idx);
        match value {
            LeafValueGetResult::Btree {
                pg_idx_1: _,
                pg_idx_2: _,
            } => {
                todo!("this is going to suck to implement")
            }
            LeafValueGetResult::ValueEmbedded { loc: _, len: _ } => {
                // no-op
            }
            LeafValueGetResult::ValueLinkedList { mut pg_idx, len } => loop {
                // self.flux.free(dirty, pg_idx);
                // TODO: reimplement free in lalloc or something

                if len < self.db.meta.page_size() {
                    break;
                }

                let pg = self.db.bio.read(pg_idx).await?;
                let next_idx =
                    pg.as_ref().len() - size_of::<PgIdxDisk>() - size_of::<ChecksumDisk>();
                pg_idx = from_bytes::<PgIdxDisk>(&pg.as_ref()[next_idx..]).get();
            },
        }
        remove_at_leaf(buf, idx);
        Ok(())
    }

    pub(crate) async fn create_value_linked_list(&mut self, value: &[u8]) -> Result<u64> {
        let chunk_size =
            self.db.meta.page_size() as usize - size_of::<PgIdxDisk>() - size_of::<ChecksumDisk>();
        let mut prev_pg_idx: PgIdx = 0;
        for chunk in value.chunks(chunk_size).rev() {
            let pg_idx = self.alloc().await?;
            let mut buf = self.db.bio.buf();
            let mbuf = buf.as_mut();
            let len = chunk.len();
            mbuf[..len].copy_from_slice(chunk);
            from_bytes_mut::<PgIdxDisk>(&mut mbuf[len..len + size_of::<PgIdxDisk>()])
                .set(prev_pg_idx);
            prev_pg_idx = pg_idx;
            self.db.bio.write(pg_idx, buf).await?;
        }
        Ok(prev_pg_idx)
    }

    pub(crate) async fn read_value_linked_list(
        &mut self,
        mut pg_idx: u64,
        buf: &mut [u8],
    ) -> Result<()> {
        let mut empty = buf.len();
        while empty > 0 {
            let pg = self.db.bio.read(pg_idx).await?;
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

    pub(crate) async fn alloc(&mut self) -> Result<u64> {
        // TODO: implement new local allocation scheme
        return Ok(0);
    }
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        bio::MIN_PAGE_SIZE,
        block::{BlockIo, BlockIoMemory},
        bnode::{self, BNodeHeader, BNodeKind, BNodeRootHeader, LeafValueEncoded},
        btree::{self, BtreeGetResult, get},
        flux::Flux,
        util::from_bytes_mut,
    };

    async fn build_basic_three_level_tree() -> Result<BlockIoMemory> {
        let bio = BlockIoMemory::new(MIN_PAGE_SIZE);
        bio.alloc(8).await?;

        let mut p0 = bio.buf();

        let h0 = from_bytes_mut::<BNodeRootHeader>(&mut p0);
        h0.init();
        bnode::insert_at_inner(&mut p0, 0, 2, b"g", 3);
        let p1 = p0.clone();

        bio.write(0, p0).await?;
        bio.write(1, p1).await?;

        let mut p2 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p2).init(BNodeKind::Inner);
        bnode::insert_at_inner(&mut p2, 0, 4, b"d", 5);
        assert_eq!(from_bytes_mut::<BNodeHeader>(&mut p2).len(), 2);
        bio.write(2, p2).await?;

        let mut p3 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p3).init(BNodeKind::Inner);
        bnode::insert_at_inner(&mut p3, 0, 6, b"j", 7);
        bio.write(3, p3).await?;

        let mut p4 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p4).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p4, 0, b"a", &LeafValueEncoded::ValueEmbedded(b"00"));
        bnode::insert_at_leaf(&mut p4, 1, b"b", &LeafValueEncoded::ValueEmbedded(b"01"));
        bnode::insert_at_leaf(&mut p4, 2, b"c", &LeafValueEncoded::ValueEmbedded(b"02"));
        bio.write(4, p4).await?;

        let mut p5 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p5).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p5, 0, b"d", &LeafValueEncoded::ValueEmbedded(b"03"));
        bnode::insert_at_leaf(&mut p5, 1, b"e", &LeafValueEncoded::ValueEmbedded(b"04"));
        bnode::insert_at_leaf(&mut p5, 2, b"f", &LeafValueEncoded::ValueEmbedded(b"05"));
        bio.write(5, p5).await?;

        let mut p6 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p6).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p6, 0, b"g", &LeafValueEncoded::ValueEmbedded(b"06"));
        bnode::insert_at_leaf(&mut p6, 1, b"h", &LeafValueEncoded::ValueEmbedded(b"07"));
        bnode::insert_at_leaf(&mut p6, 2, b"i", &LeafValueEncoded::ValueEmbedded(b"08"));
        bio.write(6, p6).await?;

        let mut p7 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p7).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p7, 0, b"j", &LeafValueEncoded::ValueEmbedded(b"09"));
        bnode::insert_at_leaf(&mut p7, 1, b"k", &LeafValueEncoded::ValueEmbedded(b"10"));
        bnode::insert_at_leaf(&mut p7, 2, b"l", &LeafValueEncoded::ValueEmbedded(b"11"));
        bio.write(7, p7).await?;

        Ok(bio)
    }

    async fn build_basic_flat() -> Result<BlockIoMemory> {
        let bio = BlockIoMemory::new(MIN_PAGE_SIZE);
        bio.alloc(6).await?;

        let mut p0 = bio.buf();

        let h0 = from_bytes_mut::<BNodeRootHeader>(&mut p0);
        h0.init();
        bnode::insert_at_inner(&mut p0, 0, 2, b"d", 3);
        bnode::insert_at_inner(&mut p0, 1, 3, b"g", 4);
        bnode::insert_at_inner(&mut p0, 2, 4, b"j", 5);
        let p1 = p0.clone();

        bio.write(0, p0).await?;
        bio.write(1, p1).await?;

        let mut p2 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p2).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p2, 0, b"a", &LeafValueEncoded::ValueEmbedded(b"00"));
        bnode::insert_at_leaf(&mut p2, 1, b"b", &LeafValueEncoded::ValueEmbedded(b"01"));
        bnode::insert_at_leaf(&mut p2, 2, b"c", &LeafValueEncoded::ValueEmbedded(b"02"));
        bio.write(2, p2).await?;

        let mut p3 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p3).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p3, 0, b"d", &LeafValueEncoded::ValueEmbedded(b"03"));
        bnode::insert_at_leaf(&mut p3, 1, b"e", &LeafValueEncoded::ValueEmbedded(b"04"));
        bnode::insert_at_leaf(&mut p3, 2, b"f", &LeafValueEncoded::ValueEmbedded(b"05"));
        bio.write(3, p3).await?;

        let mut p4: Vec<u8> = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p4).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p4, 0, b"g", &LeafValueEncoded::ValueEmbedded(b"06"));
        bnode::insert_at_leaf(&mut p4, 1, b"h", &LeafValueEncoded::ValueEmbedded(b"07"));
        bnode::insert_at_leaf(&mut p4, 2, b"i", &LeafValueEncoded::ValueEmbedded(b"08"));
        bio.write(4, p4).await?;

        let mut p5 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p5).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p5, 0, b"j", &LeafValueEncoded::ValueEmbedded(b"09"));
        bnode::insert_at_leaf(&mut p5, 1, b"k", &LeafValueEncoded::ValueEmbedded(b"10"));
        bnode::insert_at_leaf(&mut p5, 2, b"l", &LeafValueEncoded::ValueEmbedded(b"11"));
        bio.write(5, p5).await?;

        Ok(bio)
    }

    async fn build_basic_flat2() -> Result<BlockIoMemory> {
        let bio = BlockIoMemory::new(MIN_PAGE_SIZE);
        bio.alloc(7).await?;

        let mut p0 = bio.buf();

        let h0 = from_bytes_mut::<BNodeRootHeader>(&mut p0);
        h0.init();
        bnode::insert_init_at_inner(&mut p0, 2);
        let p1 = p0.clone();

        bio.write(0, p0).await?;
        bio.write(1, p1).await?;

        let mut p2 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p2).init(BNodeKind::Inner);
        bnode::insert_at_inner(&mut p2, 0, 3, b"d", 4);
        bnode::insert_at_inner(&mut p2, 1, 4, b"g", 5);
        bnode::insert_at_inner(&mut p2, 2, 5, b"j", 6);
        bio.write(2, p2).await?;

        let mut p3 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p3).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p3, 0, b"a", &LeafValueEncoded::ValueEmbedded(b"00"));
        bnode::insert_at_leaf(&mut p3, 1, b"b", &LeafValueEncoded::ValueEmbedded(b"01"));
        bnode::insert_at_leaf(&mut p3, 2, b"c", &LeafValueEncoded::ValueEmbedded(b"02"));
        bio.write(3, p3).await?;

        let mut p4 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p4).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p4, 0, b"d", &LeafValueEncoded::ValueEmbedded(b"03"));
        bnode::insert_at_leaf(&mut p4, 1, b"e", &LeafValueEncoded::ValueEmbedded(b"04"));
        bnode::insert_at_leaf(&mut p4, 2, b"f", &LeafValueEncoded::ValueEmbedded(b"05"));
        bio.write(4, p4).await?;

        let mut p5: Vec<u8> = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p5).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p5, 0, b"g", &LeafValueEncoded::ValueEmbedded(b"06"));
        bnode::insert_at_leaf(&mut p5, 1, b"h", &LeafValueEncoded::ValueEmbedded(b"07"));
        bnode::insert_at_leaf(&mut p5, 2, b"i", &LeafValueEncoded::ValueEmbedded(b"08"));
        bio.write(5, p5).await?;

        let mut p6 = bio.buf();
        from_bytes_mut::<BNodeHeader>(&mut p6).init(BNodeKind::Leaf);
        bnode::insert_at_leaf(&mut p6, 0, b"j", &LeafValueEncoded::ValueEmbedded(b"09"));
        bnode::insert_at_leaf(&mut p6, 1, b"k", &LeafValueEncoded::ValueEmbedded(b"10"));
        bnode::insert_at_leaf(&mut p6, 2, b"l", &LeafValueEncoded::ValueEmbedded(b"11"));
        bio.write(6, p6).await?;

        Ok(bio)
    }

    #[tokio::test]
    async fn get_test() -> Result<()> {
        fn assert_value<B: BlockIo>(res: Option<BtreeGetResult<B>>, value: &[u8]) {
            match res {
                Some(BtreeGetResult::ValueEmbedded { buf, loc, len }) => {
                    assert_eq!(&buf.as_ref()[loc..loc + len], value)
                }
                _ => panic!("expected embedded value"),
            }
        }

        async fn check_keys<B: BlockIo>(bio: B) -> Result<()> {
            let mut flux = Flux::new(bio);
            let root = flux.read(0).await?;

            assert_value(get(b"a", &root, &mut flux).await?, b"00");
            assert_value(get(b"b", &root, &mut flux).await?, b"01");
            assert_value(get(b"c", &root, &mut flux).await?, b"02");
            assert_value(get(b"d", &root, &mut flux).await?, b"03");
            assert_value(get(b"e", &root, &mut flux).await?, b"04");
            assert_value(get(b"f", &root, &mut flux).await?, b"05");
            assert_value(get(b"g", &root, &mut flux).await?, b"06");
            assert_value(get(b"h", &root, &mut flux).await?, b"07");
            assert_value(get(b"i", &root, &mut flux).await?, b"08");
            assert_value(get(b"j", &root, &mut flux).await?, b"09");
            assert_value(get(b"k", &root, &mut flux).await?, b"10");
            assert_value(get(b"l", &root, &mut flux).await?, b"11");

            Ok(())
        }

        check_keys(build_basic_three_level_tree().await?).await?;
        check_keys(build_basic_flat().await?).await?;
        check_keys(build_basic_flat2().await?).await?;

        Ok(())
    }

    // TODO: need to migrate to new generic Flux

    // use anyhow::Result;
    // use function_name::named;
    // use rand::{SeedableRng, rngs::StdRng};

    // use crate::{
    //     bnode::{
    //         BNodeBuf, BNodeHeader, BNodeKind, BNodeRootHeader, LeafValueGetResult, SearchResult,
    //         get_key_leaf, get_page_ptr, get_value_leaf, search_leaf,
    //     },
    //     btree::BtreeGetResult,
    //     db::Db,
    //     key_path,
    //     test::TmpDir,
    //     util::from_bytes,
    // };

    // #[named]
    // #[tokio::test(flavor = "multi_thread")]
    // async fn test_upsert() -> Result<()> {
    //     let dir = TmpDir::new(function_name!()).unwrap();
    //     let db = Db::builder().path(dir.root().join("file")).build().await?;
    //     {
    //         let mut txn = db.txn().write(key_path![])?.begin().await;
    //         let mut page = txn.flux_buf();
    //         page.as_mut().root_header_mut().init();
    //         let mut dirty = txn.create_root_dirty_entry().await?;
    //         let page = txn.btree_upsert(&mut dirty, b"key", b"value", page).await?;

    //         {
    //             let header = from_bytes::<BNodeRootHeader>(page.as_ref());
    //             assert_eq!(header.header.kind, BNodeKind::Root);
    //             assert_eq!(header.header.len.get(), 1);

    //             let pg = get_page_ptr(page.as_ref(), 0);
    //             let child_pg = txn.flux_read(pg).await?;
    //             {
    //                 let header = from_bytes::<BNodeHeader>(child_pg.as_ref());
    //                 assert_eq!(header.kind, BNodeKind::Leaf);
    //                 assert_eq!(header.len.get(), 1);

    //                 let search = search_leaf(child_pg.as_ref(), b"key");
    //                 assert!(matches!(search, SearchResult::Exact(0)));
    //                 let key = get_key_leaf(child_pg.as_ref(), 0);
    //                 assert_eq!(key, b"key");
    //                 let value = get_value_leaf(child_pg.as_ref(), 0);
    //                 match value {
    //                     LeafValueGetResult::ValueEmbedded { loc, len } => {
    //                         let value = &child_pg.as_ref()[loc..loc + len];
    //                         assert_eq!(value, b"value");
    //                     }
    //                     _ => panic!("expected embedded value"),
    //                 }
    //             }
    //         }

    //         let res = btree_get(b"key", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }

    //         let page = txn.btree_upsert(&mut dirty, b"key", b"value", page).await?;

    //         let res = txn.btree_get(b"key", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }

    //         let page = txn
    //             .btree_upsert(&mut dirty, b"key2", b"value2", page)
    //             .await?;

    //         let res = txn.btree_get(b"key", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }
    //         let res = txn.btree_get(b"key2", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value2")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }

    //         let page = txn
    //             .btree_upsert(&mut dirty, b"key3", b"value3", page)
    //             .await?;
    //         let res = txn.btree_get(b"key", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }
    //         let res = txn.btree_get(b"key2", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value2")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }
    //         let res = txn.btree_get(b"key3", &page).await?;
    //         match res.unwrap() {
    //             BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                 assert_eq!(&buf.as_ref()[loc..loc + len], b"value3")
    //             }
    //             _ => panic!("expected embedded value"),
    //         }
    //     }
    //     anyhow::Ok(())
    // }

    // #[ignore]
    // #[named]
    // #[tokio::test(flavor = "multi_thread")]
    // async fn test_upsert_many_rand() -> Result<()> {
    //     use rand::{Rng, distributions::Alphanumeric};
    //     use std::collections::HashMap;

    //     let dir = TmpDir::new(function_name!()).unwrap();
    //     let db = Db::builder().path(dir.root().join("file")).build().await?;
    //     {
    //         let mut txn = db.txn().write(key_path![])?.begin().await;
    //         let mut page = txn.flux_buf();
    //         page.as_mut().root_header_mut().init();
    //         let mut dirty = txn.create_root_dirty_entry().await?;

    //         let mut rng = StdRng::seed_from_u64(111);
    //         let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    //         for _i in 0..10_000 {
    //             let klen = rng.gen_range(1..=16);
    //             let vlen = rng.gen_range(1..=16);
    //             let key: Vec<u8> = (0..klen).map(|_| rng.sample(Alphanumeric)).collect();
    //             let value: Vec<u8> = (0..vlen).map(|_| rng.sample(Alphanumeric)).collect();

    //             page = txn.btree_upsert(&mut dirty, &key, &value, page).await?;

    //             let res = txn.btree_get(&key, &page).await?;
    //             if res.is_none() {
    //                 println!("key not found: {:?}", key);
    //             }
    //             match res.unwrap() {
    //                 BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                     assert_eq!(&buf.as_ref()[loc..loc + len], &value)
    //                 }
    //                 _ => panic!("expected embedded value"),
    //             }

    //             expected.insert(key, value);
    //         }

    //         for (key, value) in expected.iter() {
    //             let res = txn.btree_get(key, &page).await?;
    //             match res.unwrap() {
    //                 BtreeGetResult::ValueEmbedded { buf, loc, len } => {
    //                     assert_eq!(
    //                         &buf.as_ref()[loc..loc + len],
    //                         value.as_slice(),
    //                         "mismatch for key {:?}",
    //                         key
    //                     );
    //                 }
    //                 _ => panic!("expected embedded value for key {:?}", key),
    //             }
    //         }
    //     }
    //     anyhow::Ok(())
    // }
}
