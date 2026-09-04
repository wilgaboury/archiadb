//! Dioxus UI for inspecting database pages (B‑tree / Meta).
//! Non‑resizable split: left hex dump, right field table.

use std::{cmp, f32::consts::GOLDEN_RATIO, sync::OnceLock};

use dioxus::prelude::*;
#[cfg(target_os = "linux")]
use dioxus_desktop::tao::event_loop::EventLoop;
use palette::{FromColor, IntoColor, LinSrgba, Oklcha, Srgba, WithHue, rgb::PackedAbgr};

use dioxus_desktop::{Config, WindowBuilder, tao::event_loop::EventLoopBuilder};
// use tao::platform::unix::EventLoopBuilderExtUnix;

use crate::{
    btree::{BTreeHeader, BTreeRootHeader, SlotDisk, read_slot},
    util::{as_bytes, from_bytes},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InspectKind {
    Meta,
    BTree,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PageDisplayChunk {
    data: Box<[u8]>,
    name: String,
    value: Option<String>,
}

impl PageDisplayChunk {
    pub(crate) fn value(data: &[u8], name: &str, value: String) -> Self {
        Self {
            data: data.to_vec().into_boxed_slice(),
            name: name.to_string(),
            value: Some(value),
        }
    }

    pub(crate) fn empty(data: &[u8], name: &str) -> Self {
        Self {
            data: data.to_vec().into_boxed_slice(),
            name: name.to_string(),
            value: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PageDisplay {
    chunks: Vec<PageDisplayChunk>,
}

impl PageDisplay {
    pub(crate) fn from_bytes(data: &[u8], kind: InspectKind) -> Self {
        let mut chunks = Vec::new();
        match kind {
            InspectKind::Meta => {
                chunks.push(PageDisplayChunk::value(
                    data,
                    "Raw Bytes",
                    format!("{:?}", data),
                ));
            }
            InspectKind::BTree => {
                let header = from_bytes::<BTreeHeader>(data);
                match header.kind() {
                    crate::btree::BTreeNodeKind::Root => {
                        let header = from_bytes::<BTreeRootHeader>(data);
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.header.kind),
                            "Kind",
                            format!("{:?}, {}", header.header.kind, header.header.kind as u8),
                        ));
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.header.len),
                            "Length",
                            format!("{}", header.header.len.get()),
                        ));
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.version),
                            "Version",
                            format!("{}", header.version.get()),
                        ));
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.free),
                            "Free",
                            format!("{}", header.free.get()),
                        ));
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.arena.start),
                            "Arena Start",
                            format!("{}", header.arena.start.get()),
                        ));
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.arena.len),
                            "Arena Length",
                            format!("{}", header.arena.len.get()),
                        ));
                        chunks.push(PageDisplayChunk::value(
                            as_bytes(&header.arena.next),
                            "Arena Next",
                            format!("{}", header.arena.next.get()),
                        ));
                        for i in 0..(cmp::max(1, header.arena.len.get()) - 1) {
                            let slot_value = read_slot(data, i as usize);
                            chunks.push(PageDisplayChunk::value(
                                as_bytes(&SlotDisk::new(slot_value as u64)),
                                &format!("Slot {}", i),
                                format!("{}", slot_value),
                            ));
                        }
                    }
                    crate::btree::BTreeNodeKind::Inner => todo!(),
                    crate::btree::BTreeNodeKind::Leaf => todo!(),
                }
            }
        }
        Self { chunks }
    }
}

// -----------------------------------------------------------------------------
// Color generation (unchanged)
// -----------------------------------------------------------------------------

pub(crate) fn next_color(prev: u32) -> u32 {
    let unpacked: Srgba<u8> = PackedAbgr::from(prev).into();
    let linear: LinSrgba<f32> = unpacked.into_linear();
    let oklcha: Oklcha = linear.into_color();
    let ret: Oklcha = oklcha.with_hue(
        (((oklcha.hue.into_positive_degrees() / 360.0) + (1.0 - (1.0 / GOLDEN_RATIO))) % 1.0)
            * 360.0,
    );
    let linear_back: LinSrgba<f32> = ret.into_color();
    let srgb_f32: Srgba<f32> = linear_back.into_color();
    let srgb_u8: Srgba<u8> = Srgba::from_color(srgb_f32).into();
    PackedAbgr::from(srgb_u8).color
}
// -----------------------------------------------------------------------------
// Dioxus App – no resizing, fixed widths
// -----------------------------------------------------------------------------

#[derive(Props, PartialEq, Clone)]
struct AppProps {
    data: Box<[u8]>,
    kind: InspectKind,
}

static INSPECT_DATA: OnceLock<(Box<[u8]>, InspectKind)> = OnceLock::new();
fn app() -> Element {
    let (data, kind) = INSPECT_DATA.get().expect("inspect data not set");
    let display = use_memo(move || PageDisplay::from_bytes(&data, kind.clone()));
    let chunks = display().chunks.clone(); // clone for iteration; or borrow in the view
    let mut sel_chunk_idx = use_signal::<Option<usize>>(|| None);

    rsx! {
        div {
            style: "display: flex; height: 100vh; width: 100vw; overflow: hidden;",
            // Left panel: hex dump (flexible)
            div {
                style: "flex: 1; overflow-y: auto; padding: 8px;",
                h1 { "Bytes" }
                div {
                    style: "font-family: monospace; white-space: pre-wrap; word-break: break-all;",
                    { render_bytes(&chunks) }
                }
            }
            // Right panel: field table (fixed width, no resize)
            div {
                style: "width: 300px; flex-shrink: 0; overflow-y: auto; padding: 8px; border-left: 1px solid #ccc;",
                h2 { "Fields" }
                table {
                    style: "width: 100%; border-collapse: collapse;",
                    thead {
                        tr {
                            th { "Name" }
                            th { "Value" }
                        }
                    }
                    tbody {
                        for (idx, chunk) in chunks.iter().enumerate() {
                            {
                                let is_selected = sel_chunk_idx() == Some(idx);
                                rsx! {
                                    tr {
                                        key: "{idx}",
                                        style: if is_selected { "background-color: #e0e0e0;" } else { "" },
                                        onmouseenter: move |_| sel_chunk_idx.set(Some(idx)),
                                        onmouseleave: move |_| {
                                            if sel_chunk_idx() == Some(idx) {
                                                sel_chunk_idx.set(None);
                                            }
                                        },
                                        td {
                                            style: "font-weight: bold; padding: 2px 4px;",
                                            "{chunk.name}"
                                        }
                                        td {
                                            style: "font-family: monospace; padding: 2px 4px;",
                                            if let Some(ref val) = chunk.value {
                                                "{val}"
                                            } else {
                                                span { style: "font-style: italic; color: #888;", "No value" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

macro_rules! css_color_from_abgr {
    ($val:expr) => {{
        let r = ($val & 0xFF) as u8;
        let g = (($val >> 8) & 0xFF) as u8;
        let b = (($val >> 16) & 0xFF) as u8;
        let a = (($val >> 24) & 0xFF) as u8;
        format!("rgba({}, {}, {}, {})", r, g, b, a as f32 / 255.0)
    }};
}

fn render_bytes(chunks: &[PageDisplayChunk]) -> Element {
    let mut color = 0xFF6E6EFFu32;
    let mut data = Vec::new();
    for chunk in chunks {
        let css_color = css_color_from_abgr!(color);
        let hexes: Vec<String> = chunk.data.iter().map(|b| format!("{:02x}", b)).collect();
        data.push((hexes, css_color));
        color = next_color(color);
    }

    rsx! {
        for (hexes, css_color) in data {
            for hex in hexes {
                span { style: "color: {css_color};", "{hex}" }
                " "
            }
            br {}
        }
    }
}

pub(crate) fn inspect_page(data: &[u8], kind: InspectKind) {
    let data = data.to_vec().into_boxed_slice();
    INSPECT_DATA.set((data, kind)).unwrap();

    // std::thread::spawn(|| {
    //     launch(app);
    // })
    // .join()
    // .unwrap(); // optionally wait for it to finish

    let mut event_loop_builder = EventLoopBuilder::with_user_event();

    #[cfg(target_os = "linux")]
    {
        // Allow the event loop to initialize outside the main thread
        event_loop_builder.with_any_thread(true);
    }

    let event_loop = event_loop_builder.build();

    let config = Config::new()
        .with_window(WindowBuilder::new().with_title("Any-Thread App"))
        .with_event_loop(|builder| {
            // 2. Safely configure the underlying winit builder for any thread
            #[cfg(target_os = "linux")]
            {
                builder.with_any_thread(true);
            }
        });

    // 3. Launch your application (even inside a spawned background thread)
    std::thread::spawn(move || {
        LaunchBuilder::desktop().with_cfg(config).launch(app);
    })
    .join()
    .unwrap();
}
