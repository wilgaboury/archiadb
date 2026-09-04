use std::{cmp, f32::consts::GOLDEN_RATIO, thread};

use eframe::{
    NativeOptions,
    egui::{self, Color32},
};
use egui_extras::{Column, TableBuilder};
use palette::{FromColor, IntoColor, LinSrgba, Oklcha, Srgba, WithHue, rgb::PackedAbgr};
use winit::platform::{wayland::EventLoopBuilderExtWayland, x11::EventLoopBuilderExtX11};

use crate::{
    btree::{BTreeHeader, BTreeRootHeader, SlotDisk, read_slot},
    db::Db,
    util::{as_bytes, from_bytes},
};

pub(crate) enum Platform {
    Main,
    X11,
    Wayland,
}

const DEFAULT_PLATFORM: Platform = Platform::Wayland;

pub(crate) struct InspectConfig {
    data: Box<[u8]>,
    kind: InspectKind,
    platform: Platform,
}

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

pub(crate) fn inspect_launch_thread(config: InspectConfig) {
    thread::scope(|s| {
        s.spawn(|| {
            inspect_launch(config);
        });
    });
}

pub(crate) fn inspect_launch(config: InspectConfig) {
    let mut options = NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([400.0, 300.0])
            .with_title("Debug UI"),
        ..Default::default()
    };
    match config.platform {
        Platform::Main => {
            // no-op
        }
        Platform::X11 => {
            options.event_loop_builder = Some(Box::new(|builder| {
                EventLoopBuilderExtX11::with_any_thread(builder, true);
            }));
        }
        Platform::Wayland => {
            options.event_loop_builder = Some(Box::new(|builder| {
                EventLoopBuilderExtWayland::with_any_thread(builder, true);
            }));
        }
    }
    let display = PageDisplay::from_bytes(&config.data, config.kind.clone());
    let app = InspectApp {
        config,
        display,
        should_close: false,
        sel_chunk_idx: None,
    };
    let _ = eframe::run_native("Debug UI", options, Box::new(|_cc| Ok(Box::new(app))));
}

pub(crate) fn inspect_page(_db: Db, data: &[u8], kind: InspectKind) {
    let data = data.to_vec().into_boxed_slice();
    let config = InspectConfig {
        data,
        kind,
        platform: DEFAULT_PLATFORM,
    };
    inspect_launch_thread(config);
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InspectKind {
    Meta,
    BTree,
}

pub(crate) struct InspectApp {
    config: InspectConfig,
    display: PageDisplay,
    should_close: bool,
    sel_chunk_idx: Option<usize>,
}

pub(crate) struct PageDisplay {
    chunks: Vec<PageDisplayChunk>,
}

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

impl InspectApp {
    fn render_bytes(&mut self, ui: &mut egui::Ui) {
        macro_rules! color_from_abgr {
            ($val:expr) => {
                egui::Color32::from_rgba_unmultiplied(
                    ($val & 0xFF) as u8,
                    (($val >> 8) & 0xFF) as u8,
                    (($val >> 16) & 0xFF) as u8,
                    (($val >> 24) & 0xFF) as u8,
                )
            };
        }

        let mut color = 0xFF6E6EFFu32;
        for chunk in self.display.chunks.iter() {
            for byte in chunk.data.iter() {
                ui.label(
                    egui::RichText::new(format!("{:02x}", *byte).as_str())
                        .monospace()
                        .color(color_from_abgr!(color)),
                );
            }
            color = next_color(color);
        }
    }
}

impl eframe::App for InspectApp {
    /// Called before `ui` — good for updating state.
    fn logic(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // No background logic needed here.
    }

    /// Called to draw the UI each frame.
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // If the close button was clicked, request the window to close.
        if self.should_close {
            ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
            return;
        }

        egui::Panel::right("right_panel")
            .resizable(true)
            .min_size(100.0)
            .show(ui, |ui| {
                TableBuilder::new(ui)
                    .columns(Column::auto(), 2) // two auto-sized columns
                    .striped(true) // alternating row background (optional)
                    .header(20.0, |mut header| {
                        header.col(|ui| {
                            ui.label(egui::RichText::new("Name").heading());
                        });
                        header.col(|ui| {
                            ui.label(egui::RichText::new("Value").heading());
                        });
                    })
                    .body(|mut body| {
                        for (idx, chunk) in self.display.chunks.iter().enumerate() {
                            body.row(20.0, |mut row| {
                                // 1. Add the first column and capture its response
                                let (_, response) = row.col(|ui| {
                                    ui.label(egui::RichText::new(chunk.name.as_str()).strong());
                                });

                                // 2. Update selection state based on hover
                                //    The response from the first cell represents the whole row's interaction
                                if response.hovered() {
                                    self.sel_chunk_idx = Some(idx);
                                } else if self.sel_chunk_idx == Some(idx) {
                                    // Only clear if this row was previously selected
                                    self.sel_chunk_idx = None;
                                }

                                // 3. Apply selection highlight to the entire row
                                //    set_selected applies to all cells added after this call
                                if self.sel_chunk_idx == Some(idx) {
                                    row.set_selected(true);
                                }

                                // 4. Add the second column (value)
                                row.col(|ui| {
                                    if let Some(value) = &chunk.value {
                                        ui.label(egui::RichText::new(value.as_str()).monospace());
                                    } else {
                                        ui.label(egui::RichText::new("No value").italics());
                                    }
                                });
                            });
                        }
                    });
            });

        egui::CentralPanel::default().show(ui, |ui| {
            ui.heading("Bytes");
            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .show(ui, |ui| {
                    egui::Frame::default()
                        .inner_margin(egui::Margin::same(24))
                        .show(ui, |ui| {
                            ui.with_layout(
                                egui::Layout::left_to_right(egui::Align::TOP).with_main_wrap(true),
                                |ui| {
                                    self.render_bytes(ui);
                                },
                            );
                        });
                });
        });
    }
}
