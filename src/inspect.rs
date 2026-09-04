use std::{cmp, thread};

use eframe::{
    NativeOptions,
    egui::{self, Grid},
};
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
        for byte in self.config.data.iter() {
            ui.label(egui::RichText::new(format!("{:02x}", *byte).as_str()).monospace());
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
            .resizable(true) // movable vertical separator
            .min_size(100.0) // minimum width
            .show(ui, |ui| {
                Grid::new("chunk_table")
                    .num_columns(2) // name | value
                    .striped(true) // optional: alternating row colors
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("Name").heading());
                        ui.label(egui::RichText::new("Value").heading());
                        ui.end_row();

                        for chunk in self.display.chunks.iter() {
                            // Column 1: chunk name
                            ui.label(egui::RichText::new(chunk.name.as_str()).strong());

                            // Column 2: chunk value
                            if let Some(value) = &chunk.value {
                                ui.label(egui::RichText::new(value.as_str()).monospace());
                            } else {
                                ui.label(egui::RichText::new("No value").italics());
                            }

                            ui.end_row(); // important: marks the end of each row
                        }
                    })
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
                                egui::Layout::right_to_left(egui::Align::TOP).with_main_wrap(true),
                                |ui| {
                                    self.render_bytes(ui);
                                },
                            );
                        });
                });
        });
    }
}
