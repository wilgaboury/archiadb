use std::{mem::replace, sync::Arc};

use eframe::{NativeOptions, egui};
use parking_lot::{Condvar, Mutex};
use winit::platform::wayland::EventLoopBuilderExtWayland;

use crate::db::Db;

pub(crate) enum InspectState {
    None,
    Some(InspectConfig),
    Done,
}

pub(crate) struct InspectConfig {
    db: Db,
    data: Box<[u8]>,
    kind: InspectKind,
    wait: InspectWait,
}

#[derive(Clone)]
pub(crate) struct InspectWait {
    inner: Arc<InspectBlockInner>,
}

pub(crate) struct InspectBlockInner {
    pub(crate) mutex: Mutex<bool>,
    pub(crate) cond: Condvar,
}

impl InspectWait {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(InspectBlockInner {
                mutex: Mutex::new(true),
                cond: Condvar::new(),
            }),
        }
    }

    pub(crate) fn wait(&self) {
        let mut gaurd = self.inner.mutex.lock();
        while *gaurd {
            self.inner.cond.wait(&mut gaurd);
        }
    }

    pub(crate) fn signal(&self) {
        {
            let mut gaurd = self.inner.mutex.lock();
            *gaurd = false;
        }
        self.inner.cond.notify_one();
    }
}

pub(crate) struct InspectChan {
    pub(crate) mutex: Mutex<InspectState>,
    pub(crate) cond: Condvar,
}

impl InspectChan {
    const fn new() -> Self {
        Self {
            mutex: Mutex::new(InspectState::None),
            cond: Condvar::new(),
        }
    }

    fn consume(&self) -> InspectState {
        let mut gaurd = self.mutex.lock();
        while matches!(*gaurd, InspectState::None) {
            self.cond.wait(&mut gaurd);
        }
        if matches!(*gaurd, InspectState::Done) {
            InspectState::Done
        } else {
            replace(&mut gaurd, InspectState::Done)
        }
    }

    fn publish(&self, config: InspectConfig) {
        {
            let mut gaurd = self.mutex.lock();
            *gaurd = InspectState::Some(config);
        }
        self.cond.notify_one();
    }

    fn done(&self) {
        {
            let mut gaurd = self.mutex.lock();
            *gaurd = InspectState::Done
        }
        self.cond.notify_one();
    }
}

static INSPECT_CHAN: InspectChan = InspectChan::new();

pub(crate) fn inspect_main() {
    loop {
        match INSPECT_CHAN.consume() {
            InspectState::None => eprintln!("should never consume none"),
            InspectState::Some(config) => {
                let app = InspectApp {
                    config,
                    should_close: false,
                };
                let mut options = NativeOptions {
                    viewport: egui::ViewportBuilder::default()
                        .with_inner_size([400.0, 300.0])
                        .with_title("Debug UI"),
                    ..Default::default()
                };
                options.event_loop_builder = Some(Box::new(|builder| {
                    builder.with_any_thread(true);
                }));
                let _ = eframe::run_native("Debug UI", options, Box::new(|_cc| Ok(Box::new(app))));
            }
            InspectState::Done => {
                break;
            }
        }
    }
}

pub(crate) fn inspect_main_done() {
    INSPECT_CHAN.done();
}

pub(crate) fn inspect_page(db: Db, data: &[u8], kind: InspectKind) {
    let wait = InspectWait::new();
    let data = data.to_vec().into_boxed_slice();
    INSPECT_CHAN.publish(InspectConfig {
        db,
        data,
        kind,
        wait: wait.clone(),
    });
    wait.wait();
}

pub(crate) enum InspectKind {
    Meta,
    BTree,
}

pub(crate) struct InspectApp {
    config: InspectConfig,
    should_close: bool,
}

impl eframe::App for InspectApp {
    /// Called before `ui` — good for updating state.
    fn logic(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // No background logic needed here.
    }

    /// Called to draw the UI each frame.
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if self.should_close || ui.ctx().input(|i| i.viewport().close_requested()) {
            self.config.wait.signal();
        }

        // If the close button was clicked, request the window to close.
        if self.should_close {
            ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
            return;
        }

        ui.heading("Debug Data");
        ui.separator();
        ui.with_layout(
            egui::Layout::right_to_left(egui::Align::TOP).with_main_wrap(true),
            |ui| {
                for byte in self.config.data.iter() {
                    ui.label(egui::RichText::new(format!("{:02x}", *byte).as_str()).monospace());
                }
            },
        );
    }
}
