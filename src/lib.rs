// Copyright 2025 The ChromiumOS Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//! An HTTP proxy for IPP-over-USB devices
//!
//! ## Introduction
//!
//! IPP-over-USB (or IPP-USB) is a standardized protocol for transporting HTTP through USB printer
//! class interfaces.  Even though it has IPP in the name, IPP-USB allows transporting arbitrary
//! HTTP requests and responses to a printer's embedded web server.  Some common examples of what
//! it can be used for:
//!
//! - IPP printing, scanning, and faxing (the original use case)
//! - Mopria eSCL scanning
//! - Access to a printer's internal administrative web pages
//!
//! Due to limitations in the protocol, IPP-USB cannot be treated as simply a socket that forwards
//! between the host and device.  Each HTTP request and response must be transmitted in its
//! entirety to avoid leaving partial data in the device's USB buffers.  This is a limitation of
//! the protocol itself, not a limitation of this crate.
//!
//! This crate provides a simplified interface to IPP-USB that allows applications to make a
//! device look like a network HTTP server.  The application is responsible for setting up a
//! `tokio` runtime, TCP listener, and USB device; then `ippusb::Bridge` internally runs an
//! asynchronous HTTP proxy that forwards requests until a shutdown event is signalled.
//!
//! ## Example
//!
//! This example bridges a random TCP port on localhost to the ChromeOS [virtual-usb-printer].
//! After 30s, it tells the bridge to shut down because the device was unplugged.
//!
//! [virtual-usb-printer]: https://source.chromium.org/chromiumos/chromiumos/codesearch/+/main:src/third_party/virtual-usb-printer/
//!
//! ```no_run
//! use std::time::Duration;
//! use ippusb::{Bridge, Device, ShutdownReason};
//! use rusb::{Context, UsbContext};
//! use tokio::net::TcpListener;
//! use tokio::runtime::Handle;
//! use tokio::sync::mpsc;
//! use tokio::time::sleep;
//!
//! async fn serve(verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
//!     let context = Context::new()?;
//!     let rusb_device = context.open_device_with_vid_pid(0x18d1, 0x505e)
//!             .ok_or(ippusb::Error::NotIppUsb)?;
//!     let ippusb_device = ippusb::Device::new(verbose, rusb_device)?;
//!     let (tx, rx) = mpsc::channel(1);
//!     let listener = TcpListener::bind("127.0.0.1:0").await?;
//!
//!     let handle = Handle::current();
//!     handle.spawn(async move {
//!         sleep(Duration::from_secs(30)).await;
//!         tx.send(ShutdownReason::Unplugged);
//!     });
//!
//!     let mut bridge = Bridge::new(verbose, rx, listener, ippusb_device, handle);
//!     bridge.run().await;
//!     Ok(())
//! }
//! ```

mod bridge;
mod device;
mod device_info;
mod error;
mod http;
mod io_adapters;

pub use crate::bridge::{Bridge, ShutdownReason};
pub use crate::device::Device;
pub use crate::device_info::device_supports_ippusb;
pub use crate::error::Error;
