// Copyright 2024 The ChromiumOS Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use std::fmt;
use std::io;

/// Errors returned while handling IPP-over-USB HTTP requests.
#[derive(Debug)]
pub enum Error {
    /// Failed to claim an IPP-USB interface.  The value contains the interface number and
    /// underlying `rusb` error.
    ClaimInterface(u8, rusb::Error),

    /// Failed to release an IPP-USB interface.  The value contains the interface number and
    /// underlying `rusb` error.
    ReleaseInterface(u8, rusb::Error),

    /// Failed to detach a kernel driver from an interface.  The value contains the interface
    /// number and underlying `rusb` error.
    DetachDrivers(u8, rusb::Error),

    /// Failed to re-attach a kernel driver from an interface.  The value contains the interface
    /// number and underlying `rusb` error.
    AttachDrivers(u8, rusb::Error),

    /// An error occurred while cleaning up the released interface pool.  The value contains the
    /// underlying I/O error.
    CleanupThread(io::Error),

    /// Failed to read the device config descriptor.  The value contains the underlying `rusb`
    /// error.
    ReadConfigDescriptor(rusb::Error),

    /// Failed to read the device descriptor.  The value contains the underlying `rusb` error.
    ReadDeviceDescriptor(rusb::Error),

    /// Failed to set the active device config.  The value contains the underlying `rusb` error.
    SetActiveConfig(rusb::Error),

    /// Failed to set an interface to the alternate needed for IPP-USB.  The value contains the
    /// interface number and underlying `rusb` error.
    SetAlternateSetting(u8, rusb::Error),

    /// Could not find a free IPP-USB interface to handle a request.
    NoFreeInterface,

    /// The specified device does not support IPP-USB.
    NotIppUsb,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            ClaimInterface(i, err) => write!(f, "Failed to claim interface {}: {}", i, err),
            ReleaseInterface(i, err) => write!(f, "Failed to release interface {}: {}", i, err),
            DetachDrivers(i, err) => write!(
                f,
                "Failed to detach kernel driver for interface {}: {}",
                i, err
            ),
            AttachDrivers(i, err) => write!(
                f,
                "Failed to attach kernel driver for interface {}, {}",
                i, err
            ),
            CleanupThread(err) => write!(f, "Failed to start cleanup thread: {}", err),
            ReadConfigDescriptor(err) => write!(f, "Failed to read config descriptor: {}", err),
            ReadDeviceDescriptor(err) => write!(f, "Failed to read device descriptor: {}", err),
            SetActiveConfig(err) => write!(f, "Failed to set active config: {}", err),
            SetAlternateSetting(i, err) => write!(
                f,
                "Failed to set interface {} alternate setting: {}",
                i, err
            ),
            NoFreeInterface => write!(f, "There is no free IPP USB interface to claim."),
            NotIppUsb => write!(f, "The specified device is not an IPP USB device."),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
