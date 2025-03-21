// Copyright 2025 The ChromiumOS Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use log::info;
use rusb::{Direction, TransferType, UsbContext};

use crate::error::Error;
use crate::error::Result;

pub(crate) fn is_ippusb_interface(descriptor: &rusb::InterfaceDescriptor) -> bool {
    descriptor.class_code() == 0x07
        && descriptor.sub_class_code() == 0x01
        && descriptor.protocol_code() == 0x04
}

/// Check if the given device supports IPP-USB.
///
/// Given a rusb Device, search through the device's configurations to see if there is a
/// particular configuration that supports IPP-over-USB (aka IPP-USB).  If such a configuration
/// is found, return `Ok(true)`.  If there were no errors, but the device does not have such a
/// configuration, return `Ok(false)`.  If there was an error reading descriptors, return `Err`.
///
/// A device is considered to support IPP-USB if it has a configuration with at least two
/// IPP-USB interfaces.
///
/// An interface is considered an IPP-USB interface if all of the following are true:
///
/// *  The USB class is Printer (7).
/// *  The USB subclass is Printer (1).
/// *  The USB protocol is IPP-USB (4).
/// *  The interface contains a bulk-in and a bulk-out endpoint.
///
/// The device's configuration is not changed by this function.
pub fn device_supports_ippusb<T: UsbContext>(device: &rusb::Device<T>) -> Result<bool> {
    match IppusbDeviceInfo::new(device) {
        Ok(_) => Ok(true),
        Err(Error::NotIppUsb) => Ok(false),
        Err(e) => Err(e),
    }
}

/// The information for an interface descriptor that supports IPP-USB.
///
/// Bulk transfers can be read/written to the in/out endpoints, respectively.
#[derive(Copy, Clone)]
pub(crate) struct IppusbDescriptor {
    pub interface_number: u8,
    pub alternate_setting: u8,
    pub in_endpoint: u8,
    pub out_endpoint: u8,
}

/// The configuration and interfaces that support IPP-USB for a USB device.
///
///  A valid IPP-USB device will have at least two interfaces.
pub(crate) struct IppusbDeviceInfo {
    pub config: u8,
    pub interfaces: Vec<IppusbDescriptor>,
}

impl IppusbDeviceInfo {
    /// Given a rusb Device, search through the device's configurations to see if there is a
    /// particular configuration that supports IPP-over-USB (aka IPP-USB).  If such a configuration
    /// is found, return an `IppusbDeviceInfo`, which specifies the configuration as well as the
    /// IPP-USB interfaces within that configuration.
    ///
    /// See the documentation for `device_supports_ippusb()` for the details of what IPP-USB
    /// support requires.
    ///
    /// If the given device does not support IPP-USB or the descriptors cannot be read, return
    /// `Err`.
    pub(crate) fn new<T: UsbContext>(device: &rusb::Device<T>) -> Result<Self> {
        let desc = device
            .device_descriptor()
            .map_err(Error::ReadDeviceDescriptor)?;
        for i in 0..desc.num_configurations() {
            let config = device
                .config_descriptor(i)
                .map_err(Error::ReadConfigDescriptor)?;

            let mut interfaces = Vec::new();
            for interface in config.interfaces() {
                'alternates: for alternate in interface.descriptors() {
                    if !is_ippusb_interface(&alternate) {
                        continue;
                    }
                    info!(
                        concat!(
                            "Device {}:{} - Found IPP-USB interface. ",
                            "config {}, interface {}, alternate {}"
                        ),
                        device.bus_number(),
                        device.address(),
                        config.number(),
                        interface.number(),
                        alternate.setting_number()
                    );

                    // Find the bulk in and out endpoints for this interface.
                    let mut in_endpoint: Option<u8> = None;
                    let mut out_endpoint: Option<u8> = None;
                    for endpoint in alternate.endpoint_descriptors() {
                        match (endpoint.direction(), endpoint.transfer_type()) {
                            (Direction::In, TransferType::Bulk) => {
                                in_endpoint.get_or_insert(endpoint.address());
                            }
                            (Direction::Out, TransferType::Bulk) => {
                                out_endpoint.get_or_insert(endpoint.address());
                            }
                            _ => {}
                        };

                        if in_endpoint.is_some() && out_endpoint.is_some() {
                            break;
                        }
                    }

                    if let (Some(in_endpoint), Some(out_endpoint)) = (in_endpoint, out_endpoint) {
                        interfaces.push(IppusbDescriptor {
                            interface_number: interface.number(),
                            alternate_setting: alternate.setting_number(),
                            in_endpoint,
                            out_endpoint,
                        });
                        // We must consider at most one alternate setting when detecting IPP-USB
                        // interfaces.
                        break 'alternates;
                    }
                }
            }

            // A device must have at least two IPP-USB interfaces in order to be considered an
            // IPP-USB device.
            if interfaces.len() >= 2 {
                return Ok(Self {
                    config: config.number(),
                    interfaces,
                });
            }
        }

        Err(Error::NotIppUsb)
    }
}
