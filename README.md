# IPP-USB

A Rust crate for using HTTP with IPP-over-USB devices.

## Background

IPP-over-USB (or IPP-USB) is a standardized protocol for transporting HTTP
through USB printer class interfaces. Even though it has IPP in the name,
IPP-USB allows transporting arbitrary HTTP requests and responses to a printer’s
embedded web server. Some common examples of what it can be used for:

*  IPP printing, scanning, and faxing (the original use case)
*  Mopria eSCL scanning
*  Browsing a printer’s internal administrative web pages

Due to limitations in the protocol, IPP-USB cannot be treated as simply a socket
that forwards between the host and device. Each HTTP request and response must
be transmitted in its entirety to avoid leaving partial data in the device’s USB
buffers.

## Overview

`ippusb` is a Rust crate that provides a simplified interface to running an HTTP
proxy over IPP-USB. It allows applications to make a device look like a network
HTTP server. `ippusb` itself just provides the proxy, but does not include
integration with the host OS.

The main application is responsible for setting up a `tokio` runtime, `tokio`
TCP listener, and `rusb` USB device. `ippusb::Bridge` then internally runs an
asynchronous HTTP proxy that forwards requests until a shutdown event is
signalled.

## Example

This example bridges a random TCP port on localhost to the ChromeOS
[virtual-usb-printer]. After 30s, it tells the bridge to shut down because the
device was unplugged.

```rust
use std::time::Duration;
use ippusb::{Bridge, Device, ShutdownReason};
use rusb::{Context, UsbContext};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::sleep;

async fn serve(verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let rusb_device = context.open_device_with_vid_pid(0x18d1, 0x505e)
            .ok_or(ippusb::Error::NotIppUsb)?;
    let ippusb_device = ippusb::Device::new(verbose, rusb_device)?;
    let (tx, rx) = mpsc::channel(1);
    let listener = TcpListener::bind("127.0.0.1:0").await?;

    let handle = Handle::current();
    handle.spawn(async move {
        sleep(Duration::from_secs(30)).await;
        tx.send(ShutdownReason::Unplugged);
    });

    let mut bridge = Bridge::new(verbose, rx, listener, ippusb_device, handle);
    bridge.run().await;
    Ok(())
}
```

## Related Projects

For a complete application that integrates the IPP-USB proxy, see
[ippusb_bridge] from ChromeOS.  This shows how to use all of the functionality
of `ippusb` in a production setting, but is probably not directly usable outside
of ChromeOS due to the specific details of how ChromeOS works.

If you are looking for a ready-to-go application rather than a library, also
consider the [ipp-usb] project from OpenPrinting.  It has more features than
ippusb\_bridge and should be directly usable on many Linux distros.

[ippusb_bridge]: https://chromium.googlesource.com/chromiumos/platform2/+/HEAD/ippusb_bridge
[ipp-usb]: https://github.com/OpenPrinting/ipp-usb
[virtual-usb-printer]: https://chromium.googlesource.com/chromiumos/third_party/virtual-usb-printer/
