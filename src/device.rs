// Copyright 2020 The ChromiumOS Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use std::cmp::{Ord, Ordering};
use std::collections::BinaryHeap;
use std::io::{self, Read, Write};

#[cfg(target_os = "android")]
use std::os::fd::RawFd;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::vec::Vec;

use log::{debug, error, info, trace};
use rusb::{Context, UsbContext};
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;

use crate::device_info::{is_ippusb_interface, IppusbDescriptor, IppusbDeviceInfo};
use crate::error::Error;
use crate::error::Result;

const USB_TRANSFER_TIMEOUT: Duration = Duration::from_secs(60);
const USB_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

fn interface_contains_ippusb(interface: &rusb::Interface) -> bool {
    for descriptor in interface.descriptors() {
        if is_ippusb_interface(&descriptor) {
            return true;
        }
    }
    false
}

fn set_device_config<T: UsbContext>(handle: &rusb::DeviceHandle<T>, new_config: u8) -> Result<()> {
    let cur_config = handle
        .device()
        .active_config_descriptor()
        .map_err(Error::ReadConfigDescriptor)?;

    // While detaching any outstanding kernel drivers for the current config, keep
    // track of non-printer drivers so we can restore them after setting the config.
    let mut restore_interfaces = Vec::new();
    for interface in cur_config.interfaces() {
        if !interface_contains_ippusb(&interface) {
            match handle.kernel_driver_active(interface.number()) {
                Ok(false) => continue, // No active driver.
                Err(e) => return Err(Error::DetachDrivers(interface.number(), e)),
                _ => {}
            }

            info!(
                "Temporarily detaching kernel driver for non-printer interface {}",
                interface.number()
            );
            restore_interfaces.push(interface.number());
        }

        match handle.detach_kernel_driver(interface.number()) {
            Err(e) if e != rusb::Error::NotFound => {
                return Err(Error::DetachDrivers(interface.number(), e))
            }
            _ => {}
        }
    }

    info!(
        "Switching from configuration {} to {}",
        cur_config.number(),
        new_config
    );
    handle
        .set_active_configuration(new_config)
        .map_err(Error::SetActiveConfig)?;

    // Try to put back the previously detached drivers.  We don't return an error if one
    // of these fails because it won't prevent us from claiming the IPP-USB interfaces later.
    for inum in restore_interfaces {
        handle
            .attach_kernel_driver(inum)
            .unwrap_or_else(|e| error!("Failed to reattach driver for interface {}: {}", inum, e));
    }

    Ok(())
}

struct ClaimedInterface {
    handle: Arc<rusb::DeviceHandle<Context>>,
    descriptor: IppusbDescriptor,
}

impl ClaimedInterface {
    /// Send a USB claim for our interface and switch it to the correct alternate setting.
    fn claim(&mut self) -> Result<()> {
        self.handle
            .claim_interface(self.descriptor.interface_number)
            .map_err(|e| Error::ClaimInterface(self.descriptor.interface_number, e))?;
        self.handle
            .set_alternate_setting(
                self.descriptor.interface_number,
                self.descriptor.alternate_setting,
            )
            .map_err(|e| Error::SetAlternateSetting(self.descriptor.interface_number, e))
    }

    /// Send a USB release for our interface.
    fn release(&mut self) -> Result<()> {
        self.handle
            .release_interface(self.descriptor.interface_number)
            .map_err(|e| Error::ReleaseInterface(self.descriptor.interface_number, e))
    }
}

impl Ord for ClaimedInterface {
    /// Order claimed interfaces based purely on interface descriptors, but sorted in reverse
    /// order.  This allows a BinaryHeap<ClaimedInterface> to always pick the lowest-numbered
    /// interface.  The handle field is ignored because two IPP-USB interfaces on the same device
    /// can't have the same interface number and alternate.  If ClaimedInterface values from
    /// different devices are compared, the result is logically consistent, but not meaningful.
    fn cmp(&self, other: &Self) -> Ordering {
        match self.descriptor.cmp(&other.descriptor) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => Ordering::Equal,
        }
    }
}

impl PartialOrd for ClaimedInterface {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ClaimedInterface {
    fn eq(&self, other: &Self) -> bool {
        self.descriptor == other.descriptor && self.handle.device() == other.handle.device()
    }
}

impl Eq for ClaimedInterface {}

/// InterfaceManagerState contains the internal state of InterfaceManager.  It is intended to
/// be shared across InterfaceManager instances and protected by a mutex.
struct InterfaceManagerState {
    interfaces: BinaryHeap<ClaimedInterface>,
    handle: Arc<rusb::DeviceHandle<Context>>,
    usb_config: u8,
    active: usize,
    pending_cleanup: bool,
    next_cleanup: Instant,
    terminate_cleanup: bool,
    cleanup_thread: Option<JoinHandle<()>>,
}

impl InterfaceManagerState {
    fn claim_all(&mut self) -> Result<()> {
        // Detach any outstanding kernel drivers for the current config before attempting
        // to switch to our desired config.
        let config = self
            .handle
            .device()
            .active_config_descriptor()
            .map_err(Error::ReadConfigDescriptor)?;

        if config.number() != self.usb_config {
            set_device_config(self.handle.as_ref(), self.usb_config)?;
        }

        let mut heap = BinaryHeap::with_capacity(self.interfaces.len());
        let mut res = Ok(());
        for mut interface in self.interfaces.drain() {
            if let Err(e) = interface.claim() {
                // We don't bother to free any successfully claimed interfaces because
                // it's not an error to try to claim them again when the next connection
                // arrives.
                error!(
                    "Failed to reclaim interface {}: {}",
                    interface.descriptor.interface_number, e
                );
                res = res.and(Err(e));
            }
            // Every interface goes back into the heap regardless of success.
            heap.push(interface);
        }
        self.interfaces = heap;
        res
    }

    /// Call release on all interfaces.  Return Ok if all calls succeeded, or the first Err if any
    /// call failed.
    fn release_all(&mut self) -> Result<()> {
        let mut res = Ok(());
        let mut heap = BinaryHeap::with_capacity(self.interfaces.len());
        for mut interface in self.interfaces.drain() {
            res = res.and(interface.release());
            heap.push(interface);
        }
        self.interfaces = heap;
        res
    }
}

/// CleanupGuard represents the cleanup thread started by InterfaceManager.  When it is dropped,
/// the cleanup thread will be stopped.
struct CleanupGuard {
    manager: InterfaceManager,
}

impl CleanupGuard {
    pub fn new(manager: InterfaceManager) -> Self {
        Self { manager }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Err(e) = self.manager.stop_cleanup_thread() {
            error!("Failed to stop cleanup thread: {}", e);
        }
    }
}

/// InterfaceManager is responsible for managing a pool of claimed USB interfaces.
/// At construction, it is provided with a set of interfaces, and then clients
/// can use its member functions in order to request and free interfaces.
///
/// If no interfaces are currently available, requesting an interface will block
/// until an interface is freed by another thread.
///
/// InterfaceManager maintains the invariant that interfaces are claimed when
/// handed out.  It expects newly-inserted interfaces to be claimed by libusb and
/// it ensures that they are still claimed when retrieved.  Internally it releases
/// and claims free interfaces to allow sharing with other programs that might need
/// to access the USB interfaces.
#[derive(Clone)]
struct InterfaceManager {
    interface_available: Arc<Condvar>,
    state: Arc<Mutex<InterfaceManagerState>>,
}

impl InterfaceManager {
    fn new(
        handle: Arc<rusb::DeviceHandle<Context>>,
        usb_config: u8,
        mut interfaces: Vec<ClaimedInterface>,
    ) -> Self {
        let mut heap: BinaryHeap<ClaimedInterface> = BinaryHeap::with_capacity(interfaces.len());
        for mut interface in interfaces.drain(..) {
            interface.release().unwrap_or_else(|e| {
                error!(
                    "Failed to release interface {}: {}",
                    interface.descriptor.interface_number, e
                );
            });
            // Every interface goes into the heap regardless of success.
            heap.push(interface);
        }
        Self {
            interface_available: Arc::new(Condvar::new()),
            state: Arc::new(Mutex::new(InterfaceManagerState {
                interfaces: heap,
                handle,
                usb_config,
                active: 0,
                pending_cleanup: false,
                next_cleanup: Instant::now(),
                terminate_cleanup: false,
                cleanup_thread: None,
            })),
        }
    }

    /// Start a separate thread to release interfaces.  Interfaces are released once
    /// USB_CLEANUP_TIMEOUT elapses with no activity after all interfaces are internally
    /// returned.  This thread will be stopped when the returned CleanupGuard is dropped.
    fn start_cleanup_thread(&mut self) -> Result<CleanupGuard> {
        let manager = self.clone();

        let handle = thread::Builder::new()
            .name("interface cleanup".into())
            .spawn(move || {
                debug!("Cleanup thread starting");
                let mut state = manager.state.lock().unwrap();

                // We wait in two phases:
                // 1. As long as no cleanup is pending or there is an active interface, we need to
                //    wait indefinitely.  Each interface sets pending_cleanup when it is returned,
                //    so this will eventually drop to 0 active interfaces with a cleanup pending.
                // 2. Once cleanup is needed and there are no active interfaces, we wait for a
                //    timeout.  If another connection comes during the timeout, we go back to the
                //    previous phase.
                'outer: loop {
                    // Phase 1: Wait for cleanup to be needed and all active interfaces to be
                    // returned.
                    state = manager
                        .interface_available
                        .wait_while(state, |t| {
                            !t.terminate_cleanup && (t.active > 0 || !t.pending_cleanup)
                        })
                        .unwrap();
                    if state.terminate_cleanup {
                        break;
                    }

                    // Phase 2: Wait for the cleanup time to arrive.
                    // 1. If an interface is claimed and returned during the timeout, we will
                    //    detect this because the timeout will be extended.  We can simply wait
                    //    more if this happens.
                    // 2. If an interface is claimed and is still active after our timeout, there's
                    //    no need to keep waking up.  Go back to phase 1 and wait for the active
                    //    interfaces to be returned.
                    // 3. Once everything remains the same for the whole timeout period, we can
                    //    exit the while loop and release all the interfaces.
                    while state.next_cleanup > Instant::now() {
                        let wait = state.next_cleanup - Instant::now();
                        let result = manager
                            .interface_available
                            .wait_timeout_while(state, wait, |t| {
                                !t.terminate_cleanup && t.active == 0
                            })
                            .unwrap();
                        state = result.0; // Throw away the timed out part of the result.
                        if state.terminate_cleanup {
                            break 'outer;
                        }
                        if state.active > 0 {
                            continue 'outer;
                        }
                    }

                    // Now we know that there must be no active clients and the cleanup time must
                    // have arrived:
                    // 1.  If there were an active client, the check inside the while loop above
                    //     would have gone back to the outer loop.
                    // 2.  If the cleanup time hadn't arrived, the inner while loop wouldn't have
                    //     exited.
                    // This means we can safely release all the interfaces without affecting any
                    // active connections.
                    assert_eq!(state.active, 0, "Active interfaces not expected");
                    assert!(
                        Instant::now() >= state.next_cleanup,
                        "Cleanup time not arrived"
                    );
                    debug!("Releasing all USB interfaces");
                    match state.release_all() {
                        Ok(()) => {}

                        // If the device was unplugged, don't bother to print an error.  We're
                        // about to exit anyway.
                        Err(Error::ReleaseInterface(_, rusb::Error::NoDevice)) => {
                            break;
                        }

                        // If this failed for some other reason, we're in some bad state.  There
                        // are no active interfaces, so restarting won't interrupt an active
                        // connection.  We should just quit and let upstart reset us back to a
                        // known good state.
                        Err(e) => panic!("Failed to release interfaces: {}", e),
                    };
                    state.pending_cleanup = false;
                }

                trace!("Cleanup thread terminating");
            })
            .map_err(Error::CleanupThread)?;

        trace!("Cleanup thread {:?} started", handle.thread().id());
        let mut state = self.state.lock().unwrap();
        state.cleanup_thread = Some(handle);
        Ok(CleanupGuard::new(self.clone()))
    }

    /// Stop the cleanup thread if it was previously started by `start_cleanup_thread`.  After
    /// stopping the thread, attempt to release all interfaces.
    fn stop_cleanup_thread(&mut self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.terminate_cleanup = true;
        let cleanup_thread = state.cleanup_thread.take();
        self.interface_available.notify_all();
        drop(state); // Release the lock so the cleanup thread can make progress.
        if let Some(handle) = cleanup_thread {
            let tid = handle.thread().id();
            handle.join().map_err(|_| {
                Error::CleanupThread(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to join cleanup thread",
                ))
            })?;
            trace!("Cleanup thread {:?} exited", tid);
        }
        // The thread is guaranteed to be done, so release all the interfaces.  This may produce
        // errors if the device has been unplugged or gets shut down when interfaces have already
        // been released; no need to log these cases.
        match self.state.lock().unwrap().release_all() {
            Ok(_) => {}
            Err(Error::ReleaseInterface(_, rusb::Error::NoDevice)) => {}
            Err(Error::ReleaseInterface(_, rusb::Error::NotFound)) => {}
            Err(e) => error!("Failed to release interfaces: {}", e),
        }

        Ok(())
    }

    /// Get an interface from the pool of tracked interfaces.
    /// Will block until an interface is available.
    /// If interfaces are currently not claimed, will first set the active device
    /// configuration and re-claim USB interfaces.
    fn request_interface(&mut self) -> Result<ClaimedInterface> {
        let mut state = self.state.lock().unwrap();

        if state.active == 0 && !state.pending_cleanup {
            debug!("Claiming all interfaces");
            state.claim_all()?;
            state.pending_cleanup = true;
        }
        state.active += 1;

        loop {
            if let Some(interface) = state.interfaces.pop() {
                debug!(
                    "* Using interface {}",
                    interface.descriptor.interface_number
                );
                return Ok(interface);
            }

            state = self.interface_available.wait(state).unwrap();
        }
    }

    /// Return an interface to the pool of interfaces.
    fn free_interface(&mut self, interface: ClaimedInterface) {
        debug!(
            "* Returning interface {}",
            interface.descriptor.interface_number
        );
        let mut state = self.state.lock().unwrap();
        state.interfaces.push(interface);
        state.next_cleanup = Instant::now() + USB_CLEANUP_TIMEOUT;
        state.pending_cleanup = true;
        state.active -= 1;

        // Use notify_all instead of notify_one because the cleanup thread may also be
        // waiting on this condition variable.
        self.interface_available.notify_all();
    }
}

/// An opened IPP-USB device.
///
/// `Device` itself manages a pool of IPP-USB interfaces, but does not perform I/O.  Users
/// can temporarily request a `Connection` from the `Device` using `get_connection()`. The
/// `Connection` can be used to perform I/O to the device.
#[derive(Clone)]
pub struct Device {
    verbose_log: bool,
    handle: Arc<rusb::DeviceHandle<Context>>,
    manager: InterfaceManager,
    _cleanup: Arc<CleanupGuard>,
}

impl Device {
    /// Create a new `Device` to wrap an `rusb::DeviceHandle`.
    ///
    /// The device will be reset into the correct configuration and all IPP-USB interfaces will be
    /// claimed.  A background thread will be started to manage the active interface pool.
    ///
    /// Returns an error if the device does not support IPP-USB or an error occurs during the
    /// initialization described above.
    pub fn new(verbose_log: bool, handle: rusb::DeviceHandle<Context>) -> Result<Device> {
        let handle = Arc::new(handle);
        handle
            .set_auto_detach_kernel_driver(true)
            .map_err(|e| Error::DetachDrivers(u8::MAX, e))?; // Use MAX to mean "no interface".

        let info = IppusbDeviceInfo::new(&handle.device())?;

        set_device_config(handle.as_ref(), info.config)?;

        // Open the IPP-USB interfaces.
        let mut connections = Vec::new();
        for descriptor in info.interfaces {
            handle
                .claim_interface(descriptor.interface_number)
                .map_err(|e| Error::ClaimInterface(descriptor.interface_number, e))?;
            handle
                .set_alternate_setting(descriptor.interface_number, descriptor.alternate_setting)
                .map_err(|e| Error::SetAlternateSetting(descriptor.interface_number, e))?;
            connections.push(ClaimedInterface {
                handle: handle.clone(),
                descriptor,
            });
        }

        let mut manager = InterfaceManager::new(handle.clone(), info.config, connections);
        let cleanup_thread = manager.start_cleanup_thread()?;

        Ok(Device {
            verbose_log,
            handle,
            manager,
            _cleanup: cleanup_thread.into(),
        })
    }

    /// Return the contained device.
    pub fn device(&self) -> rusb::Device<Context> {
        self.handle.device()
    }

    /// Return a `Connection` representing a claimed IPP-USB interface.
    ///
    /// The returned interface can be used for I/O with the USB device.  Returns an error if no
    /// IPP-USB interfaces are currently available or if claiming the interface fails.
    pub fn get_connection(&mut self) -> Result<Connection> {
        let interface = self.manager.request_interface()?;
        Ok(Connection::new(
            self.verbose_log,
            self.manager.clone(),
            interface,
        ))
    }
}

/// A struct representing a claimed IPP-USB interface.
///
/// The owner of this struct can communicate with the IPP-USB device via the Read and Write traits.
pub struct Connection {
    verbose_log: bool,
    manager: InterfaceManager,
    // `interface` is never None until the Connection is dropped, at which point the
    // ClaimedInterface is returned to the pool of connections in InterfaceManager.
    interface: Option<ClaimedInterface>,
}

impl Connection {
    fn new(verbose_log: bool, manager: InterfaceManager, interface: ClaimedInterface) -> Self {
        Self {
            verbose_log,
            manager,
            interface: Some(interface),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Unwrap because interface only becomes None at drop.
        let interface = self.interface.take().unwrap();
        self.manager.free_interface(interface);
    }
}

fn to_io_error(err: rusb::Error) -> io::Error {
    let kind = match err {
        rusb::Error::InvalidParam => io::ErrorKind::InvalidInput,
        rusb::Error::NotFound => io::ErrorKind::NotFound,
        rusb::Error::Timeout => io::ErrorKind::TimedOut,
        rusb::Error::Pipe => io::ErrorKind::BrokenPipe,
        rusb::Error::Interrupted => io::ErrorKind::Interrupted,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, err)
}

impl Write for &Connection {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Unwrap because interface only becomes None at drop.
        let interface = self.interface.as_ref().unwrap();
        let endpoint = interface.descriptor.out_endpoint;
        let written = interface
            .handle
            .write_bulk(endpoint, buf, USB_TRANSFER_TIMEOUT)
            .map_err(to_io_error)?;

        if self.verbose_log {
            debug!("USB write: {} bytes", written);
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Unwrap because interface only becomes None at drop.
        let interface = self.interface.as_ref().unwrap();
        let endpoint = interface.descriptor.in_endpoint;
        let start = Instant::now();
        let mut result = interface
            .handle
            .read_bulk(endpoint, buf, USB_TRANSFER_TIMEOUT)
            .map_err(to_io_error);
        let mut zero_reads = 0;

        // USB reads cannot hit EOF. We will retry after a short delay so that higher-level
        // readers can pretend this doesn't exist.
        while let Ok(0) = result {
            zero_reads += 1;
            if start.elapsed() > USB_TRANSFER_TIMEOUT {
                result = Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Timed out waiting for non-zero USB read",
                ));
                break;
            }
            thread::sleep(Duration::from_millis(10));
            result = interface
                .handle
                .read_bulk(endpoint, buf, USB_TRANSFER_TIMEOUT)
                .map_err(to_io_error);
        }
        if self.verbose_log {
            if let Ok(num) = result {
                debug!("USB read: {} bytes", num);
            }
        }

        if zero_reads > 0 {
            debug!(
                "Spent {}ms waiting for {} 0-byte USB reads",
                start.elapsed().as_millis(),
                zero_reads
            );
        }
        result
    }
}
