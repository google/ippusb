// Copyright 2025 The ChromiumOS Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use std::convert::Infallible;

use hyper::http::StatusCode;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Body, Request, Response};
use log::{debug, error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle as AsyncHandle;
use tokio::sync::mpsc;

use crate::device::{Connection, Device};
use crate::error::Error;
use crate::http::handle_request;

/// Reason for shutting down the proxy.
///
/// When a `ShutdownReason` is received in `Bridge::run()`, it will stop accepting incoming
/// connections and exit cleanly.  The specific reason is used for logging, but the behavior is the
/// same no matter which one is received.
#[derive(Debug)]
pub enum ShutdownReason {
    /// An unspecified error occurred.
    Error,

    /// The main process received a shutdown signal.
    Signal,

    /// The IPP-USB device was unplugged.
    Unplugged,
}

/// The main HTTP proxy.
///
/// `Bridge` waits for incoming connections. It forwards HTTP requests from the TCP socket to the
/// USB device and forwards the responses back.  It ensures that the complete response is read from
/// USB even if the client disconnects without reading the full response.
pub struct Bridge {
    verbose_log: bool,
    num_clients: usize,

    shutdown: mpsc::Receiver<ShutdownReason>,
    listener: TcpListener,
    usb: Device,
    handle: AsyncHandle,
}

impl Bridge {
    /// Create a new `Bridge`.  The `Bridge` does not begin forwarding traffic until `run()` is
    /// called.
    ///
    /// * `verbose_log`: If true, log HTTP headers, additional info about HTTP bodies, and progress
    ///    messages.
    /// * `shutdown`: When this receives a value, stop processing new connections.
    /// * `listener`: A listening TCP socket for new incoming connections.
    /// * `usb`: An open device that supports IPP-USB.
    /// * `handle`: A tokio runtime Handle where new tasks will be spawned.
    pub fn new(
        verbose_log: bool,
        shutdown: mpsc::Receiver<ShutdownReason>,
        listener: TcpListener,
        usb: Device,
        handle: AsyncHandle,
    ) -> Self {
        Self {
            verbose_log,
            num_clients: 0,
            shutdown,
            listener,
            usb,
            handle,
        }
    }

    /// Run the HTTP proxy loop.
    ///
    /// `Bridge` listens for new connections.  When a connection arrives, it reads the full HTTP
    /// header.  It generates an outgoing request with the same headers minus a few that don't make
    /// sense over USB.  If the body is small, `Bridge` reads the entire body.  If the body is
    /// large or the request uses the chunked Transfer-Encoding, the downstream request will use
    /// the chunked Transfer-Encoding.  `Bridge` then claims an IPP-USB interface, sends the
    /// request headers, and streams the request body.
    ///
    /// After the request has been fully sent, `Bridge` reads the full HTTP headers from the device
    /// and generates an outgoing response back to the client with the same headers.  Next,
    /// `Bridge` streams the full body from the USB device back to the caller.  It does not change
    /// the Transfer-Encoding of the response.  If the caller drops the connection without reading
    /// the full response, `Bridge` reads the rest of the response anyway and discards it.
    ///
    /// After reading the full response, the USB interface is placed in a pool of interfaces that
    /// can be released.  If another request arrives quickly, an already-claimed interface will be
    /// reused from the pool instead of claiming a new interface.  If no requests arrive after a
    /// timeout, a separate thread will release the USB interfaces in the pool to enable sharing
    /// with other software that may be trying to communicate with the same device.
    ///
    /// If `shutdown` receives a value, `Bridge` will stop responding to requests and `run()` will
    /// return.  Previously started requests that are still in progress will finish even after
    /// `run()` returns; this ensures that the device is not left with a partially read response.
    pub async fn run(&mut self) {
        'poll: loop {
            tokio::select! {
                shutdown_type = self.shutdown.recv() => {
                    info!(
                        "Shutdown event received: {:?}",
                        shutdown_type.unwrap_or(ShutdownReason::Error));
                    break 'poll;
                }

                c = self.listener.accept() => {
                    match c {
                        Ok((stream, addr)) => {
                            info!("Connection opened from {}", addr);
                            self.handle_connection(stream);
                        }
                        Err(err) => error!("Failed to accept connection: {}", err),
                    }
                }
            }
        }
    }

    async fn service_request(
        verbose: bool,
        usb: Option<Connection>,
        request: Request<Body>,
        handle: AsyncHandle,
    ) -> std::result::Result<Response<Body>, Infallible> {
        if usb.is_none() {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap());
        }
        let usb = usb.unwrap();

        handle_request(verbose, usb, request, handle)
            .await
            .or_else(|err| {
                error!("Request failed: {}", err);
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap())
            })
    }

    fn handle_connection(&mut self, stream: TcpStream) {
        let mut thread_usb = self.usb.clone();
        let verbose = self.verbose_log;
        self.num_clients += 1;
        let client_num = self.num_clients;
        let async_handle = self.handle.clone();

        self.handle.spawn(async move {
            if verbose {
                debug!("Connection {} opened", client_num);
            }
            if let Err(http_err) = http1::Builder::new()
                .title_case_headers(true)
                .preserve_header_case(true)
                .keep_alive(false)
                .serve_connection(
                    stream,
                    service_fn(move |req| {
                        // We would normally want to extract usb_conn and return early if it's an
                        // error, but that doesn't work here because we can't match the return type
                        // of Bridge::service_request.  Instead, convert to an Option and handle a
                        // missing value in service_request.
                        let usb_conn = thread_usb
                            .get_connection()
                            .inspect_err(|err| {
                                error!("Getting USB connection failed: {}", err);
                            })
                            .ok();

                        Bridge::service_request(verbose, usb_conn, req, async_handle.clone())
                    }),
                )
                .await
            {
                error!("Error serving HTTP connection: {}", http_err);
            }
            if verbose {
                debug!("Connection {} closed", client_num);
            }
            Ok::<(), Error>(())
        });
    }
}
