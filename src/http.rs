// Copyright 2020 The ChromiumOS Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use std::fmt;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::thread;
use std::time::Duration;

use hyper::body::{self, Buf, Bytes, HttpBody};
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Body, Method, Response, StatusCode};
use log::{debug, error, info, trace};
use tokio::runtime::Handle as AsyncHandle;
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::JoinError;

use crate::device::Connection;
use crate::io_adapters::{ChunkedWriter, CompleteReader, LoggingReader, LoggingWriter};

// Minimum Request body size, in bytes, before we switch to forwarding requests
// using a chunked Transfer-Encoding.
const CHUNKED_THRESHOLD: usize = 1 << 15;
const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

#[derive(Debug)]
pub(crate) enum Error {
    DuplicateBodyReader,
    EmptyField(String),
    ReadRequestBody(hyper::Error),
    WriteRequestBody(io::Error),
    MalformedRequest,
    ParseResponse(httparse::Error),
    ReadResponseHeader(io::Error),
    WriteRequestHeader(io::Error),
    ReadResponseBody(io::Error),
    WriteResponseBody(hyper::http::Error),
    ResponseBodyTimeout,
    AsyncTaskFailure(JoinError),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            DuplicateBodyReader => write!(f, "Attempted to call body_reader() multiple times."),
            EmptyField(field) => write!(f, "HTTP Response field {} was unexpectedly empty", field),
            ReadRequestBody(err) => write!(f, "Reading request body failed: {}", err),
            WriteRequestBody(err) => write!(f, "Writing request body failed: {}", err),
            MalformedRequest => write!(f, "HTTP request is malformed"),
            ParseResponse(err) => write!(f, "Failed to parse HTTP Response header: {}", err),
            ReadResponseHeader(err) => write!(f, "Reading response header failed: {}", err),
            WriteRequestHeader(err) => write!(f, "Writing request header failed: {}", err),
            ReadResponseBody(err) => write!(f, "Reading response failed: {}", err),
            WriteResponseBody(err) => write!(f, "Responding to request failed: {}", err),
            ResponseBodyTimeout => write!(f, "Failed to write chunk to body channel"),
            AsyncTaskFailure(err) => write!(f, "Failed to wait for blocking task: {}", err),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone, Debug, PartialEq)]
enum BodyLength {
    Chunked,
    Exactly(usize),
}

struct ResponseReader<R: BufRead + Sized> {
    verbose_log: bool,
    reader: R,
    body_length: BodyLength,
    header_was_read: bool,
    created_body_reader: bool,
}

impl<R> ResponseReader<R>
where
    R: BufRead + Sized,
{
    fn new(verbose_log: bool, reader: R) -> ResponseReader<R> {
        ResponseReader {
            verbose_log,
            reader,
            // Assume body is empty unless we see a header to the contrary.
            body_length: BodyLength::Exactly(0),
            header_was_read: false,
            created_body_reader: false,
        }
    }

    fn read_header(&mut self) -> Result<(StatusCode, HeaderMap)> {
        self.header_was_read = true;

        let buf = read_until_delimiter(&mut self.reader, b"\r\n\r\n")
            .map_err(Error::ReadResponseHeader)?;
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut headers);
        let (status, headers) = match response.parse(&buf).map_err(Error::ParseResponse)? {
            httparse::Status::Complete(i) if i == buf.len() => {
                let code = response
                    .code
                    .ok_or_else(|| Error::EmptyField("code".to_owned()))?;
                let status =
                    StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                let version = response
                    .version
                    .ok_or_else(|| Error::EmptyField("version".to_owned()))?;
                let reason = status.canonical_reason().unwrap_or("Unknown");
                info!("Response: HTTP/1.{} {} {}", version, code, reason);
                if self.verbose_log {
                    // This is redundant with the line above, but it gets us a consistent set of
                    // returned lines in verbose mode.
                    debug!("< HTTP/1.{} {} {}", version, code, reason);
                }
                let mut parsed_headers = HeaderMap::new();
                for header in headers.iter().take_while(|&&h| h != httparse::EMPTY_HEADER) {
                    let name = HeaderName::from_bytes(header.name.as_bytes());
                    let val = HeaderValue::from_bytes(header.value);
                    if name.is_ok() && val.is_ok() {
                        let val = val.unwrap();
                        if self.verbose_log {
                            debug!(
                                "<  {}: {}",
                                header.name,
                                val.to_str().unwrap_or("Binary data")
                            );
                        }
                        parsed_headers.append(name.unwrap(), val);
                    } else {
                        error!(
                            "Ignoring malformed header {}:{:#?}",
                            header.name, header.value
                        );
                    }
                }
                (status, parsed_headers)
            }
            _ => return Err(Error::MalformedRequest),
        };

        // Determine the size of the body content.
        if headers.contains_key(header::TRANSFER_ENCODING) {
            self.body_length = BodyLength::Chunked;
        } else if let Some(length) = request_body_length(&headers) {
            self.body_length = BodyLength::Exactly(length);
        }

        Ok((status, headers))
    }

    fn body_reader<'r>(&'r mut self) -> Result<Box<dyn Read + 'r>> {
        if self.created_body_reader {
            return Err(Error::DuplicateBodyReader);
        }

        self.created_body_reader = true;
        match self.body_length {
            BodyLength::Exactly(length) => {
                let reader = (&mut self.reader).take(length as u64);
                Ok(Box::new(CompleteReader::new(reader)))
            }
            BodyLength::Chunked => {
                let reader = chunked_transfer::Decoder::new(&mut self.reader);
                Ok(Box::new(CompleteReader::new(reader)))
            }
        }
    }
}

impl<R> Drop for ResponseReader<R>
where
    R: BufRead,
{
    fn drop(&mut self) {
        if !self.created_body_reader {
            debug!("Draining in drop");
            if !self.header_was_read {
                // Read header to figure out how long the body is.
                let _ = self.read_header();
            }

            // Create a body reader which will totally read the response on drop.
            let _ = self.body_reader();
        }
    }
}

fn is_end_to_end(header: &HeaderName) -> bool {
    let keep_alive = HeaderName::from_bytes(b"Keep-Alive").unwrap();
    !matches!(
        header,
        &header::CONNECTION
            | &header::EXPECT
            | &header::PROXY_AUTHENTICATE
            | &header::PROXY_AUTHORIZATION
            | &header::TE
            | &header::TRAILER
            | &header::TRANSFER_ENCODING
            | &header::UPGRADE
    ) && header != keep_alive
}

fn supports_request_body(method: &Method) -> bool {
    !matches!(
        *method,
        Method::GET | Method::HEAD | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}

fn request_body_length(headers: &HeaderMap) -> Option<usize> {
    let header = headers.get(header::CONTENT_LENGTH)?;
    let str_length = header.to_str().ok()?;
    str_length.trim().parse().ok()
}

struct Request {
    method: String,
    url: String,
    headers: HeaderMap,
    forwarded_body_length: BodyLength,
}

// Converts a hyper::Request into our internal Request format.
// Filter out Hop-by-hop headers and add Content-Length or Transfer-Encoding
// headers as needed.
fn rewrite_request(request: &hyper::Request<Body>) -> Request {
    let mut headers = HeaderMap::with_capacity(request.headers().len());
    // If the incoming request specifies a Transfer-Encoding, it must be chunked.
    let request_is_chunked = request.headers().contains_key(header::TRANSFER_ENCODING);

    for (header, val) in request.headers().iter().filter(|(h, _)| is_end_to_end(h)) {
        headers.append(header, val.clone());
    }

    let body_length = if !supports_request_body(request.method()) {
        BodyLength::Exactly(0)
    } else if request_is_chunked {
        BodyLength::Chunked
    } else if let Some(length) = request_body_length(request.headers()) {
        BodyLength::Exactly(length)
    } else {
        BodyLength::Exactly(0)
    };

    let user_agent = format!("ippusb_bridge/{}", VERSION.unwrap_or("unknown"));
    headers.insert(
        header::USER_AGENT,
        HeaderValue::from_str(&user_agent).unwrap(),
    );

    // If the request body is relatively small, don't use a chunked encoding for
    // the proxied request.
    let forwarded_body_length = match body_length {
        BodyLength::Exactly(length) if length < CHUNKED_THRESHOLD => body_length,
        _ => BodyLength::Chunked,
    };

    if forwarded_body_length == BodyLength::Chunked {
        // Content-Length and chunked encoding are mutually exclusive.
        // We don't need to delete any existing Transfer-Encoding since it's a
        // Hop-by-hop header and is already filtered out above.
        headers.remove(header::CONTENT_LENGTH);
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
    } else if !headers.contains_key(header::CONTENT_LENGTH) {
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    }

    Request {
        method: request.method().to_string(),
        url: request.uri().to_string(),
        headers,
        forwarded_body_length,
    }
}

fn content_type(request: &hyper::Request<Body>) -> Option<&str> {
    request.headers().get(header::CONTENT_TYPE)?.to_str().ok()
}

// Convert a HeaderName to a title-case String.
// hyper always converts header names to lowercase for performance.  Even though HTTP headers are
// supposed to be case-insensitive, some printers only handle title-case headers.  For improved
// compatibility, this allows sending the more common title-case versions.  This assumes that
// header names are ASCII, as required by the HTTP RFC.
fn title_case_header(field: &HeaderName) -> String {
    let name = field.as_str();
    let mut result = Vec::with_capacity(name.len());
    let mut upper = true;

    for c in name.chars() {
        if upper {
            upper = false;
            result.push(c.to_ascii_uppercase());
        } else {
            result.push(c);
        }
        if c == '-' {
            upper = true;
        }
    }

    result.into_iter().collect()
}

fn send_request_header(
    verbose_log: bool,
    request: &Request,
    usb: Connection,
) -> io::Result<Connection> {
    let mut writer = BufWriter::new(&usb);
    serialize_request_header(verbose_log, request, &mut writer)?;
    drop(writer);
    Ok(usb)
}

fn serialize_request_header(
    verbose_log: bool,
    request: &Request,
    writer: &mut dyn Write,
) -> io::Result<()> {
    write!(writer, "{} {} HTTP/1.1\r\n", request.method, request.url)?;
    if verbose_log {
        debug!("> {} {} HTTP/1.1\\r\n", request.method, request.url);
    }
    for (field, value) in request.headers.iter() {
        let header_name = title_case_header(field);
        write!(writer, "{}: ", header_name)?;
        writer.write_all(value.as_bytes())?;
        write!(writer, "\r\n")?;
        if verbose_log {
            debug!(
                ">  {}: {}\\r",
                header_name,
                value.to_str().unwrap_or("Binary header")
            );
        }
    }

    write!(writer, "\r\n")?;
    if verbose_log {
        debug!("> \\r");
    }
    writer.flush()?;
    Ok(())
}

// Read the response body from `response_reader` in chunks and send them to the client via
// `sender`.
fn copy_response_body<R: BufRead + Sized>(
    mut response_reader: ResponseReader<R>,
    sender: &mut body::Sender,
) -> Result<usize> {
    let mut reader = match response_reader.body_reader() {
        Ok(r) => r,
        Err(err) => {
            error!("Failed to create body reader: {}", err);
            return Err(err);
        }
    };

    // Reuse the same chunk threshold for reading the body back from the printer.  This loop does
    // not depend on the buffer size for correctness, but it seems to be a reasonable middle ground
    // between memory use and iterations.
    let mut buf = [0; CHUNKED_THRESHOLD];

    let mut copied = 0;
    loop {
        match reader.read(&mut buf) {
            Ok(0) => {
                trace!("Got EOF from USB");
                break;
            }
            Ok(num) => {
                trace!("Read {} bytes from USB", num);
                let mut to_send = Bytes::copy_from_slice(&buf[0..num]);
                let mut tries = 10;
                loop {
                    match sender.try_send_data(to_send) {
                        Ok(_) => {
                            trace!("Sent {} bytes to body channel", num);
                            copied += num;
                            break;
                        }
                        Err(remaining) => {
                            error!(
                                "Tried to send {} bytes.  Body channel did not accept bytes: {}",
                                num,
                                remaining.len()
                            );
                            // Give the remote side a brief time to read what was previously sent.
                            thread::sleep(Duration::from_millis(10));
                            to_send = remaining;
                            tries -= 1;
                            if tries == 0 {
                                error!("Failed to send bytes after 10 tries");
                                return Err(Error::ResponseBodyTimeout);
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to read from USB: {}", err);
                return Err(Error::ReadResponseBody(err));
            }
        }
    }

    Ok(copied)
}

// The request body is expected to be delivered as a sequence of readers.  For each reader received
// on the incoming channel, copy all of its bytes to the Connection.  When the input channel
// closes, return the total number of bytes copied plus the original Connection.
fn send_request_body<R: Read>(
    mut rx: Receiver<R>,
    usb: Connection,
    length: BodyLength,
    log_body: bool,
) -> Result<(Connection, u64)> {
    trace!("Starting body sender");
    let mut total = 0;
    let usb_writer =
        BufWriter::with_capacity(CHUNKED_THRESHOLD, LoggingWriter::new(&usb, log_body));
    let mut writer: Box<dyn Write> = match length {
        BodyLength::Chunked => Box::new(ChunkedWriter::new(usb_writer)),
        _ => Box::new(usb_writer),
    };
    while let Some(mut reader) = rx.blocking_recv() {
        total += io::copy(&mut reader, &mut writer).map_err(Error::WriteRequestBody)?;
    }
    writer.flush().map_err(Error::WriteRequestBody)?;
    drop(writer); // Release the borrow on usb.
    Ok((usb, total))
}

// A request is handled in 4 pieces:
//    1. Read the request headers and generate a request for the printer.  The outgoing request will
//       be similar to the incoming request except for a potentially different Transfer-Encoding.
//    2. Stream the request body to the printer if the request has a body.
//    3. Read the response headers and generate a response for the client.  The response is purely
//       passed through.
//    4. Stream the response body to the client.
//
// rusb and libusb don't support async I/O, so each task that interacts with the USB device
// needs to be run as a blocking task.  Even though the Connection is only needed by one
// task at a time and we wait for completion before continuing, we can't pass a reference
// because tokio can't guarantee that this async function stays alive long enough to keep the
// reference valid.  Instead, this function uses the pattern of passing ownership of the
// Connection to each blocking task and returning it back again when it completes.
pub(crate) async fn handle_request(
    verbose_log: bool,
    mut usb: Connection,
    request: hyper::Request<Body>,
    handle: AsyncHandle,
) -> Result<Response<Body>> {
    info!(
        "Request: {} {} {:?}",
        request.method(),
        request.uri(),
        request.version()
    );

    // Filter out headers that should not be forwarded, and update Content-Length and
    // Transfer-Encoding headers based on how the body (if any) will be transferred.
    let new_request = rewrite_request(&request);
    let log_body = verbose_log && content_type(&request).unwrap_or("").starts_with("text/");
    let forwarded_body_length = new_request.forwarded_body_length;

    let mut body = request.into_body();
    let next_buf = match new_request.forwarded_body_length {
        BodyLength::Exactly(length) => {
            // If we're not using chunked, we must have the entire request body before beginning to
            // forward the request. If we didn't and the client were to drop in the middle of
            // forwarding a request, we would have no way of cleanly terminating the connection.
            let mut buf = Vec::with_capacity(length);
            while let Some(chunk) = body.data().await {
                buf.extend(chunk.map_err(Error::ReadRequestBody)?);
            }
            Bytes::from(buf)
        }
        _ => {
            // If we're using chunked, just read enough of the body to have an initial buffer for
            // the loop below.  This is a streaming format, so we don't have any good way to buffer
            // up the exact right amount.
            match body.data().await {
                None => Err(Error::MalformedRequest),
                Some(result) => result.map_err(Error::ReadRequestBody),
            }?
        }
    };

    // We know everything needed to send the header, so send it all as one chunk.
    usb = handle
        .spawn_blocking(move || send_request_header(verbose_log, &new_request, usb))
        .await
        .map_err(Error::AsyncTaskFailure)?
        .map_err(Error::WriteRequestHeader)?;

    // We may not have read the entire body yet.  We could read the whole thing into a buffer and
    // send it along like the headers above, but the body might be quite large.  Instead, read one
    // chunk at a time and pass them through an mpsc channel to the blocking task.  This means
    // we need to join the blocking task at the end instead of directly getting the result.
    if forwarded_body_length != BodyLength::Exactly(0) {
        debug!("* Forwarding client request body");
        let (tx, rx) = mpsc::channel(2);
        let body_task = handle
            .spawn_blocking(move || send_request_body(rx, usb, forwarded_body_length, log_body));
        trace!("Copying first {} bytes to USB", next_buf.remaining());
        tx.send(next_buf.reader()).await.map_err(|_| {
            error!("Failed to send request body chunk");
            Error::WriteRequestBody(io::Error::from(io::ErrorKind::UnexpectedEof))
        })?;
        while let Some(chunk) = body.data().await {
            let next_buf = chunk.map_err(Error::ReadRequestBody)?;
            trace!("Copying {} bytes to USB", next_buf.remaining());
            tx.send(next_buf.reader()).await.map_err(|_| {
                error!("Failed to send request body chunk");
                Error::WriteRequestBody(io::Error::from(io::ErrorKind::UnexpectedEof))
            })?;
        }
        drop(tx); // Close the channel to tell the writer to finish.
        let body_result = body_task.await.map_err(Error::AsyncTaskFailure)?;
        match body_result {
            Ok((usb_out, num)) => {
                debug!("Copied {} bytes of request body to USB", num);
                usb = usb_out;
            }
            Err(err) => {
                error!("Failed to copy request body: {}", err);
                return Err(err);
            }
        }
    }

    // Now that we have written data to the printer, we must ensure that we read a complete HTTP
    // response from the printer. Otherwise, that data may remain in the printer's buffers and be
    // sent to some other client.  ResponseReader ensures that this happens internally.  Since we
    // don't need to get the Connection back for any subsequent steps, we give it away entirely
    // instead of following the earlier pattern.
    let usb_reader = BufReader::new(LoggingReader::new(usb, "printer"));
    let mut response_reader = ResponseReader::new(verbose_log, usb_reader);

    debug!("* Reading printer response header");
    let (status, headers) = response_reader.read_header()?;

    let mut builder = Response::builder().status(status);
    for (h, val) in headers.iter() {
        // Don't pass through the printer's Connection header. Force it to Connection: close below.
        if h == header::CONNECTION {
            continue;
        }
        builder = builder.header(h, val);
    }
    builder = builder.header(header::CONNECTION, "close");

    debug!("* Forwarding printer response body");
    let (mut sender, body) = Body::channel();
    handle.spawn_blocking(
        move || match copy_response_body(response_reader, &mut sender) {
            Ok(num) => debug!("Copied {} bytes of response body", num),
            Err(err) => {
                error!("Failed to copy response body: {}", err);
                sender.abort();
            }
        },
    );
    builder.body(body).map_err(Error::WriteResponseBody)
}

/// Read from `reader` until `delimiter` is seen or EOF is reached.
/// Returns read data.
fn read_until_delimiter(reader: &mut dyn BufRead, delimiter: &[u8]) -> io::Result<Vec<u8>> {
    let mut result: Vec<u8> = Vec::new();
    loop {
        let buf = match reader.fill_buf() {
            Ok(buf) => buf,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        if buf.is_empty() {
            return Ok(result);
        }

        // First check if our delimiter spans the old buffer and the new buffer.
        for split in 1..delimiter.len() {
            let (first_delimiter, second_delimiter) = delimiter.split_at(split);
            if first_delimiter.len() > result.len() || second_delimiter.len() > buf.len() {
                continue;
            }

            let first = result.get(result.len() - first_delimiter.len()..);
            let second = buf.get(..second_delimiter.len());
            if let (Some(first), Some(second)) = (first, second) {
                if first == first_delimiter && second == second_delimiter {
                    result.extend_from_slice(second);
                    reader.consume(second_delimiter.len());
                    return Ok(result);
                }
            }
        }

        // Then check if our delimiter occurs in the new buffer.
        if let Some(i) = buf
            .windows(delimiter.len())
            .position(|window| window == delimiter)
        {
            result.extend_from_slice(&buf[..i + delimiter.len()]);
            reader.consume(i + delimiter.len());
            return Ok(result);
        }

        // Otherwise just copy the entire buffer into result.
        let consumed = buf.len();
        result.extend_from_slice(buf);
        reader.consume(consumed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Version;
    use std::io::Cursor;

    #[test]
    fn body_support() {
        assert!(!supports_request_body(&Method::GET));
        assert!(!supports_request_body(&Method::HEAD));
        assert!(!supports_request_body(&Method::OPTIONS));
        assert!(!supports_request_body(&Method::DELETE));
        assert!(!supports_request_body(&Method::TRACE));
        assert!(supports_request_body(&Method::POST));
        assert!(supports_request_body(&Method::PUT));
        assert!(supports_request_body(&Method::PATCH));
        assert!(supports_request_body(&Method::from_bytes(b"TEST").unwrap()));
    }

    #[test]
    fn response_reader_invalid_status_line() {
        let payload = b"HTTP/1.1 OK\r\n\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        assert!(reader.read_header().is_err());
    }

    #[test]
    fn response_reader_invalid_http_version() {
        let payload = b"HTTP/0.9 200 OK\r\n\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        assert!(reader.read_header().is_err());
    }

    #[test]
    fn response_reader_missing_header_end() {
        let payload = b"HTTP/1.1 200 OK\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        assert!(reader.read_header().is_err());
    }

    #[test]
    fn response_reader_empty_response() {
        let payload = b"HTTP/1.1 200 OK\r\n\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        let (status, headers) = reader.read_header().expect("failed to read header");
        assert_eq!(status, 200);
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn response_reader_static_response() {
        let payload = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        let (status, headers) = reader.read_header().expect("failed to read header");
        assert_eq!(status, 200);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), &"100");
    }

    #[test]
    fn response_reader_chunked_response() {
        let payload = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        let (status, headers) = reader.read_header().expect("failed to read header");
        assert_eq!(status, 200);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get(header::TRANSFER_ENCODING).unwrap(), &"chunked");
    }

    #[test]
    fn copy_request_header() {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "text/plain".parse().unwrap());
        let request = Request {
            method: "GET".to_string(),
            url: "/eSCL/ScannerCapabilities".to_string(),
            headers,
            forwarded_body_length: BodyLength::Exactly(0),
        };

        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);

        assert!(serialize_request_header(false, &request, &mut writer).is_ok());
        drop(writer);
        assert_eq!(
            buf,
            b"GET /eSCL/ScannerCapabilities HTTP/1.1\r
Content-Type: text/plain\r
\r
"
        );
    }

    #[test]
    fn rewrite_request_no_body() {
        let request_in = hyper::Request::builder()
            .method("GET")
            .version(Version::HTTP_11)
            .uri("/eSCL/ScannerCapabilities")
            .header("Content-Type", "text/plain")
            .body(Body::empty())
            .unwrap();

        let request_out = rewrite_request(&request_in);
        assert_eq!(request_out.method, "GET");
        assert_eq!(request_out.url, "/eSCL/ScannerCapabilities");
        assert_eq!(request_out.headers.len(), 3);
        assert!(request_out.headers.contains_key("User-Agent"));
        assert!(request_out.headers.contains_key("Content-Length"));
        assert!(request_out.headers.contains_key("Content-Type"));
        assert_eq!(request_out.forwarded_body_length, BodyLength::Exactly(0));
    }

    #[test]
    fn rewrite_request_small_body() {
        let request_in = hyper::Request::builder()
            .method("POST")
            .version(Version::HTTP_11)
            .uri("/eSCL/ScannerCapabilities")
            .header("Content-Length", "4")
            .body(Body::empty())
            .unwrap();

        let request_out = rewrite_request(&request_in);
        assert_eq!(request_out.method, "POST");
        assert_eq!(request_out.url, "/eSCL/ScannerCapabilities");
        assert_eq!(request_out.headers.len(), 2);
        assert!(request_out.headers.contains_key("User-Agent"));
        assert!(request_out.headers.contains_key("Content-Length"));
        assert!(!request_out.headers.contains_key("Transfer-Encoding"));
        assert_eq!(request_out.forwarded_body_length, BodyLength::Exactly(4));
    }

    #[test]
    fn rewrite_request_large_body() {
        let request_in = hyper::Request::builder()
            .method("POST")
            .version(Version::HTTP_11)
            .uri("/eSCL/ScannerCapabilities")
            .header("Content-Length", format!("{}", CHUNKED_THRESHOLD))
            .body(Body::empty())
            .unwrap();

        let request_out = rewrite_request(&request_in);
        assert_eq!(request_out.method, "POST");
        assert_eq!(request_out.url, "/eSCL/ScannerCapabilities");
        assert_eq!(request_out.headers.len(), 2);
        assert!(request_out.headers.contains_key("User-Agent"));
        assert!(!request_out.headers.contains_key("Content-Length"));
        assert!(request_out.headers.contains_key("Transfer-Encoding"));
        assert_eq!(request_out.forwarded_body_length, BodyLength::Chunked);
    }

    #[test]
    fn rewrite_request_chunked_body() {
        let request_in = hyper::Request::builder()
            .method("POST")
            .version(Version::HTTP_11)
            .uri("/eSCL/ScannerCapabilities")
            .header("Transfer-Encoding", "chunked")
            .body(Body::empty())
            .unwrap();

        let request_out = rewrite_request(&request_in);
        assert_eq!(request_out.method, "POST");
        assert_eq!(request_out.url, "/eSCL/ScannerCapabilities");
        assert_eq!(request_out.headers.len(), 2);
        assert!(request_out.headers.contains_key("User-Agent"));
        assert!(!request_out.headers.contains_key("Content-Length"));
        assert!(request_out.headers.contains_key("Transfer-Encoding"));
        assert_eq!(request_out.forwarded_body_length, BodyLength::Chunked);
    }

    #[test]
    fn e2e_header() {
        let header = HeaderName::from_bytes(b"Content-Type").unwrap();
        assert!(is_end_to_end(&header));

        let header = HeaderName::from_bytes(b"Connection").unwrap();
        assert!(!is_end_to_end(&header));

        let header = HeaderName::from_bytes(b"Keep-Alive").unwrap();
        assert!(!is_end_to_end(&header));

        let header = HeaderName::from_bytes(b"Transfer-Encoding").unwrap();
        assert!(!is_end_to_end(&header));

        // Special case since Expect is normally end-to-end.
        let header = HeaderName::from_bytes(b"Expect").unwrap();
        assert!(!is_end_to_end(&header));
    }

    #[test]
    fn extract_content_type() {
        let request = hyper::Request::builder().body(Body::empty()).unwrap();
        assert!(content_type(&request).is_none());

        let request = hyper::Request::builder()
            .header("content-TYPE", "text/html")
            .body(Body::empty())
            .unwrap();
        assert!(content_type(&request).is_some());
    }

    #[test]
    fn body_length_no_header() {
        let headers = HeaderMap::new();
        assert!(request_body_length(&headers).is_none());
    }

    #[test]
    fn body_length_nonascii_header() {
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(header::CONTENT_LENGTH, "\u{80}".parse().unwrap());
        assert!(request_body_length(&headers).is_none());
    }

    #[test]
    fn body_length_invalid_number() {
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(header::CONTENT_LENGTH, "xyz".parse().unwrap());
        assert!(request_body_length(&headers).is_none());
    }

    #[test]
    fn body_length_zero_header() {
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(header::CONTENT_LENGTH, "0".parse().unwrap());
        assert_eq!(request_body_length(&headers), Some(0));
    }

    #[test]
    fn body_length_nonzero_header() {
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(header::CONTENT_LENGTH, "32768".parse().unwrap());
        assert_eq!(request_body_length(&headers), Some(32768));
    }

    #[tokio::test]
    async fn copy_response_empty() {
        let payload = b"HTTP/1.1 200 OK\r\n\r\n";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        let (status, headers) = reader.read_header().expect("should read headers");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.len(), 0);

        let (mut sender, body) = Body::channel();
        #[allow(deprecated)]
        let bytes_task = tokio::spawn(async move { hyper::body::to_bytes(body).await });

        let len = tokio::task::spawn_blocking(move || copy_response_body(reader, &mut sender))
            .await
            .expect("failed to join copy_response_body task")
            .expect("failed to copy body");
        assert_eq!(len, 0);

        let bytes = bytes_task
            .await
            .expect("failed to join to_bytes task")
            .expect("failed to read body");
        assert_eq!(bytes, b""[..]);
    }

    #[tokio::test]
    async fn copy_response_static() {
        let payload =
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 8\r\n\r\ntestbody";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        let (status, headers) = reader.read_header().expect("should read headers");
        assert_eq!(status, StatusCode::OK);
        assert!(headers.contains_key(header::CONTENT_LENGTH));
        assert!(!headers.contains_key(header::TRANSFER_ENCODING));
        assert_eq!(
            headers
                .get(header::CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            "8"
        );

        let (mut sender, body) = Body::channel();
        #[allow(deprecated)]
        let bytes_task = tokio::spawn(async move { hyper::body::to_bytes(body).await });

        let len = tokio::task::spawn_blocking(move || copy_response_body(reader, &mut sender))
            .await
            .expect("failed to join copy_response_body task")
            .expect("failed to copy body");
        assert_eq!(len, 8);

        let bytes = bytes_task
            .await
            .expect("failed to join to_bytes task")
            .expect("failed to read body");
        assert_eq!(bytes, b"testbody"[..]);
    }

    #[tokio::test]
    async fn copy_response_chunked() {
        let payload = b"HTTP/1.1 200 OK\r
Content-Type: text/plain\r
Transfer-Encoding: chunked\r
\r
4\r
test\r
4\r
body\r
0\r
\r
\r";
        let mut reader = ResponseReader::new(false, BufReader::new(&payload[..]));
        let (status, headers) = reader.read_header().expect("should read headers");
        assert_eq!(status, StatusCode::OK);
        assert!(!headers.contains_key(header::CONTENT_LENGTH));
        assert!(headers.contains_key(header::TRANSFER_ENCODING));
        assert_eq!(
            headers
                .get(header::TRANSFER_ENCODING)
                .unwrap()
                .to_str()
                .unwrap(),
            "chunked"
        );

        let (mut sender, body) = Body::channel();
        #[allow(deprecated)]
        let bytes_task = tokio::spawn(async move { hyper::body::to_bytes(body).await });

        let len = tokio::task::spawn_blocking(move || copy_response_body(reader, &mut sender))
            .await
            .expect("failed to join copy_response_body task")
            .expect("failed to copy body");
        assert_eq!(len, 8);

        let bytes = bytes_task
            .await
            .expect("failed to join to_bytes task")
            .expect("failed to read body");
        assert_eq!(bytes, b"testbody"[..]);
    }

    #[test]
    fn test_read_until_delimiter() {
        let mut source = Cursor::new(&b"abdcdef"[..]);
        let v = read_until_delimiter(&mut source, b"20").unwrap();
        assert_eq!(v, b"abdcdef");

        let mut source = Cursor::new(&b"abdcdef"[..]);
        let v = read_until_delimiter(&mut source, b"de").unwrap();
        assert_eq!(v, b"abdcde");

        let mut source = Cursor::new(&b"abdcdef"[..]);
        let v = read_until_delimiter(&mut source, b"dc").unwrap();
        assert_eq!(v, b"abdc");

        let mut source = Cursor::new(&b"abdcdef"[..]);
        let v = read_until_delimiter(&mut source, b"abd").unwrap();
        assert_eq!(v, b"abd");

        let mut source = BufReader::with_capacity(2, Cursor::new(&b"abdcdeffegh"[..]));
        let v = read_until_delimiter(&mut source, b"bdc").unwrap();
        assert_eq!(v, b"abdc");

        let v = read_until_delimiter(&mut source, b"ef").unwrap();
        assert_eq!(v, b"def");

        let v = read_until_delimiter(&mut source, b"g").unwrap();
        assert_eq!(v, b"feg");
    }
}
