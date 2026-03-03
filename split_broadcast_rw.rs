//! Example using BroadcastGroup with separated read/write tasks
//!
//! This example demonstrates the updated `BroadcastGroup` where the write
//! path is owned by a dedicated task (draining a write queue) and the read
//! path enqueues replies into that queue. The WebSocket sink still uses an
//! internal forwarder to perform the actual network writes for Warp.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use tokio::net::{TcpListener, TcpStream};
use yrs_warp::compat::{
    Error as CompatError, Spawner, JoinHandle, JoinHandleTrait, JoinError,
    WebSocketConn, WsMessage, RwLock, Sink, Stream as CompatStream, AsyncMutex as CompatAsyncMutex,
    // HTTP / upgrade helpers
    Request, Response, WsUpgrade,
    // channels
    UnboundedSender, UnboundedReceiver, unbounded_channel,
};
use yrs::sync::Awareness;
use yrs::Doc;

// TimeProvider trait from yrs
use yrs::compat::time::{TimeProvider, set_time_provider};

// Time Provider Implementation (uses std::time)
// ============================================================================
struct StdTimeProvider {
    start: Instant,
}

impl StdTimeProvider {
    fn new() -> Self {
        Self { start: Instant::now() }
    }
}

impl TimeProvider for StdTimeProvider {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn monotonic_millis(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

use yrs_warp::broadcast::BroadcastGroup;
// compat imports consolidated above
use yrs_warp::AwarenessRef;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
// futures utilities not required directly here
// use compat channels instead of tokio mpsc
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ============================================================================
// Tokio Spawner Implementation
// ============================================================================

/// Wrapper for Tokio's JoinHandle
struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T: Send + 'static> JoinHandleTrait<T> for TokioJoinHandle<T> {
    fn poll_join(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let handle = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match handle.poll(cx) {
            Poll::Ready(Ok(val)) => Poll::Ready(Ok(val)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError)),
            Poll::Pending => Poll::Pending,
        }
    }
    
    fn abort(&self) {
        self.0.abort();
    }
}

/// Tokio-based spawner
#[derive(Clone, Copy)]
struct TokioSpawner;

impl Spawner for TokioSpawner {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn(future);
        JoinHandle::new(TokioJoinHandle(handle))
    }
}

// (Warp-specific adapters removed; using TCP-based websocket framing below)

// ============================================================================
// Application
// ============================================================================
type Rooms = Arc<RwLock<HashMap<String, Arc<BroadcastGroup>>>>;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build runtime");

    rt.block_on(async_main());
}

async fn async_main() {
    // Initialize the time provider (required for yrs timestamps)
    set_time_provider(Box::new(StdTimeProvider::new()));
    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
    
    // Bind TCP listener and accept raw TCP connections (will be treated as WebSocket)
    let listener = TcpListener::bind("0.0.0.0:5560").await.expect("bind failed");
    println!("🚀 Split broadcast (RW) TCP server running on tcp://0.0.0.0:5560");
    println!("📝 Connect to: ws://0.0.0.0:5560 (perform websocket client handshake)");
    println!("📦 Using BroadcastGroup with separated read/write tasks (TCP framed websocket)");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let rooms = rooms.clone();
                tokio::spawn(async move {
                    // Handle connection; room will be extracted from the HTTP Upgrade path
                    handle_tcp_connection(stream, rooms).await;
                });
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
}

// Warp handler removed for TCP example

async fn get_or_create_room(room: &str, rooms: &Rooms) -> Arc<BroadcastGroup> {
    // First try read lock
    {
        let rooms_read = rooms.read_async().await;
        if let Some(bcast) = rooms_read.get(room) {
            return bcast.clone();
        }
    }
    
    // Need to create new room
    let mut rooms_write = rooms.write_async().await;
    
    // Double-check (another task might have created it)
    if let Some(bcast) = rooms_write.get(room) {
        return bcast.clone();
    }
    
    // Create new awareness/doc for this room
    let doc = Doc::new();
    let awareness: AwarenessRef = Arc::new(Awareness::new(doc));
    
    // Create broadcast group with spawner for async awareness updates
    let bcast = Arc::new(BroadcastGroup::new_with_spawner(awareness, 32, &TokioSpawner));
    
    rooms_write.insert(room.to_string(), bcast.clone());
    println!("📁 Created new room: {}", room);
    
    bcast
}

async fn handle_tcp_connection(mut stream: TcpStream, rooms: Rooms) {

    // Perform a minimal HTTP WebSocket upgrade handshake (clients will send an HTTP Upgrade)
    // Read the initial request bytes and respond with the upgrade response.
    let mut req_buf = vec![0u8; 4096];
    let n = match stream.read(&mut req_buf).await {
        Ok(n) => n,
        Err(e) => { eprintln!("handshake read failed: {}", e); return; }
    };

    let (request, _) = match Request::parse(&req_buf[..n]) {
        Ok(Some(r)) => r,
        Ok(None) => {
            let resp = Response::bad_request().text("Incomplete request");
            let _ = stream.write_all(&resp.encode()).await;
            return;
        }
        Err(_) => {
            let resp = Response::bad_request().text("Invalid HTTP request");
            let _ = stream.write_all(&resp.encode()).await;
            return;
        }
    };

    if !request.is_websocket_upgrade() {
        let resp = Response::bad_request().text("Expected WebSocket");
        let _ = stream.write_all(&resp.encode()).await;
        return;
    }

    let ws_key = match request.websocket_key() {
        Some(k) => k,
        None => {
            let resp = Response::bad_request().text("Missing Sec-WebSocket-Key");
            let _ = stream.write_all(&resp.encode()).await;
            return;
        }
    };

    let upgrade_response = WsUpgrade::build_upgrade_response(ws_key);
    if let Err(e) = stream.write_all(&upgrade_response).await {
        eprintln!("failed to write upgrade response: {}", e);
        return;
    }

    // Extract room name from the request path. Expected form: /collaboration/<room>
    let room: String = {
        let parts: Vec<&str> = request.path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.len() >= 2 && parts[0] == "collaboration" {
            parts[1].to_string()
        } else if parts.len() >= 1 {
            // Fallback to last segment if pattern doesn't match exactly
            parts[parts.len() - 1].to_string()
        } else {
            let resp = Response::bad_request().text("Missing room in path");
            let _ = stream.write_all(&resp.encode()).await;
            return;
        }
    };

    // Get or create the broadcast group for this room
    let bcast = get_or_create_room(&room, &rooms).await;
    println!("✅ Accepted TCP client for room: {}", room);

    // Split the TCP stream into independent reader and writer halves and
    // run separate tasks for reading and writing to demonstrate concurrency.
    let (read_half, write_half) = stream.into_split();

    // Reader: owns a WebSocketConn for parsing incoming frames
    struct WsReader {
        read: tokio::net::tcp::OwnedReadHalf,
        ws: WebSocketConn,
        buf: Vec<u8>,
    }

    impl WsReader {
        fn new(read: tokio::net::tcp::OwnedReadHalf) -> Self {
            Self { read, ws: WebSocketConn::server(), buf: vec![0u8; 4096] }
        }

        async fn run(mut self, read_tx: UnboundedSender<Vec<u8>>, write_ctrl: UnboundedSender<Outbound>) {
            loop {
                match self.read.read(&mut self.buf).await {
                    Ok(0) => { eprintln!("[reader] socket closed"); break; }
                    Ok(n) => {
                        let slice = &self.buf[..n];
                        let hex: String = slice.iter().take(64).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ");
                        eprintln!("[reader] <-- {} bytes{}: {}", n, if n>64 {" (truncated)"} else {""}, hex);
                        self.ws.feed(slice);
                        loop {
                            match self.ws.next_message() {
                                Ok(Some(WsMessage::Binary(data))) => { let _ = read_tx.send(data); }
                                Ok(Some(WsMessage::Text(text))) => { let _ = read_tx.send(text.into_bytes()); }
                                Ok(Some(WsMessage::Ping(payload))) => {
                                    let _ = write_ctrl.send(Outbound::Pong(payload));
                                }
                                Ok(Some(WsMessage::Pong(_))) => { /* ignore */ }
                                Ok(Some(WsMessage::Close(_))) => { let _ = write_ctrl.send(Outbound::Close); eprintln!("[reader] received Close"); return; }
                                Ok(None) => break,
                                Err(e) => { eprintln!("[reader] parse error: {:?}", e); return; }
                            }
                        }
                    }
                    Err(e) => { eprintln!("[reader] read error: {}", e); break; }
                }
            }
        }
    }

    // Writer: owns a WebSocketConn for encoding and the write half
    struct WsWriter {
        write: tokio::net::tcp::OwnedWriteHalf,
        ws: WebSocketConn,
    }

    impl WsWriter {
        fn new(write: tokio::net::tcp::OwnedWriteHalf) -> Self {
            Self { write, ws: WebSocketConn::server() }
        }

        async fn run(mut self, mut write_rx: UnboundedReceiver<Outbound>) {
            while let Some(out) = write_rx.recv().await {
                let msg = match out {
                    Outbound::Binary(d) => WsMessage::Binary(d),
                    Outbound::Pong(d) => WsMessage::Pong(d),
                    Outbound::Close => {
                        let frame = self.ws.encode(&WsMessage::Close(None));
                        let hex: String = frame.iter().take(64).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ");
                        eprintln!("[writer] --> {} bytes{}: {}", frame.len(), if frame.len()>64 {" (truncated)"} else {""}, hex);
                        if let Err(e) = self.write.write_all(&frame).await { eprintln!("[writer] write error: {}", e); }
                        return;
                    }
                };

                let frame = self.ws.encode(&msg);
                let hex: String = frame.iter().take(64).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ");
                eprintln!("[writer] --> {} bytes{}: {}", frame.len(), if frame.len()>64 {" (truncated)"} else {""}, hex);
                if let Err(e) = self.write.write_all(&frame).await { eprintln!("[writer] write error: {}", e); return; }
            }
        }
    }

    // Channels for writer and reader
    // Outbound messages can be application binary or control frames (Pong/Close).
    enum Outbound {
        Binary(Vec<u8>),
        Pong(Vec<u8>),
        Close,
    }

    let (write_tx, write_rx) = unbounded_channel::<Outbound>();
    let (read_tx, read_rx) = unbounded_channel::<Vec<u8>>();

    // Spawn independent reader and writer tasks
    let reader = WsReader::new(read_half);
    let writer = WsWriter::new(write_half);

    let reader_write_tx = write_tx.clone();
    let reader_task = tokio::spawn(async move { reader.run(read_tx.clone(), reader_write_tx).await });
    let writer_task = tokio::spawn(async move { writer.run(write_rx).await });

    // Build sink and stream adapters
    struct TcpSink { tx: UnboundedSender<Outbound> }
    impl Sink<Vec<u8>> for TcpSink {
        type Error = CompatError;
        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
            if self.tx.send(Outbound::Binary(item)).is_err() {
                eprintln!("[TcpSink] write queue closed when sending binary");
                return Err(CompatError::other("write queue closed"));
            }
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
    }

    struct TcpStreamWrapper { inner: UnboundedReceiver<Vec<u8>> }
    impl CompatStream for TcpStreamWrapper {
        type Item = Result<Vec<u8>, CompatError>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Fast-path: try non-blocking receive first
            if let Some(data) = self.inner.try_recv() {
                return Poll::Ready(Some(Ok(data)));
            }

            // Otherwise, poll the async recv future from compat channel
            let mut fut = self.inner.recv();
            match Pin::new(&mut fut).poll(cx) {
                Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(data))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    let sink = Arc::new(CompatAsyncMutex::new(TcpSink { tx: write_tx }));
    let stream = TcpStreamWrapper { inner: read_rx };

    // Subscribe to broadcast group using the same API as Warp variant
    let sub = bcast.subscribe_with_spawner(sink, stream, &TokioSpawner);

    match sub.completed().await {
        Ok(_) => println!("✅ Client disconnected gracefully from room: {}", room),
        Err(e) => eprintln!("❌ Client disconnected with error from room {}: {:?}", room, e),
    }

    // Abort reader/writer tasks on subscription completion
    reader_task.abort();
    writer_task.abort();
}
