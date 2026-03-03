//! Memory Test: no_std crates (yrs_no_std + yrs-warp-nostd)
//!
//! This example tracks heap allocation metrics to compare memory usage
//! with the standard yrs/yrs-warp implementation.
//!
//! Run with: cargo run --example memory_test_nostd

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use yoffice_rust_api::{ledger_request, ledger_response};
// yrs no_std imports
use yrs::compat::time::{set_time_provider, TimeProvider};
use yrs::sync::Awareness;
use yrs::Doc;

// yrs-warp-nostd imports
use yrs_warp::broadcast::BroadcastGroup;
use yrs_warp::compat::{
    bounded_channel, unbounded_channel, BoundedReceiver, BoundedSender, JoinError, JoinHandle,
    JoinHandleTrait, Request, RwLock, Sink, Spawner, Stream, UnboundedReceiver, UnboundedSender,
    WebSocketConn, WsMessage, WsUpgrade,
};
use yrs_warp::AwarenessRef;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

// ============================================================================
// Global Heap Metrics
// ============================================================================

static HEAP_ALLOC: AtomicUsize = AtomicUsize::new(0);
static HEAP_DEALLOC: AtomicUsize = AtomicUsize::new(0);
static PEAK_USAGE: AtomicUsize = AtomicUsize::new(0);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Maximum allowed heap usage: 1 GB
const HEAP_LIMIT: usize = 1024 * 1024 * 1024;

fn update_peak() {
    let current = HEAP_ALLOC
        .load(Ordering::Relaxed)
        .saturating_sub(HEAP_DEALLOC.load(Ordering::Relaxed));
    let mut peak = PEAK_USAGE.load(Ordering::Relaxed);
    while current > peak {
        match PEAK_USAGE.compare_exchange_weak(peak, current, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(p) => peak = p,
        }
    }
}

fn log_heap_stats(label: &str) {
    let alloc = HEAP_ALLOC.load(Ordering::Relaxed);
    let dealloc = HEAP_DEALLOC.load(Ordering::Relaxed);
    let current = alloc.saturating_sub(dealloc);
    let peak = PEAK_USAGE.load(Ordering::Relaxed);
    let alloc_ops = ALLOC_COUNT.load(Ordering::Relaxed);
    let dealloc_ops = DEALLOC_COUNT.load(Ordering::Relaxed);

    println!("=== HEAP STATS [{}] ===", label);
    println!(
        "  Total allocated:   {:>12} bytes ({:.2} MB)",
        alloc,
        alloc as f64 / 1_048_576.0
    );
    println!(
        "  Total deallocated: {:>12} bytes ({:.2} MB)",
        dealloc,
        dealloc as f64 / 1_048_576.0
    );
    println!(
        "  Current usage:     {:>12} bytes ({:.2} MB)",
        current,
        current as f64 / 1_048_576.0
    );
    println!(
        "  Peak usage:        {:>12} bytes ({:.2} MB)",
        peak,
        peak as f64 / 1_048_576.0
    );
    println!("  Alloc operations:  {:>12}", alloc_ops);
    println!("  Dealloc operations:{:>12}", dealloc_ops);
    println!();
}

fn reset_heap_stats() {
    HEAP_ALLOC.store(0, Ordering::Relaxed);
    HEAP_DEALLOC.store(0, Ordering::Relaxed);
    PEAK_USAGE.store(0, Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    DEALLOC_COUNT.store(0, Ordering::Relaxed);
}

// ============================================================================
// Tracking Allocator
// ============================================================================

use std::alloc::{GlobalAlloc, Layout, System};

struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Check if we would exceed the 1GB limit
        let current = HEAP_ALLOC
            .load(Ordering::Relaxed)
            .saturating_sub(HEAP_DEALLOC.load(Ordering::Relaxed));
        if current + layout.size() > HEAP_LIMIT {
            eprintln!(
                "HEAP LIMIT EXCEEDED! Current: {} bytes ({:.2} MB), requested: {} bytes",
                current,
                current as f64 / 1_048_576.0,
                layout.size()
            );
            // Return null to signal allocation failure
            return std::ptr::null_mut();
        }

        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            HEAP_ALLOC.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            update_peak();
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP_DEALLOC.fetch_add(layout.size(), Ordering::Relaxed);
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            HEAP_DEALLOC.fetch_add(layout.size(), Ordering::Relaxed);
            HEAP_ALLOC.fetch_add(new_size, Ordering::Relaxed);
            update_peak();
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

// ============================================================================
// Time Provider
// ============================================================================

struct StdTimeProvider {
    start: Instant,
}

impl StdTimeProvider {
    fn new() -> Self {
        Self {
            start: Instant::now(),
        }
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

// ============================================================================
// Tokio Spawner
// ============================================================================

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

#[derive(Clone, Copy)]
struct TokioSpawner;

impl Spawner for TokioSpawner {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle::new(TokioJoinHandle(tokio::spawn(future)))
    }
}

// ============================================================================
// Room Management
// ============================================================================

type Rooms = Arc<RwLock<HashMap<String, Arc<BroadcastGroup>>>>;

async fn get_or_create_room(room: &str, rooms: &Rooms) -> Arc<BroadcastGroup> {
    {
        let rooms_read = rooms.read_async().await;
        if let Some(bcast) = rooms_read.get(room) {
            return bcast.clone();
        }
    }

    let mut rooms_write = rooms.write_async().await;
    if let Some(bcast) = rooms_write.get(room) {
        return bcast.clone();
    }

    let doc = Doc::new();
    let awareness: AwarenessRef = Arc::new(Awareness::new(doc));
    let bcast = Arc::new(BroadcastGroup::new_with_spawner(
        awareness,
        1000,
        &TokioSpawner,
    ));
    rooms_write.insert(room.to_string(), bcast.clone());

    println!("Created new room: {}", room);
    bcast
}

// ============================================================================
// Connection Handler
// ============================================================================

// Channels for writer and reader
enum Outbound {
    Binary(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

struct TcpSink {
    tx: BoundedSender<Outbound>,
}
impl Sink<Vec<u8>> for TcpSink {
    type Error = yrs_warp::compat::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.tx
            .poll_ready(cx)
            .map_err(|_| yrs_warp::compat::Error::other("write queue closed"))
    }
    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        if self.tx.try_send(Outbound::Binary(item)).is_err() {
            eprintln!("[TcpSink] write queue closed");
            return Err(yrs_warp::compat::Error::other("write queue closed"));
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct TcpStreamWrapper {
    inner: BoundedReceiver<Vec<u8>>,
}
impl Stream for TcpStreamWrapper {
    type Item = Result<Vec<u8>, yrs_warp::compat::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut fut = self.inner.recv();
        match unsafe { Pin::new_unchecked(&mut fut) }.poll(cx) {
            Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(data))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
pub fn form_response_from_payload(
    payload: Vec<u8>,
    id: Option<u64>,
    status: ledger_response::Status,
) -> ledger_response::Response {
    ledger_response::Response {
        id,
        operation: Some(ledger_response::response::Operation::YofficeSend(
            ledger_response::YofficeSend {
                status: status as i32,
                payload,
            },
        )),
    }
}
// use yoffice_rust_api::ledger_request::Request;
use prost::Message;
async fn handle_connection(stream: TcpStream, rooms: Rooms) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let _ = stream.set_nodelay(true);

    let (mut read_half, mut write_half) = stream.into_split();

    // Read HTTP upgrade request
    let mut req_buf = vec![0u8; 4096];
    let n = match read_half.read(&mut req_buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let (request, _) = match Request::parse(&req_buf[..n]) {
        Ok(Some(r)) => r,
        _ => return,
    };

    if !request.is_websocket_upgrade() {
        return;
    }

    let ws_key = match request.websocket_key() {
        Some(k) => k,
        None => return,
    };

    // Extract room
    let room: String = {
        let parts: Vec<&str> = request.path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.len() >= 2 && parts[0] == "collaboration" {
            parts[1].to_string()
        } else {
            "default".to_string()
        }
    };

    let upgrade_response = WsUpgrade::build_upgrade_response(ws_key);
    if write_half.write_all(&upgrade_response).await.is_err() {
        return;
    }

    let bcast = get_or_create_room(&room, &rooms).await;

    println!("[{}] Client connected to room: {}", peer_addr, room);
    log_heap_stats(&format!("After connect {}", peer_addr));

    // Channels for framing tasks
    // Channels for framing tasks
    let (write_tx, write_rx) = bounded_channel::<Outbound>(32);
    let (read_tx, read_rx) = bounded_channel::<Vec<u8>>(32);

    // Spawn FRAMING tasks
    struct WsReader {
        read: tokio::net::tcp::OwnedReadHalf,
        ws: WebSocketConn,
        buf: Vec<u8>,
    }
    impl WsReader {
        async fn run(
            mut self,
            read_tx: BoundedSender<Vec<u8>>,
            write_ctrl: BoundedSender<Outbound>,
        ) {
            loop {
                match self.read.read(&mut self.buf).await {
                    Ok(0) => {
                        let _ = write_ctrl.send(Outbound::Close);
                        break;
                    }
                    Ok(n) => {
                        self.ws.feed(&self.buf[..n]);
                        loop {
                            match self.ws.next_message() {
                                Ok(Some(WsMessage::Binary(data))) => {
                                    let request =
                                        ledger_request::Request::decode(data.as_slice()).unwrap();
                                    if let Some(ledger_request::request::Operation::YofficeSend(
                                        req,
                                    )) = request.operation
                                    {
                                        // Wait for send capacity (backpressure!)
                                        if read_tx.send(req.payload).await.is_err() {
                                            break;
                                        }
                                    } else {
                                        panic!("Invalid request");
                                        break;
                                    }
                                    // println!("request from client is {:?}",request);
                                }
                                Ok(Some(WsMessage::Text(text))) => {
                                    if read_tx.send(text.into_bytes()).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(Some(WsMessage::Ping(p))) => {
                                    let _ = write_ctrl.try_send(Outbound::Pong(p));
                                }
                                Ok(Some(WsMessage::Pong(_))) => {}
                                Ok(Some(WsMessage::Close(_))) => {
                                    let _ = write_ctrl.try_send(Outbound::Close);
                                    return;
                                }
                                Ok(None) => break,
                                Err(_) => return,
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    struct WsWriter {
        write: tokio::net::tcp::OwnedWriteHalf,
        ws: WebSocketConn,
    }
    impl WsWriter {
        async fn run(mut self, mut rx: BoundedReceiver<Outbound>) {
            while let Some(msg) = rx.recv().await {
                let m = match msg {
                    Outbound::Binary(d) => {
                        let response =
                            form_response_from_payload(d, None, ledger_response::Status::Success);
                        let mut buf = Vec::new();
                        response.encode(&mut buf).unwrap();
                        WsMessage::Binary(buf)
                    }
                    Outbound::Pong(d) => WsMessage::Pong(d),
                    Outbound::Close => {
                        let f = self.ws.encode(&WsMessage::Close(None));
                        let _ = self.write.write_all(&f).await;
                        return;
                    }
                };
                let f = self.ws.encode(&m);
                if self.write.write_all(&f).await.is_err() {
                    return;
                }
            }
        }
    }

    let reader = WsReader {
        read: read_half,
        ws: WebSocketConn::server(),
        buf: vec![0u8; 4096],
    };
    let writer = WsWriter {
        write: write_half,
        ws: WebSocketConn::server(),
    };

    let reader_ctrl = write_tx.clone();
    let reader_task = tokio::spawn(async move { reader.run(read_tx, reader_ctrl).await });
    let writer_task = tokio::spawn(async move { writer.run(write_rx).await });

    // Build adapter for BroadcastGroup
    use yrs_warp::compat::AsyncMutex;
    let sink = Arc::new(AsyncMutex::new(TcpSink { tx: write_tx }));
    let stream_wrapper = TcpStreamWrapper { inner: read_rx };

    // SUBSCRIBE to the broadcast group!
    let sub = bcast.subscribe_with_spawner(sink, stream_wrapper, &TokioSpawner);

    // Wait until completion
    match sub.completed().await {
        Ok(_) => println!("OK disconnected"),
        Err(e) => println!("Error disconnected: {:?}", e),
    }

    reader_task.abort();
    writer_task.abort();

    println!("[{}] Client disconnected from room: {}", peer_addr, room);
    log_heap_stats(&format!("After disconnect {}", peer_addr));
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(5)
        .max_blocking_threads(3)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        println!("=================================================");
        println!("  Memory Test: no_std crates");
        println!("  (yrs_no_std + yrs-warp-nostd)");
        println!("=================================================");
        println!();

        // Set time provider
        set_time_provider(Box::new(StdTimeProvider::new()));

        reset_heap_stats();
        // log_heap_stats("Initial");

        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));

        let listener = TcpListener::bind("0.0.0.0:5560")
            .await
            .expect("Failed to bind");
        println!("Server listening on ws://0.0.0.0:5560/collaboration/<room>");
        println!("Press Ctrl+C to stop and see final stats");
        println!();

        log_heap_stats("After server setup");

        // Periodic stats logging
        let rooms_clone = rooms.clone();
        // tokio::spawn(async move {
        //     let mut interval = tokio::time::interval(Duration::from_secs(3));
        //     loop {
        //         interval.tick().await;
        //         let rooms_count = rooms_clone.read_async().await.len();
        //         println!("--- Periodic Stats (rooms: {}) ---", rooms_count);
        //         log_heap_stats("Periodic");
        //     }
        // });

        // Accept connections
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let rooms = rooms.clone();
                    tokio::spawn(async move {
                        handle_connection(stream, rooms).await;
                    });
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }
    });
}
