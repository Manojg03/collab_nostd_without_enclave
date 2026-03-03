//! Example: Using yrs-warp-nostd with a std backend (tokio + raw TCP)
//!
//! This example demonstrates how to use the no_std yrs-warp library
//! with a standard library backend. The yrs-warp library handles all
//! the parsing/encoding, while we just provide the I/O.
//!
//! ## What yrs-warp-nostd provides (no I/O):
//! - HTTP request parsing
//! - Route matching
//! - CORS handling
//! - WebSocket frame encoding/decoding
//! - WebSocket handshake (upgrade)
//! - Yrs sync protocol
//!
//! ## What we implement here (I/O only):
//! - TCP accept
//! - Read bytes from socket  
//! - Write bytes to socket
//! - TimeProvider (for timestamps)
//! - Spawner (for async tasks)
//!
//! Run with: cargo run --example unified_broadcast_nostd

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{self, Certificate, PrivateKey, ServerConfig};
use std::fs::File;
use std::io::{BufReader};

use yrs::sync::Awareness;
use yrs::Doc;
// TimeProvider trait from yrs
use yrs::compat::time::{TimeProvider, set_time_provider};

// Import from yrs-warp-nostd - all pure logic, no I/O
use yrs_warp::compat::{
    // HTTP
    Request, Response, Method,
    // Routing  
    Route,
    // CORS
    Cors,
    // WebSocket
    WsMessage, WsUpgrade, WebSocketConn,
    // Broadcast (uses our abstractions)
    Spawner, JoinHandle, JoinHandleTrait, JoinError,
    // Select
    select2, SelectResult,
    // Async RwLock from compat (no tokio dependency)
    RwLock,
};
use yrs_warp::broadcast_unified::{UnifiedBroadcastGroup, UnifiedWebSocket, UnifiedWebSocketExt, BroadcastReceiver, RecvResult};
use yrs_warp::AwarenessRef;
use yrs_warp::compat::Error as CompatError;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

// ============================================================================
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

// ============================================================================
// Tokio Spawner Implementation
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
        let handle = tokio::spawn(future);
        JoinHandle::new(TokioJoinHandle(handle))
    }
}

// ============================================================================
// WebSocket Connection Wrapper
// ============================================================================

/// Our WebSocket connection - wraps a TcpStream + WebSocketConn for framing
struct WsConnection {
    stream: TcpStream,
    ws: WebSocketConn,
    read_buf: Vec<u8>,
}

impl WsConnection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            ws: WebSocketConn::server(),
            read_buf: vec![0u8; 4096],
        }
    }

    /// Read and parse next WebSocket message
    async fn read_message(&mut self) -> Option<Result<WsMessage, CompatError>> {
        loop {
            // First check if we already have a complete message buffered
            match self.ws.next_message() {
                Ok(Some(msg)) => return Some(Ok(msg)),
                Ok(None) => {} // Need more data
                Err(e) => return Some(Err(CompatError::other(format!("{:?}", e)))),
            }

            // Read more data from socket
            match self.stream.read(&mut self.read_buf).await {
                Ok(0) => return None, // Connection closed
                Ok(n) => {
                    self.ws.feed(&self.read_buf[..n]);
                }
                Err(e) => return Some(Err(CompatError::other(format!("{}", e)))),
            }
        }
    }

    /// Send a WebSocket message
    async fn send_message(&mut self, msg: &WsMessage) -> Result<(), CompatError> {
        let frame = self.ws.encode(msg);
        self.stream.write_all(&frame).await
            .map_err(|e| CompatError::other(format!("{}", e)))
    }
}

// ============================================================================
// Implement UnifiedWebSocket - User only provides send/recv
// ============================================================================

impl UnifiedWebSocket for WsConnection {
    /// Send binary data - just encode and write bytes
    async fn send(&mut self, data: Vec<u8>) -> Result<(), CompatError> {
        self.send_message(&WsMessage::Binary(data)).await
    }
    
    /// Receive binary data - read bytes, decode, return payload
    async fn recv(&mut self) -> Option<Result<Vec<u8>, CompatError>> {
        loop {
            match self.read_message().await {
                Some(Ok(WsMessage::Binary(data))) => return Some(Ok(data)),
                Some(Ok(WsMessage::Text(text))) => return Some(Ok(text.into_bytes())),
                Some(Ok(WsMessage::Ping(data))) => {
                    // Auto-respond to pings - uses same send mechanism
                    let _ = self.send_message(&WsMessage::Pong(data)).await;
                    continue;
                }
                Some(Ok(WsMessage::Pong(_))) => continue,
                Some(Ok(WsMessage::Close(_))) => return None,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
    
    /// Close connection - just sends close frame bytes (same as send)
    async fn close(&mut self) -> Result<(), CompatError> {
        // close() is NOT a separate I/O operation!
        // It just encodes a close frame and sends it using the same write
        let frame = self.ws.encode_close();
        self.stream.write_all(&frame).await
            .map_err(|e| CompatError::other(format!("{}", e)))
    }
}

/// Internal result for the select operation
enum SelectAction {
    WsData(Vec<u8>),
    WsClosed,
    WsPing(Vec<u8>),
    WsError(CompatError),
    Broadcast(Vec<u8>),
    BroadcastClosed,
    Continue,
}

impl UnifiedWebSocketExt for WsConnection {
    async fn recv_or_broadcast<'a>(
        &'a mut self,
        broadcast_rx: &'a mut BroadcastReceiver,
    ) -> (RecvResult, RecvResult) {
        use core::pin::pin;

        let first = loop {
            let action = {
                let ws_fut = self.read_message();
                let broadcast_fut = broadcast_rx.recv();

                match select2(pin!(ws_fut), pin!(broadcast_fut)).await {
                    SelectResult::First(ws_result) => match ws_result {
                        Some(Ok(WsMessage::Binary(data))) => SelectAction::WsData(data),
                        Some(Ok(WsMessage::Text(text))) => SelectAction::WsData(text.into_bytes()),
                        Some(Ok(WsMessage::Ping(data))) => SelectAction::WsPing(data),
                        Some(Ok(WsMessage::Pong(_))) => SelectAction::Continue,
                        Some(Ok(WsMessage::Close(_))) => SelectAction::WsClosed,
                        Some(Err(e)) => SelectAction::WsError(e),
                        None => SelectAction::WsClosed,
                    },
                    SelectResult::Second(broadcast_msg) => match broadcast_msg {
                        Some(msg) => SelectAction::Broadcast(msg),
                        None => SelectAction::BroadcastClosed,
                    },
                }
            };

            match action {
                SelectAction::WsData(data) => break RecvResult::WebSocket(Some(Ok(data))),
                SelectAction::WsClosed => break RecvResult::WebSocket(None),
                SelectAction::WsPing(data) => {
                    let _ = self.send_message(&WsMessage::Pong(data)).await;
                    continue;
                }
                SelectAction::WsError(e) => break RecvResult::WebSocket(Some(Err(e))),
                SelectAction::Broadcast(msg) => break RecvResult::Broadcast(msg),
                SelectAction::BroadcastClosed => break RecvResult::WebSocket(None),
                SelectAction::Continue => continue,
            }
        };

        let second = loop {
            let action = {
                let ws_fut = self.read_message();
                let broadcast_fut = broadcast_rx.recv();

                match select2(pin!(ws_fut), pin!(broadcast_fut)).await {
                    SelectResult::First(ws_result) => match ws_result {
                        Some(Ok(WsMessage::Binary(data))) => SelectAction::WsData(data),
                        Some(Ok(WsMessage::Text(text))) => SelectAction::WsData(text.into_bytes()),
                        Some(Ok(WsMessage::Ping(data))) => SelectAction::WsPing(data),
                        Some(Ok(WsMessage::Pong(_))) => SelectAction::Continue,
                        Some(Ok(WsMessage::Close(_))) => SelectAction::WsClosed,
                        Some(Err(e)) => SelectAction::WsError(e),
                        None => SelectAction::WsClosed,
                    },
                    SelectResult::Second(broadcast_msg) => match broadcast_msg {
                        Some(msg) => SelectAction::Broadcast(msg),
                        None => SelectAction::BroadcastClosed,
                    },
                }
            };

            match action {
                SelectAction::WsData(data) => break RecvResult::WebSocket(Some(Ok(data))),
                SelectAction::WsClosed => break RecvResult::WebSocket(None),
                SelectAction::WsPing(data) => {
                    let _ = self.send_message(&WsMessage::Pong(data)).await;
                    continue;
                }
                SelectAction::WsError(e) => break RecvResult::WebSocket(Some(Err(e))),
                SelectAction::Broadcast(msg) => break RecvResult::Broadcast(msg),
                SelectAction::BroadcastClosed => break RecvResult::WebSocket(None),
                SelectAction::Continue => continue,
            }
        };

        (first, second)
    }
}

// ============================================================================
// Application State
// ============================================================================

type Rooms = Arc<RwLock<HashMap<String, Arc<UnifiedBroadcastGroup>>>>;

// ============================================================================
// Main Server
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the time provider (required for yrs timestamps)
    set_time_provider(Box::new(StdTimeProvider::new()));

    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));

    // Load TLS cert and key
    let cert_file = &mut BufReader::new(File::open("cert.pem")?);
    let key_file = &mut BufReader::new(File::open("key.pem")?);
    let certs = rustls_pemfile::certs(cert_file)?.into_iter().map(Certificate).collect();
    let mut keys = rustls_pemfile::pkcs8_private_keys(key_file)?;
    if keys.is_empty() {
        panic!("No private key found in key.pem");
    }
    let key = PrivateKey(keys.remove(0));

    let config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("0.0.0.0:5560").await?;

    println!("🚀 no_std WebSocket server running on wss://0.0.0.0:5560");
    println!("📝 Connect to: wss://0.0.0.0:5560/collaboration/ROOM_NAME");
    println!();
    println!("┌─────────────────────────────────────────────────────┐");
    println!("│  yrs-warp-nostd handles (pure logic, no I/O):       │");
    println!("│    ✓ HTTP request parsing                           │");
    println!("│    ✓ Route matching (/collaboration/:room)          │");
    println!("│    ✓ CORS headers                                   │");
    println!("│    ✓ WebSocket handshake                            │");
    println!("│    ✓ WebSocket frame encoding/decoding              │");
    println!("│    ✓ Yrs sync protocol                              │");
    println!("├─────────────────────────────────────────────────────┤");
    println!("│  We implement (traits/I/O):                         │");
    println!("│    → TimeProvider (for timestamps)                  │");
    println!("|    -> Spawner (for async tasks)");
    println!("|    -> TLS accept");
    println!("|    -> Read bytes from socket");
    println!("|    -> Write bytes to socket");
    println!("|    (close is just writing close frame bytes)");
    println!("└─────────────────────────────────────────────────────┘");

    loop {
        let (stream, addr) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let rooms = rooms.clone();
        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    if let Err(e) = handle_tls_connection(tls_stream, rooms).await {
                        eprintln!("❌ TLS Connection error from {}: {}", addr, e);
                    }
                }
                Err(e) => {
                    eprintln!("❌ TLS handshake failed from {}: {}", addr, e);
                }
            }
        });
    }
}

// Wrapper for TLS stream
use tokio_rustls::server::TlsStream;
async fn handle_tls_connection(
    mut stream: TlsStream<TcpStream>,
    rooms: Rooms,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // === I/O: Read bytes ===
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;

    // === PURE LOGIC: Parse HTTP request ===
    let (request, _) = match Request::parse(&buf[..n]) {
        Ok(Some(r)) => r,
        Ok(None) => {
            let resp = Response::bad_request().text("Incomplete request");
            stream.write_all(&resp.encode()).await?;
            return Ok(());
        }
        Err(_) => {
            let resp = Response::bad_request().text("Malformed request");
            stream.write_all(&resp.encode()).await?;
            return Ok(());
        }
    };
    // ...existing logic for routing, WebSocket upgrade, etc. (adapt to use TLS stream)
    Ok(())
}
async fn handle_connection(
    mut stream: TcpStream,
    rooms: Rooms,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // === I/O: Read bytes ===
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    
    // === PURE LOGIC: Parse HTTP request ===
    let (request, _) = match Request::parse(&buf[..n]) {
        Ok(Some(r)) => r,
        Ok(None) => {
            let resp = Response::bad_request().text("Incomplete request");
            stream.write_all(&resp.encode()).await?;
            return Ok(());
        }
        Err(_) => {
            let resp = Response::bad_request().text("Invalid HTTP request");
            stream.write_all(&resp.encode()).await?;
            return Ok(());
        }
    };

    // === PURE LOGIC: CORS ===
    let cors = Cors::new()
        .allow_any_origin()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(["content-type"]);

    if request.method == Method::OPTIONS {
        if let Some(resp) = cors.process_preflight(&request) {
            stream.write_all(&resp.encode()).await?;
            return Ok(());
        }
    }

    // === PURE LOGIC: Route matching ===
    let route = Route::new("/collaboration/:room").get();
    
    let params = match route.matches(&request) {
        Some(p) => p,
        None => {
            let resp = cors.apply(&request, Response::not_found().text("Not found"));
            stream.write_all(&resp.encode()).await?;
            return Ok(());
        }
    };

    let room_name = params.get("room").unwrap_or("default").to_string();

    // === PURE LOGIC: Check WebSocket upgrade ===
    if !request.is_websocket_upgrade() {
        let resp = cors.apply(&request, Response::bad_request().text("Expected WebSocket"));
        stream.write_all(&resp.encode()).await?;
        return Ok(());
    }

    // === PURE LOGIC: Build upgrade response ===
    let ws_key = request.websocket_key().ok_or("Missing Sec-WebSocket-Key")?;
    let upgrade_response = WsUpgrade::build_upgrade_response(ws_key);
    
    // === I/O: Write upgrade response ===
    stream.write_all(&upgrade_response).await?;

    println!("✅ Client connected to room: {}", room_name);

    // Get or create room
    let bcast = get_or_create_room(&room_name, &rooms).await;

    // Wrap connection (provides send/recv using our TcpStream)
    let ws = WsConnection::new(stream);
    let spawner = TokioSpawner;

    // Subscribe to broadcast group
    let sub = bcast.subscribe(ws, &spawner);

    // Wait for connection to complete
    match sub.completed().await {
        Ok(_) => println!("✅ Client disconnected gracefully from room: {}", room_name),
        Err(e) => eprintln!("❌ Client error in room {}: {}", room_name, e),
    }

    Ok(())
}

async fn get_or_create_room(room: &str, rooms: &Rooms) -> Arc<UnifiedBroadcastGroup> {
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
    let spawner = TokioSpawner;
    let bcast = Arc::new(UnifiedBroadcastGroup::new(awareness, 32, &spawner));
    
    rooms_write.insert(room.to_string(), bcast.clone());
    println!("📁 Created new room: {}", room);
    
    bcast
}
