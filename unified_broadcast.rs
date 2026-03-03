//! Example using UnifiedBroadcastGroup with a non-split WebSocket
//!
//! This example shows how to use the unified broadcast approach where the
//! WebSocket is wrapped in an AsyncMutex instead of being split into sink/stream.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::ws::{WebSocket, Ws, Message as WarpMessage};
use warp::{Filter, Rejection, Reply};
use yrs::sync::Awareness;
use yrs::Doc;

use yrs_warp::broadcast_unified::{UnifiedBroadcastGroup, UnifiedWebSocket, UnifiedWebSocketExt, BroadcastReceiver, RecvResult};
use yrs_warp::compat::{Error as CompatError, Spawner, JoinHandle, JoinHandleTrait, JoinError};
use yrs_warp::AwarenessRef;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::{SinkExt, StreamExt, FutureExt};

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

// ============================================================================
// Unified WebSocket Wrapper
// ============================================================================

/// Wrapper around Warp's WebSocket that implements UnifiedWebSocket
struct WarpWebSocket {
    inner: WebSocket,
}

impl WarpWebSocket {
    fn new(ws: WebSocket) -> Self {
        Self { inner: ws }
    }
}

impl UnifiedWebSocket for WarpWebSocket {
    async fn send(&mut self, data: Vec<u8>) -> Result<(), CompatError> {
        self.inner
            .send(WarpMessage::binary(data))
            .await
            .map_err(|e| CompatError::other(format!("send error: {}", e)))
    }
    
    async fn recv(&mut self) -> Option<Result<Vec<u8>, CompatError>> {
        loop {
            match self.inner.next().await {
                Some(Ok(msg)) => {
                    if msg.is_binary() {
                        return Some(Ok(msg.into_bytes()));
                    } else if msg.is_close() {
                        return None;
                    }
                    // Skip non-binary messages (text, ping, pong)
                    continue;
                }
                Some(Err(e)) => {
                    return Some(Err(CompatError::other(format!("recv error: {}", e))));
                }
                None => return None,
            }
        }
    }
    
    async fn close(&mut self) -> Result<(), CompatError> {
        // Send a close message rather than consuming self
        self.inner
            .send(WarpMessage::close())
            .await
            .map_err(|e| CompatError::other(format!("close error: {}", e)))
    }
}

impl UnifiedWebSocketExt for WarpWebSocket {
    async fn recv_or_broadcast<'a>(
        &'a mut self,
        broadcast_rx: &'a mut BroadcastReceiver,
    ) -> RecvResult {
        // Use tokio::select to wait on BOTH WebSocket AND broadcast channel
        tokio::select! {
            // Wait for WebSocket message
            ws_msg = self.inner.next() => {
                match ws_msg {
                    Some(Ok(msg)) => {
                        if msg.is_binary() {
                            RecvResult::WebSocket(Some(Ok(msg.into_bytes())))
                        } else if msg.is_close() {
                            RecvResult::WebSocket(None)
                        } else {
                            // For non-binary, return empty to continue loop
                            // This handles ping/pong/text by treating as empty binary
                            RecvResult::WebSocket(Some(Ok(Vec::new())))
                        }
                    }
                    Some(Err(e)) => {
                        RecvResult::WebSocket(Some(Err(CompatError::other(format!("recv error: {}", e)))))
                    }
                    None => RecvResult::WebSocket(None),
                }
            }
            // Wait for broadcast message
            broadcast_msg = broadcast_rx.recv() => {
                match broadcast_msg {
                    Some(msg) => RecvResult::Broadcast(msg),
                    None => RecvResult::WebSocket(None), // Broadcast channel closed
                }
            }
        }
    }
}

// ============================================================================
// Application
// ============================================================================

type Rooms = Arc<RwLock<HashMap<String, Arc<UnifiedBroadcastGroup>>>>;

#[tokio::main]
async fn main() {
    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
    
    let ws_route = warp::path!("collaboration" / String)
        .and(warp::ws())
        .and(warp::any().map(move || rooms.clone()))
        .and_then(ws_handler);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "OPTIONS"])
        .allow_headers(vec!["content-type"]);

    let routes = ws_route.with(cors);

    println!("🚀 Unified broadcast server running on ws://0.0.0.0:1234");
    println!("📝 Connect to: ws://0.0.0.0:1235/collaboration/ROOM_NAME");
    println!("📦 Using UnifiedBroadcastGroup (no WebSocket splitting)");
    
    warp::serve(routes).run(([0, 0, 0, 0], 1234)).await;
}

async fn ws_handler(room: String, ws: Ws, rooms: Rooms) -> Result<impl Reply, Rejection> {
    let bcast = get_or_create_room(&room, &rooms).await;
    
    println!("✅ Client connecting to room: {}", room);
    
    Ok(ws.on_upgrade(move |socket| handle_connection(socket, room, bcast)))
}

async fn get_or_create_room(room: &str, rooms: &Rooms) -> Arc<UnifiedBroadcastGroup> {
    // First try read lock
    {
        let rooms_read = rooms.read().await;
        if let Some(bcast) = rooms_read.get(room) {
            return bcast.clone();
        }
    }
    
    // Need to create new room
    let mut rooms_write = rooms.write().await;
    
    // Double-check
    if let Some(bcast) = rooms_write.get(room) {
        return bcast.clone();
    }
    
    // Create new awareness/doc for this room
    let doc = Doc::new();
    let awareness: AwarenessRef = Arc::new(Awareness::new(doc));
    
    // Create broadcast group with our spawner
    let spawner = TokioSpawner;
    let bcast = Arc::new(UnifiedBroadcastGroup::new(awareness, 32, &spawner));
    
    rooms_write.insert(room.to_string(), bcast.clone());
    println!("📁 Created new room: {}", room);
    
    bcast
}

async fn handle_connection(ws: WebSocket, room: String, bcast: Arc<UnifiedBroadcastGroup>) {
    // Wrap the WebSocket (without splitting!)
    let ws = WarpWebSocket::new(ws);
    let spawner = TokioSpawner;
    
    // Subscribe using the unified approach
    let sub = bcast.subscribe(ws, &spawner);
    
    match sub.completed().await {
        Ok(_) => println!("✅ Client disconnected gracefully from room: {}", room),
        Err(e) => eprintln!("❌ Client disconnected with error from room {}: {}", room, e),
    }
}
