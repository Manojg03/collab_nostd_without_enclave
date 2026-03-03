//! Example using BroadcastGroup with split WebSocket (sink/stream pattern)
//!
//! This example shows how to use the standard broadcast approach where the
//! WebSocket is split into separate sink and stream halves.
//!
//! This pattern is similar to the original yrs-warp crate's approach.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::ws::{WebSocket, Ws, Message as WarpMessage};
use warp::{Filter, Rejection, Reply};
use yrs::sync::Awareness;
use yrs::Doc;

use yrs_warp::broadcast::BroadcastGroup;
use yrs_warp::compat::{
    Error as CompatError, Spawner, JoinHandle, JoinHandleTrait, JoinError,
    Sink, Stream as CompatStream, AsyncMutex as CompatAsyncMutex,
};
use yrs_warp::AwarenessRef;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::stream::Stream as FuturesStream;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

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
// Warp WebSocket Sink Wrapper
// ============================================================================

/// Wrapper around the write half of Warp's WebSocket
struct WarpSink {
    tx: mpsc::UnboundedSender<WarpMessage>,
}

impl Sink<Vec<u8>> for WarpSink {
    type Error = CompatError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Unbounded channel is always ready
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        self.tx
            .send(WarpMessage::binary(item))
            .map_err(|e| CompatError::other(format!("send error: {}", e)))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// ============================================================================
// Warp WebSocket Stream Wrapper
// ============================================================================

/// Wrapper around the read half of Warp's WebSocket
struct WarpStream {
    inner: futures_util::stream::SplitStream<WebSocket>,
}

impl CompatStream for WarpStream {
    type Item = Result<Vec<u8>, CompatError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                if msg.is_binary() {
                    Poll::Ready(Some(Ok(msg.into_bytes())))
                } else if msg.is_close() {
                    Poll::Ready(None)
                } else {
                    // Skip non-binary messages by waking ourselves immediately
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(CompatError::other(format!("recv error: {}", e)))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ============================================================================
// Application
// ============================================================================

type Rooms = Arc<RwLock<HashMap<String, Arc<BroadcastGroup>>>>;

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

    println!("🚀 Split broadcast server running on ws://0.0.0.0:1234");
    println!("📝 Connect to: ws://0.0.0.0:1234/collaboration/ROOM_NAME");
    println!("📦 Using BroadcastGroup with split sink/stream");
    
    warp::serve(routes).run(([0, 0, 0, 0], 1234)).await;
}

async fn ws_handler(room: String, ws: Ws, rooms: Rooms) -> Result<impl Reply, Rejection> {
    let bcast = get_or_create_room(&room, &rooms).await;
    
    println!("✅ Client connecting to room: {}", room);
    
    Ok(ws.on_upgrade(move |socket| handle_connection(socket, room, bcast)))
}

async fn get_or_create_room(room: &str, rooms: &Rooms) -> Arc<BroadcastGroup> {
    // First try read lock
    {
        let rooms_read = rooms.read().await;
        if let Some(bcast) = rooms_read.get(room) {
            return bcast.clone();
        }
    }
    
    // Need to create new room
    let mut rooms_write = rooms.write().await;
    
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

async fn handle_connection(ws: WebSocket, room: String, bcast: Arc<BroadcastGroup>) {
    // Split the WebSocket into sink and stream
    let (ws_sink, ws_stream) = ws.split();
    
    // Create a channel to forward messages to the WebSocket sink
    let (tx, mut rx) = mpsc::unbounded_channel::<WarpMessage>();
    
    // Spawn a task to forward messages from the channel to the actual WebSocket
    let sink_forward_task = tokio::spawn(async move {
        let mut ws_sink = ws_sink;
        while let Some(msg) = rx.recv().await {
            if ws_sink.send(msg).await.is_err() {
                break;
            }
        }
    });
    
    // Wrap sink and stream
    let sink = Arc::new(CompatAsyncMutex::new(WarpSink { tx }));
    let stream = WarpStream { inner: ws_stream };
    
    // Subscribe to broadcast group
    let sub = bcast.subscribe_with_spawner(sink, stream, &TokioSpawner);
    
    match sub.completed().await {
        Ok(_) => println!("✅ Client disconnected gracefully from room: {}", room),
        Err(e) => eprintln!("❌ Client disconnected with error from room {}: {:?}", room, e),
    }
    
    // Abort the forward task when done
    sink_forward_task.abort();
}
