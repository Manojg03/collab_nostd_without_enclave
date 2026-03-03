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

use yrs::sync::Awareness;
use yrs::Doc;
// TimeProvider trait from yrs
use yrs::compat::time::{TimeProvider, set_time_provider};

// Import from yrs-warp-nostd - all pure logic, no I/O
use yrs_warp::compat::{
    // HTTP
    Cors, JoinError, JoinHandle, JoinHandleTrait, Method, Request, Response, Route, RwLock, SelectResult, Spawner, WebSocketConn, WsMessage, WsUpgrade, select2
};
use yrs_warp::broadcast_unified::{UnifiedBroadcastGroup, UnifiedWebSocket, UnifiedWebSocketExt, BroadcastReceiver, RecvResult};
use yrs_warp::AwarenessRef;
use yrs_warp::compat::Error as CompatError;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
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
        // self.0.abort();
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
        // Try to disable Nagle (reduce small-packet latency)
        let _ = stream.set_nodelay(true);
        Self {
            stream,
            ws: WebSocketConn::server(),
            read_buf: vec![0u8; 4096],
        }
    }
    // Returns the peer (client) address as a string, or "unknown" if unavailable
    fn peer_addr_str(&self) -> String {
        self.stream.peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    }

    /// Read and parse next WebSocket message
    async fn read_message(&mut self) -> Option<Result<WsMessage, CompatError>> {
        loop {
            // First check if we already have a complete message buffered
            match self.ws.next_message() {
                Ok(Some(msg)) => return Some(Ok(msg)),
                            Ok(None) => { } // Need more data
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
    /// Try to read a WebSocket message without blocking.
    /// Returns (Option<Result<WsMessage, CompatError>>, bool)
    /// The bool is true if an error occurred or connection closed, false if just WouldBlock.
    async fn try_read_message(&mut self) -> (Option<Result<WsMessage, CompatError>>, bool) {
        loop {
            // First check if we already have a complete message buffered
            match self.ws.next_message() {
                Ok(Some(msg)) => return (Some(Ok(msg)), false),
                Ok(None) => { } // Need more data
                Err(e) => return (Some(Err(CompatError::other(format!("{:?}", e)))), true),
            }

            // Try to read more data from socket (non-blocking)
            match self.stream.try_read(&mut self.read_buf) {
                Ok(0) => return (None, true), // Connection closed
                Ok(n) => {
                    self.ws.feed(&self.read_buf[..n]);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available right now, return None and false (not an error)
                    println!("try_read_message: WouldBlock for peer {}", self.peer_addr_str());
                    return (None, true);
                }
                Err(e) => {
                    return (Some(Err(CompatError::other(format!("{}", e)))), false);
                }
            }
        }
    }

    /// Send a WebSocket message
    async fn send_message(&mut self, msg: &WsMessage) -> Result<(), CompatError> {
        let start_time=Instant::now();
        let frame = self.ws.encode(msg);
        let res = self.stream.write_all(&frame).await
            .map_err(|e| CompatError::other(format!("{}", e)));
        let end_time=Instant::now();
        println!(
            "Sent message of length {} in {:?} from peer {}",
            frame.len(),
            end_time.duration_since(start_time),
            self.peer_addr_str()
        );
        if res.is_err() {
            println!("send failed: {:?}", res.as_ref().err());
        }
        res
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

// Internal select helper enum removed (not used)

/*
// Previous sequential implementation (preserved as comment):
// This version preferred `try_recv()` then awaited `read_message()` and
// returned two results by calling the helper twice. It caused head-of-line
// blocking for broadcasts that arrived during a websocket await.
//
// async fn recv_or_broadcast<'a>(
//     &'a mut self,
//     broadcast_rx: &'a mut BroadcastReceiver,
// ) -> (RecvResult, RecvResult) {
//     // Helper: get one result
//     async fn get_one<'a>(me: &'a mut WsConnection, brx: &'a mut BroadcastReceiver) -> RecvResult {
//         loop {
//             // Prefer immediate broadcast if available
//             if let Some(msg) = brx.try_recv() {
//                 println!("received broadcast message of length {:?}",msg.len());
//                 return RecvResult::Broadcast(msg);
//             }

//             // Otherwise wait for a websocket message
//             match me.read_message().await {
//                 Some(Ok(WsMessage::Binary(data))) => return RecvResult::WebSocket(Some(Ok(data))),
//                 Some(Ok(WsMessage::Text(text))) => return RecvResult::WebSocket(Some(Ok(text.into_bytes()))),
//                 Some(Ok(WsMessage::Ping(data))) => {
//                     let _ = me.send_message(&WsMessage::Pong(data)).await;
//                     continue; // handle next event
//                 }
//                 Some(Ok(WsMessage::Pong(_))) => continue,
//                 Some(Ok(WsMessage::Close(_))) => return RecvResult::WebSocket(None),
//                 Some(Err(e)) => return RecvResult::WebSocket(Some(Err(e))),
//                 None => return RecvResult::WebSocket(None),
//             }
//         }
//     }

//     let first = get_one(self, broadcast_rx).await;
//     let second = get_one(self, broadcast_rx).await;
//     (first, second)
// }
*/
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
    /*
    // Previous concurrent implementation (preserved but commented):
    async fn recv_or_broadcast<'a>(
        &'a mut self,
        broadcast_rx: &'a mut BroadcastReceiver,
    ) -> Vec<RecvResult> {
        ;let iterations=1;
        let mut result=Vec::new();
        // Two `tokio::select!` waits to concurrently await broadcast or websocket
        async fn get_one(me: &mut WsConnection, brx: &mut BroadcastReceiver) -> RecvResult {
            loop {
                tokio::select! {
                    biased;
                    maybe_bcast = brx.recv() => {
                        match maybe_bcast {
                            Some(msg) => return RecvResult::Broadcast(msg),
                            None => return RecvResult::WebSocket(None),
                        }
                    }
                    ws_msg = me.read_message() => {
                        match ws_msg {
                            Some(Ok(WsMessage::Binary(data))) => return RecvResult::WebSocket(Some(Ok(data))),
                            Some(Ok(WsMessage::Text(text))) => return RecvResult::WebSocket(Some(Ok(text.into_bytes()))),
                            Some(Ok(WsMessage::Ping(data))) => {
                                println!("ping received");
                                let _ = me.send_message(&WsMessage::Pong(data)).await;
                                continue;
                            }
                            Some(Ok(WsMessage::Pong(_))) => continue,
                            Some(Ok(WsMessage::Close(_))) => return RecvResult::WebSocket(None),
                            Some(Err(e)) => return RecvResult::WebSocket(Some(Err(e))),
                            None => return RecvResult::WebSocket(None),
                        }
                    }
                }
            }
        }
        for _ in 0..iterations {
            let res=get_one(self, broadcast_rx).await;
            match &res {
                RecvResult::WebSocket(None)=>break,
                _=>result.push(res),
            }
        }
        result
    }
    */

    /*
    // Previous random-choice implementation (preserved but commented):
    // New implementation: randomly choose either the broadcast or websocket
    // on each iteration and return only that result (never both). No timeouts.
    async fn recv_or_broadcast<'a>(
        &'a mut self,
        broadcast_rx: &'a mut BroadcastReceiver,
    ) -> Vec<RecvResult> {
        let iterations = 1;
        let mut result = Vec::new();

        for _ in 0..iterations {
            // Fast per-connection xorshift64 RNG (very cheap)
            let mut x = self.rng_state;
            x ^= x.wrapping_shl(13);
            x ^= x.wrapping_shr(7);
            x ^= x.wrapping_shl(17);
            self.rng_state = x;
            let choose_broadcast = (x & 1) == 0;

            if choose_broadcast {
                // Choose broadcast this iteration and await it
                let start = std::time::Instant::now();
                match broadcast_rx.recv().await {
                    Some(msg) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "received broadcast message of length {:?} in {:.3} seconds",
                            msg.len(),
                            elapsed
                        );
                        result.push(RecvResult::Broadcast(msg))
                    },
                    None => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "broadcast closed in {:.3} seconds",
                            elapsed
                        );
                        // broadcast closed -> signal websocket closed to caller
                        result.push(RecvResult::WebSocket(None));
                        break;
                    }
                }
            } else {
                // Choose websocket this iteration and await a ws message
                let start = std::time::Instant::now();
                match self.read_message().await {
                    Some(Ok(WsMessage::Binary(data))) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "received websocket binary message of length {:?} in {:.3} seconds",
                            data.len(),
                            elapsed
                        );
                        result.push(RecvResult::WebSocket(Some(Ok(data))))
                    },
                    Some(Ok(WsMessage::Text(text))) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "received websocket text message of length {:?} in {:.3} seconds",
                            text.len(),
                            elapsed
                        );
                        result.push(RecvResult::WebSocket(Some(Ok(text.into_bytes()))))
                    },
                    Some(Ok(WsMessage::Ping(data))) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "received websocket ping in {:.3} seconds",
                            elapsed
                        );
                        // respond to ping and continue the loop (will re-roll randomness)
                        let _ = self.send_message(&WsMessage::Pong(data)).await;
                        continue;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "received websocket pong in {:.3} seconds",
                            elapsed
                        );
                        continue;
                    }
                    Some(Ok(WsMessage::Close(_))) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "received websocket close in {:.3} seconds",
                            elapsed
                        );
                        result.push(RecvResult::WebSocket(None));
                        break;
                    }
                    Some(Err(e)) => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "websocket error in {:.3} seconds: {:?}",
                            elapsed,
                            e
                        );
                        result.push(RecvResult::WebSocket(Some(Err(e))));
                        break;
                    }
                    None => {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!(
                            "websocket closed in {:.3} seconds",
                            elapsed
                        );
                        result.push(RecvResult::WebSocket(None));
                        break;
                    }
                }
            }
        }

        result
    }
    */

    /*
    // Previous implementation (preserved but commented):
    // New implementation: wait for both websocket and broadcast concurrently
    // with a 0.5 second timeout for each; collect any results available.
    async fn recv_or_broadcast<'a>(
        &'a mut self,
        broadcast_rx: &'a mut BroadcastReceiver,
    ) -> Vec<RecvResult> {
        use tokio::time::{timeout,Duration};

        let mut result = Vec::new();

        loop {
            // await both concurrently with a 0.5s timeout
            let dur = Duration::from_millis(500);
            let start = std::time::Instant::now();
            let (ws_res, br_res) = tokio::join!(
                timeout(dur, self.read_message()),
                timeout(dur, broadcast_rx.recv())
            );

            let mut got_any = false;

            // Process broadcast result
            match br_res {
                Ok(Some(msg)) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!(
                        "received broadcast message of length {:?} in {:.3} seconds",
                        msg.len(),
                        elapsed
                    );
                    result.push(RecvResult::Broadcast(msg));
                    got_any = true;
                }
                Ok(None) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!("broadcast closed in {:.3} seconds", elapsed);
                    // broadcast closed -> signal websocket closed to caller
                    result.push(RecvResult::WebSocket(None));
                    return result;
                }
                Err(_) => {
                    // timed out waiting for broadcast
                    //println!("broadcast timeout after 0.5s");
                }
            }

            // Process websocket result (handle WsMessage variants)
            match ws_res {
                Ok(Some(Ok(WsMessage::Binary(data)))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!(
                        "received websocket binary message of length {:?} in {:.3} seconds",
                        data.len(),
                        elapsed
                    );
                    result.push(RecvResult::WebSocket(Some(Ok(data))));
                    got_any = true;
                }
                Ok(Some(Ok(WsMessage::Text(text)))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!(
                        "received websocket text message of length {:?} in {:.3} seconds",
                        text.len(),
                        elapsed
                    );
                    result.push(RecvResult::WebSocket(Some(Ok(text.into_bytes()))));
                    got_any = true;
                }
                Ok(Some(Ok(WsMessage::Ping(data)))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!("received websocket ping in {:.3} seconds", elapsed);
                    // respond to ping
                    let _ = self.send_message(&WsMessage::Pong(data)).await;
                }
                Ok(Some(Ok(WsMessage::Pong(_)))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!("received websocket pong in {:.3} seconds", elapsed);
                }
                Ok(Some(Ok(WsMessage::Close(_)))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!("received websocket close in {:.3} seconds", elapsed);
                    result.push(RecvResult::WebSocket(None));
                    return result;
                }
                Ok(Some(Err(e))) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!("websocket error in {:.3} seconds: {:?}", elapsed, e);
                    result.push(RecvResult::WebSocket(Some(Err(e))));
                    return result;
                }
                Ok(None) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    println!("websocket closed in {:.3} seconds", elapsed);
                    result.push(RecvResult::WebSocket(None));
                    return result;
                }
                Err(_) => {
                    // timed out waiting for websocket
                    //println!("websocket timeout after 0.5s");
                }
            }

            if got_any {
                return result;
            }

            // Neither produced a result within timeout; loop again
        }
    }
    */

    // New sequential implementation: try broadcast first (25ms), then websocket (25ms).
    // If one succeeds, do not attempt the other in that iteration. Loop until success.
    async fn recv_or_broadcast<'a>(
        &'a mut self,
        broadcast_rx: &'a mut BroadcastReceiver,
    ) -> Vec<RecvResult> {
        let start_time = SystemTime::now();
        let mut result_broadcast = Vec::new();
        loop {
            if let Some(bcast_opt) = broadcast_rx.try_recv() {
                // There was a broadcast-related event immediately available.
                result_broadcast.push(RecvResult::Broadcast(bcast_opt));
            } else {
                if result_broadcast.is_empty() {
                    break;
                }
                let end_time = SystemTime::now();
                let elapsed = end_time.duration_since(start_time)
                    .unwrap_or_default()
                    .as_secs_f64();
                println!(
                    "Broadcast try_recv took {:.3} s, returning {} broadcast msgs for peer {}",
                    elapsed,
                    result_broadcast.len(),
                    self.peer_addr_str()
                );
                return result_broadcast;
            }
        }

        // No broadcast now — wait for a websocket message and return it.
        // This will suspend the task until read_message completes.
        loop {
            // let (res,flag)=self.try_read_message().await;
            // if flag{
            //     //TODO:remove these things later
            //     return vec![];
            // }
            let res=self.read_message().await;  
            match res{
                Some(Ok(WsMessage::Binary(data))) => {
                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time)
                        .unwrap_or_default()
                        .as_secs_f64();
                    println!(
                        "WebSocket Binary read_message took {:.3} s from peer {}",
                        elapsed,
                        self.peer_addr_str()
                    );
                    return vec![RecvResult::WebSocket(Some(Ok(data)))];
                }
                Some(Ok(WsMessage::Text(text))) => {
                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time)
                        .unwrap_or_default()
                        .as_secs_f64();
                    println!(
                        "WebSocket Text read_message took {:.3} s",
                        elapsed
                    );
                    return vec![RecvResult::WebSocket(Some(Ok(text.into_bytes())))];
                }
                Some(Ok(WsMessage::Ping(data))) => {
                    println!("received ping msgs");
                    let _ = self.send_message(&WsMessage::Pong(data)).await;
                    continue;
                }
                Some(Ok(WsMessage::Pong(_))) => {
                    continue;
                }
                Some(Ok(WsMessage::Close(_))) => {
                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time)
                        .unwrap_or_default()
                        .as_secs_f64();
                    println!(
                        "WebSocket Close read_message took {:.3} s",
                        elapsed
                    );
                    return vec![RecvResult::WebSocket(None)];
                }
                Some(Err(e)) => {
                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time)
                        .unwrap_or_default()
                        .as_secs_f64();
                    println!(
                        "WebSocket Error read_message took {:.3} s",
                        elapsed
                    );
                    return vec![RecvResult::WebSocket(Some(Err(e)))];
                }
                None => {
                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time)
                        .unwrap_or_default()
                        .as_secs_f64();
                    println!(
                        "WebSocket None (connection closed) read_message took {:.3} s",
                        elapsed
                    );
                    return vec![RecvResult::WebSocket(None)];
                }
            }
        }
    }

    // async fn recv_or_broadcast<'a>(
    //     &'a mut self,
    //     broadcast_rx: &'a mut BroadcastReceiver,
    // ) -> Vec<RecvResult> {
    //     use tokio::time::{timeout, Duration};

    //     let mut result = Vec::new();
    //     let dur = Duration::from_millis(25);

    //     loop {
    //         // First: try broadcast with timeout
    //         let start = std::time::Instant::now();
    //         match timeout(dur, broadcast_rx.recv()).await {
    //             Ok(Some(msg)) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!(
    //                     "received broadcast message of length {:?} in {:.3} seconds",
    //                     msg.len(),
    //                     elapsed
    //                 );
    //                 result.push(RecvResult::Broadcast(msg));
    //                 return result;
    //             }
    //             Ok(None) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!("broadcast closed in {:.3} seconds", elapsed);
    //                 result.push(RecvResult::WebSocket(None));
    //                 return result;
    //             }
    //             Err(_) => {
    //                 // timed out waiting for broadcast; fall through to websocket
    //             }
    //         }

    //         // Second: try websocket with timeout
    //         let start = std::time::Instant::now();
    //         match timeout(dur, self.read_message()).await {
    //             Ok(Some(Ok(WsMessage::Binary(data)))) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!(
    //                     "received websocket binary message of length {:?} in {:.3} seconds",
    //                     data.len(),
    //                     elapsed
    //                 );
    //                 result.push(RecvResult::WebSocket(Some(Ok(data))));
    //                 return result;
    //             }
    //             Ok(Some(Ok(WsMessage::Text(text)))) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!(
    //                     "received websocket text message of length {:?} in {:.3} seconds",
    //                     text.len(),
    //                     elapsed
    //                 );
    //                 result.push(RecvResult::WebSocket(Some(Ok(text.into_bytes()))));
    //                 return result;
    //             }
    //             Ok(Some(Ok(WsMessage::Ping(data)))) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!("received websocket ping in {:.3} seconds", elapsed);
    //                 let _ = self.send_message(&WsMessage::Pong(data)).await;
    //                 continue; // go to next iteration
    //             }
    //             Ok(Some(Ok(WsMessage::Pong(_)))) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!("received websocket pong in {:.3} seconds", elapsed);
    //                 continue;
    //             }
    //             Ok(Some(Ok(WsMessage::Close(_)))) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!("received websocket close in {:.3} seconds", elapsed);
    //                 result.push(RecvResult::WebSocket(None));
    //                 return result;
    //             }
    //             Ok(Some(Err(e))) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!("websocket error in {:.3} seconds: {:?}", elapsed, e);
    //                 result.push(RecvResult::WebSocket(Some(Err(e))));
    //                 return result;
    //             }
    //             Ok(None) => {
    //                 let elapsed = start.elapsed().as_secs_f64();
    //                 println!("websocket closed in {:.3} seconds", elapsed);
    //                 result.push(RecvResult::WebSocket(None));
    //                 return result;
    //             }
    //             Err(_) => {
    //                 // websocket timed out as well; loop again
    //                 println!("both broadcast and websocket timed out after 25ms; retrying");
    //             }
    //         }
    //     }
    // }
    //    async fn recv_or_broadcast<'a>(
    //     &'a mut self,
    //     broadcast_rx: &'a mut BroadcastReceiver,
    // ) -> RecvResult {
    //     use core::pin::pin;
        
    //     loop {
    //         // Determine what action to take
    //         let action = {
    //             let ws_fut = self.read_message();
    //             let broadcast_fut = broadcast_rx.recv();
                
    //             match select2(pin!(ws_fut), pin!(broadcast_fut)).await {
    //                 SelectResult::First(ws_result) => {
    //                     match ws_result {
    //                         Some(Ok(WsMessage::Binary(data))) => SelectAction::WsData(data),
    //                         Some(Ok(WsMessage::Text(text))) => SelectAction::WsData(text.into_bytes()),
    //                         Some(Ok(WsMessage::Ping(data))) => SelectAction::WsPing(data),
    //                         Some(Ok(WsMessage::Pong(_))) => SelectAction::Continue,
    //                         Some(Ok(WsMessage::Close(_))) => SelectAction::WsClosed,
    //                         Some(Err(e)) => SelectAction::WsError(e),
    //                         None => SelectAction::WsClosed,
    //                     }
    //                 }
    //                 SelectResult::Second(broadcast_msg) => {
    //                     match broadcast_msg {
    //                         Some(msg) => SelectAction::Broadcast(msg),
    //                         None => SelectAction::BroadcastClosed,
    //                     }
    //                 }
    //             }
    //         }; // ws_fut is dropped here
            
    //         // Now handle the action (borrows are released)
    //         match action {
    //             SelectAction::WsData(data) => return RecvResult::WebSocket(Some(Ok(data))),
    //             SelectAction::WsClosed => return RecvResult::WebSocket(None),
    //             SelectAction::WsPing(data) => {
    //                 let _ = self.send_message(&WsMessage::Pong(data)).await;
    //                 continue;
    //             }
    //             SelectAction::WsError(e) => return RecvResult::WebSocket(Some(Err(e))),
    //             SelectAction::Broadcast(msg) => return RecvResult::Broadcast(msg),
    //             SelectAction::BroadcastClosed => return RecvResult::WebSocket(None),
    //             SelectAction::Continue => continue,
    //         }
    //     }
    // }
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
    
    let listener = TcpListener::bind("0.0.0.0:5560").await?;
    
    println!("🚀 no_std WebSocket server running on ws://0.0.0.0:5560");
    println!("📝 Connect to: ws://0.0.0.0:5560/collaboration/ROOM_NAME");
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
    println!("│    → Spawner (for async tasks)                      │");
    println!("│    → TCP accept                                     │");
    println!("│    → Read bytes from socket                         │");
    println!("│    → Write bytes to socket                          │");
    println!("│    (close is just writing close frame bytes)        │");
    println!("└─────────────────────────────────────────────────────┘");
    
    loop {
        let (stream, addr) = listener.accept().await?;
        let rooms = rooms.clone();
        
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, rooms).await {
                eprintln!("❌ Connection error from {}: {}", addr, e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    rooms: Rooms,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // === I/O: Read bytes ===
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    println!("read {:?} bytes from client",n);
    
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
    println!("first http request {:?} of length {:?}",request,n);

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

    // Special-case: low-overhead echo handler for latency measurements
    // Room name "latency" bypasses Yrs deserialization and simply echoes
    // binary/text frames back, which gives a clean RTT measurement.
    if room_name == "latency" {
        let mut conn = WsConnection::new(stream);
        loop {
            match conn.read_message().await {
                Some(Ok(WsMessage::Binary(data))) => {
                    let _ = conn.send_message(&WsMessage::Binary(data)).await;
                }
                Some(Ok(WsMessage::Text(text))) => {
                    let _ = conn.send_message(&WsMessage::Text(text)).await;
                }
                Some(Ok(WsMessage::Ping(data))) => {
                    let _ = conn.send_message(&WsMessage::Pong(data)).await;
                    continue;
                }
                Some(Ok(WsMessage::Pong(_))) => continue,
                Some(Ok(WsMessage::Close(_))) => {
                    let _ = conn.send_message(&WsMessage::Close(None)).await;
                    break;
                }
                Some(Err(e)) => {
                    eprintln!("latency handler read error: {:?}", e);
                    break;
                }
                None => break,
            }
        }
        println!("✅ Latency client disconnected from room: {}", room_name);
        return Ok(());
    }

    // Get or create room (normal Yrs path)
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
    let bcast = Arc::new(UnifiedBroadcastGroup::new(awareness, 320, &spawner));
    
    rooms_write.insert(room.to_string(), bcast.clone());
    println!("📁 Created new room: {}", room);
    
    bcast
}
