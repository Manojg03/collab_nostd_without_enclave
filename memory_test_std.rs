//! Memory Test: Standard yrs + y-sync crates
//!
//! This example tracks heap allocation metrics to compare memory usage
//! with the no_std yrs implementation.
//!
//! Run with: cargo run --example memory_test_std

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

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
// Room Management (simple counter for testing, avoiding yrs complexity)
// ============================================================================

/// Simple room counter - just tracks connections per room
struct Room {
    connection_count: AtomicUsize,
}

type Rooms = Arc<RwLock<HashMap<String, Arc<Room>>>>;

async fn get_or_create_room(room: &str, rooms: &Rooms) -> Arc<Room> {
    {
        let rooms_read = rooms.read().await;
        if let Some(r) = rooms_read.get(room) {
            r.connection_count.fetch_add(1, Ordering::Relaxed);
            return r.clone();
        }
    }

    let mut rooms_write = rooms.write().await;
    if let Some(r) = rooms_write.get(room) {
        r.connection_count.fetch_add(1, Ordering::Relaxed);
        return r.clone();
    }

    let r = Arc::new(Room {
        connection_count: AtomicUsize::new(1),
    });
    rooms_write.insert(room.to_string(), r.clone());

    println!("Created new room: {}", room);
    r
}

// ============================================================================
// Connection Handler
// ============================================================================

async fn handle_connection(stream: tokio::net::TcpStream, rooms: Rooms) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    // Accept WebSocket connection using tungstenite
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("[{}] WebSocket handshake failed: {}", peer_addr, e);
            return;
        }
    };

    // Default room for simplicity
    let room_name = "default".to_string();
    let room = get_or_create_room(&room_name, &rooms).await;

    println!(
        "[{}] Client connected to room: {} (count: {})",
        peer_addr,
        room_name,
        room.connection_count.load(Ordering::Relaxed)
    );
    log_heap_stats(&format!("After connect {}", peer_addr));

    let (mut write, mut read) = ws_stream.split();

    // Simple echo/broadcast loop
    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Binary(data)) => {
                // Echo back for testing
                if let Err(e) = write.send(Message::Binary(data)).await {
                    eprintln!("[{}] Send error: {}", peer_addr, e);
                    break;
                }
            }
            Ok(Message::Text(text)) => {
                if let Err(e) = write.send(Message::Text(text)).await {
                    eprintln!("[{}] Send error: {}", peer_addr, e);
                    break;
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) => {
                println!("[{}] Client sent close", peer_addr);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("[{}] Read error: {}", peer_addr, e);
                break;
            }
        }
    }

    room.connection_count.fetch_sub(1, Ordering::Relaxed);
    println!(
        "[{}] Client disconnected from room: {} (count: {})",
        peer_addr,
        room_name,
        room.connection_count.load(Ordering::Relaxed)
    );
    log_heap_stats(&format!("After disconnect {}", peer_addr));
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    println!("=================================================");
    println!("  Memory Test: Standard library baseline");
    println!("  (tokio + tokio-tungstenite, no yrs)");
    println!("=================================================");
    println!();

    reset_heap_stats();
    log_heap_stats("Initial");

    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));

    let listener = TcpListener::bind("0.0.0.0:5560")
        .await
        .expect("Failed to bind");
    println!("Server listening on ws://0.0.0.0:5560");
    println!("(Note: uses port 8081 to run alongside nostd version on 5560)");
    println!("Press Ctrl+C to stop and see final stats");
    println!();

    log_heap_stats("After server setup");

    // Periodic stats logging
    let rooms_clone = rooms.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let rooms_count = rooms_clone.read().await.len();
            println!("--- Periodic Stats (rooms: {}) ---", rooms_count);
            log_heap_stats("Periodic");
        }
    });

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
}
