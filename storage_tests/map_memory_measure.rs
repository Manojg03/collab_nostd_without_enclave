//! Measure in-memory RAM usage of a y.Map with 1M cells vs its encoded size.
//!
//! Run with:
//!   cargo run --example map_memory_measure --release
//!
//! What this does:
//!   1. Creates a Doc with a y.Map
//!   2. Inserts 1,000,000 cells (key: "R0C0".."R999C999" style, value: short string)
//!   3. Prints heap usage after insertion (the in-memory CRDT cost)
//!   4. Encodes the entire doc as a v1 update
//!   5. Prints the encoded byte size (what you'd store on disk/redis)
//!   6. Drops the doc and prints heap usage after drop

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// ============================================================================
// Tracking Allocator
// ============================================================================
use std::alloc::{GlobalAlloc, Layout, System};

static HEAP_ALLOC: AtomicUsize = AtomicUsize::new(0);
static HEAP_DEALLOC: AtomicUsize = AtomicUsize::new(0);
static PEAK_USAGE: AtomicUsize = AtomicUsize::new(0);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            HEAP_ALLOC.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            // Update peak
            let current = HEAP_ALLOC
                .load(Ordering::Relaxed)
                .saturating_sub(HEAP_DEALLOC.load(Ordering::Relaxed));
            let mut peak = PEAK_USAGE.load(Ordering::Relaxed);
            while current > peak {
                match PEAK_USAGE.compare_exchange_weak(
                    peak, current, Ordering::Relaxed, Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP_DEALLOC.fetch_add(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            HEAP_DEALLOC.fetch_add(layout.size(), Ordering::Relaxed);
            HEAP_ALLOC.fetch_add(new_size, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn heap_current() -> usize {
    HEAP_ALLOC
        .load(Ordering::Relaxed)
        .saturating_sub(HEAP_DEALLOC.load(Ordering::Relaxed))
}

fn heap_peak() -> usize {
    PEAK_USAGE.load(Ordering::Relaxed)
}

fn print_heap(label: &str) {
    let current = heap_current();
    let peak = heap_peak();
    let allocs = ALLOC_COUNT.load(Ordering::Relaxed);
    println!(
        "[{:30}]  current: {:>12} bytes ({:>8.2} MB)  peak: {:>12} bytes ({:>8.2} MB)  allocs: {}",
        label,
        current,
        current as f64 / 1_048_576.0,
        peak,
        peak as f64 / 1_048_576.0,
        allocs,
    );
}

fn reset_heap() {
    HEAP_ALLOC.store(0, Ordering::Relaxed);
    HEAP_DEALLOC.store(0, Ordering::Relaxed);
    PEAK_USAGE.store(0, Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
}

// ============================================================================
// yrs imports
// ============================================================================

use yrs::compat::time::{set_time_provider, TimeProvider};
use yrs::{Doc, Map, ReadTxn, StateVector, Transact};
use std::time::{SystemTime, UNIX_EPOCH};

struct StdTimeProvider(Instant);
impl TimeProvider for StdTimeProvider {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
    fn monotonic_millis(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    set_time_provider(Box::new(StdTimeProvider(Instant::now())));
    reset_heap();

    println!("==========================================================");
    println!("  y.Map Memory Measure — 1,000,000 cells");
    println!("==========================================================\n");

    let num_rows = 1000;
    let num_cols = 1000;
    let total_cells = num_rows * num_cols;

    print_heap("Before Doc creation");

    // ---- Create Doc + Map and insert cells ----
    let t0 = Instant::now();

    let doc = Doc::new();
    let map = doc.get_or_insert_map("sheet");
    {
        let mut txn = doc.transact_mut();
        for r in 0..num_rows {
            for c in 0..num_cols {
                let key = format!("R{}C{}", r, c);
                let val = format!("v{}_{}", r, c);
                map.insert(&mut txn, key, val);
            }
        }
        // txn commits on drop
    }
    let insert_time = t0.elapsed();

    println!("\nInserted {} cells in {:.2?}", total_cells, insert_time);
    print_heap("After 1M cell insert");

    let ram_after_insert = heap_current();

    // ---- Encode the entire doc as a v1 update ----
    let t1 = Instant::now();
    let encoded = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };
    let encode_time = t1.elapsed();
    let encoded_len = encoded.len();

    println!(
        "\nEncoded size: {} bytes ({:.2} MB)  — took {:.2?}",
        encoded_len,
        encoded_len as f64 / 1_048_576.0,
        encode_time,
    );
    print_heap("After encode");

    // ---- Ratio ----
    println!(
        "\nRAM / encoded ratio: {:.1}x  ({:.2} MB in RAM vs {:.2} MB encoded)",
        ram_after_insert as f64 / encoded_len as f64,
        ram_after_insert as f64 / 1_048_576.0,
        encoded_len as f64 / 1_048_576.0,
    );

    // ---- Drop doc and see what's freed ----
    drop(encoded);
    drop(doc);
    print_heap("After dropping Doc");

    println!("\nDone.");
}
