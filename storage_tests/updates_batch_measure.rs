//! Compare: storing raw update bytes (batched) vs loading them into a Doc.
//!
//! Run with:
//!   cargo run --example updates_batch_measure --release
//!
//! This simulates the "stateless relay" architecture:
//!
//!   CLIENT side: a Doc producing updates (one per cell edit)
//!   SERVER side: two strategies measured —
//!     Strategy A: just store raw update bytes (append to a Vec, like a Redis stream)
//!     Strategy B: apply every update into a live Doc (the "hold everything in RAM" approach)
//!
//! Then we measure:
//!   - Total size of raw update bytes stored
//!   - merge_updates_v1: merge batches of updates (no Doc needed) — size after merge
//!   - Doc-in-RAM size after applying all updates
//!   - Encoding the loaded Doc back to a snapshot — size
//!
//! Also demonstrates incremental merge:
//!   u1,u2,u3 → merged_a    u4,u5 → merged_b    merged_a + merged_b → final_merged

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// ============================================================================
// Tracking Allocator (same as map_memory_measure)
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
    println!(
        "[{:40}]  current: {:>12} ({:>8.2} MB)  peak: {:>12} ({:>8.2} MB)",
        label,
        current,
        current as f64 / 1_048_576.0,
        peak,
        peak as f64 / 1_048_576.0,
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
use yrs::updates::decoder::Decode;
use yrs::{Doc, Map, ReadTxn, StateVector, Transact, Update, merge_updates_v1};
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

fn fmt_bytes(b: usize) -> String {
    if b < 1024 {
        format!("{} B", b)
    } else if b < 1_048_576 {
        format!("{:.2} KB", b as f64 / 1024.0)
    } else {
        format!("{:.2} MB", b as f64 / 1_048_576.0)
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    set_time_provider(Box::new(StdTimeProvider(Instant::now())));

    let num_cells: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_00);

    println!("==========================================================");
    println!("  Update Batching vs Doc-in-RAM  —  {} cells", num_cells);
    println!("==========================================================\n");

    // =====================================================================
    // PHASE 1: CLIENT — produce individual updates (one per cell edit)
    // =====================================================================
    println!("--- Phase 1: Client produces {} individual updates ---\n", num_cells);

    reset_heap();
    let t0 = Instant::now();

    let client_doc = Doc::new();
    let client_map = client_doc.get_or_insert_map("sheet");

    // Capture each transaction's update bytes via observe_update_v1
    let updates: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::with_capacity(num_cells)));

    let updates_clone = updates.clone();
    let _sub = client_doc
        .observe_update_v1(move |_txn, event| {
            updates_clone.lock().unwrap().push(event.update.clone());
        })
        .unwrap();

    // Insert cells one at a time (each in its own transaction = one update each)
    for i in 0..num_cells {
        let mut txn = client_doc.transact_mut();
        let key = format!("R{}C{}", i / 1000, i % 1000);
        let val = format!("v{}", i);
        client_map.insert(&mut txn, key, val);
        // txn commits on drop → triggers observe_update_v1
    }

    let produce_time = t0.elapsed();
    drop(_sub); // drop subscription first so the closure's Arc clone is released
    let updates = std::sync::Arc::try_unwrap(updates).unwrap().into_inner().unwrap();

    let total_raw_bytes: usize = updates.iter().map(|u| u.len()).sum();
    let avg_update_size = total_raw_bytes as f64 / updates.len() as f64;

    println!("  Produced {} updates in {:.2?}", updates.len(), produce_time);
    println!(
        "  Total raw update bytes:  {} ({} updates × {:.0} avg bytes each)",
        fmt_bytes(total_raw_bytes),
        updates.len(),
        avg_update_size,
    );
    print_heap("After client produces all updates");

    // Drop client doc — we only care about the raw bytes now
    drop(client_doc);
    println!();

    // =====================================================================
    // PHASE 2: STRATEGY A — just store the bytes, then merge_updates
    // =====================================================================
    println!("--- Phase 2: Strategy A — store raw bytes + merge_updates ---\n");

    // Simulate storage: the updates Vec is already our "stream"
    println!("  Raw bytes in storage: {}", fmt_bytes(total_raw_bytes));

    // Merge all at once
    let t1 = Instant::now();
    let merged_all = merge_updates_v1(updates.iter().map(|u| u.as_slice())).unwrap();
    let merge_all_time = t1.elapsed();

    println!(
        "  merge_updates_v1 (all {} → 1): {} — took {:.2?}",
        updates.len(),
        fmt_bytes(merged_all.len()),
        merge_all_time,
    );

    // Demonstrate incremental merge: split into two halves, merge each, then merge the two
    let half = updates.len() / 2;
    let t2 = Instant::now();
    let merged_a = merge_updates_v1(updates[..half].iter().map(|u| u.as_slice())).unwrap();
    let merged_b = merge_updates_v1(updates[half..].iter().map(|u| u.as_slice())).unwrap();
    let merged_final =
        merge_updates_v1([merged_a.as_slice(), merged_b.as_slice()]).unwrap();
    let incr_merge_time = t2.elapsed();

    println!(
        "  Incremental merge: first half → {}, second half → {}, combined → {} — took {:.2?}",
        fmt_bytes(merged_a.len()),
        fmt_bytes(merged_b.len()),
        fmt_bytes(merged_final.len()),
        incr_merge_time,
    );

    // Verify merged sizes match
    assert_eq!(
        merged_all.len(),
        merged_final.len(),
        "Full merge and incremental merge should produce same size"
    );
    println!("  ✓ Full merge == incremental merge (both {} bytes)", merged_all.len());
    println!();

    // =====================================================================
    // PHASE 3: STRATEGY B — apply all updates into a server-side Doc (RAM)
    // =====================================================================
    println!("--- Phase 3: Strategy B — apply updates into server Doc (RAM) ---\n");

    reset_heap();
    let t3 = Instant::now();

    let server_doc = Doc::new();
    let _server_map = server_doc.get_or_insert_map("sheet");

    // Apply each update
    for (i, update_bytes) in updates.iter().enumerate() {
        let update = Update::decode_v1(update_bytes).unwrap();
        let mut txn = server_doc.transact_mut();
        txn.apply_update(update).unwrap();
        if (i + 1) % 25_000 == 0 {
            print_heap(&format!("After applying {} updates", i + 1));
        }
    }

    let apply_time = t3.elapsed();
    print_heap("After applying ALL updates");
    let ram_with_doc = heap_current();

    // Encode snapshot from server doc
    let t4 = Instant::now();
    let snapshot = {
        let txn = server_doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };
    let snapshot_time = t4.elapsed();

    println!(
        "\n  Snapshot from server Doc: {} — took {:.2?}",
        fmt_bytes(snapshot.len()),
        snapshot_time,
    );

    // Also try applying the single merged update instead of N individual ones
    drop(server_doc);
    reset_heap();

    let t5 = Instant::now();
    let server_doc2 = Doc::new();
    let _server_map2 = server_doc2.get_or_insert_map("sheet");
    {
        let update = Update::decode_v1(&merged_all).unwrap();
        let mut txn = server_doc2.transact_mut();
        txn.apply_update(update).unwrap();
    }
    let apply_merged_time = t5.elapsed();
    print_heap("After applying single merged update");
    let ram_with_merged = heap_current();

    let snapshot2 = {
        let txn = server_doc2.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };
    drop(server_doc2);

    println!();

    // =====================================================================
    // SUMMARY
    // =====================================================================
    println!("==========================================================");
    println!("  SUMMARY");
    println!("==========================================================");
    println!("  Cells:                   {}", num_cells);
    println!("  Individual updates:      {} × {:.0} avg bytes = {}",
        updates.len(), avg_update_size, fmt_bytes(total_raw_bytes));
    println!("  Merged update (single):  {}", fmt_bytes(merged_all.len()));
    println!("  Snapshot from Doc:       {}", fmt_bytes(snapshot.len()));
    println!();
    println!("  Doc RAM (apply N updates one by one):  {}", fmt_bytes(ram_with_doc));
    println!("  Doc RAM (apply 1 merged update):       {}", fmt_bytes(ram_with_merged));
    println!("  Raw bytes in storage:                  {}", fmt_bytes(total_raw_bytes));
    println!();
    println!("  Apply {} updates:         {:.2?}", updates.len(), apply_time);
    println!("  Apply 1 merged update:    {:.2?}", apply_merged_time);
    println!("  merge_updates_v1 (all):   {:.2?}", merge_all_time);
    println!();
    println!("  Snapshot == merged: {} (should be true)",
        snapshot.len() == snapshot2.len());
    println!(
        "  RAM / encoded ratio: {:.1}x",
        ram_with_doc as f64 / snapshot.len() as f64,
    );
    println!();
}
