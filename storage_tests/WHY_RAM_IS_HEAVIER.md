# Why y.Map (and other CRDTs) Use Way More RAM Than Their Encoded Size

## The Numbers

From `updates_batch_measure` (10,000 cells):

| What                    | Size       |
|-------------------------|------------|
| Encoded snapshot (disk) | 222 KB     |
| Doc in RAM              | 3.85 MB    |
| **Ratio**               | **~17x**   |

This ratio grows worse with more entries. For 1M map cells expect ~385 MB RAM
for ~22 MB encoded. This doc explains exactly why.

---

## What An Item Looks Like In Memory

Every single key-value pair in a y.Map — and every element in y.Array, and
every character run in y.Text — is an **Item**. This is the core CRDT struct
in `yrs_no_std/src/block.rs`:

```
┌─────────────────────────────────────────────────────────────┐
│ Item (in memory)                                            │
├──────────────────────┬──────────┬───────────────────────────┤
│ Field                │ Size     │ Purpose                   │
├──────────────────────┼──────────┼───────────────────────────┤
│ id: ID               │ 12 bytes │ (client_id: u64, clock:u32)│
│ len: u32             │  4 bytes │ length of content         │
│ left: Option<ItemPtr>│  8 bytes │ ← linked list prev ptr   │
│ right:Option<ItemPtr>│  8 bytes │ → linked list next ptr   │
│ origin: Option<ID>   │ 16 bytes │ left origin (for merge)  │
│ right_origin: Opt<ID>│ 16 bytes │ right origin (for merge) │
│ content: ItemContent │ ~32 bytes│ the actual value (enum)  │
│ parent: TypePtr      │ ~24 bytes│ pointer to parent Branch │
│ redone: Option<ID>   │ 16 bytes │ undo manager bookkeeping │
│ parent_sub: Opt<Arc> │  8 bytes │ map key (Arc<str> ptr)   │
│ moved: Option<ItemPtr│  8 bytes │ move tracking            │
│ info: ItemFlags      │  2 bytes │ bitflags (deleted etc)   │
├──────────────────────┼──────────┼───────────────────────────┤
│ SUBTOTAL (struct)    │~154 bytes│ before heap allocations  │
│ + Box<Item> overhead │  8 bytes │ heap pointer in BlockCell│
│ + alignment padding  │ ~6 bytes │ struct layout padding    │
├──────────────────────┼──────────┼───────────────────────────┤
│ TOTAL per Item       │~168 bytes│ MINIMUM, not counting    │
│                      │          │ content heap allocs      │
└──────────────────────┴──────────┴───────────────────────────┘
```

For a map entry like `"R42C7" → "v42007"`, add:

- **`parent_sub` key string**: `Arc<str>` header (16 bytes)
  + string bytes (5 bytes "R42C7") = ~24 bytes on heap
- **`content`**: `ItemContent::Any(Vec<Any>)` — Vec header (24 bytes in-struct)
  + 1 × `Any::String(SmolStr)` on heap (~32 bytes)
- **Branch.map HashMap entry**: bucket pointer + `Arc<str>` key clone + `ItemPtr`
  ≈ ~48 bytes per entry amortized

**Total per map entry: ~270-350 bytes in RAM.**

---

## What The Same Item Looks Like Encoded (On Disk)

The encoder in `updates/encoder.rs` writes an Item as:

```
┌─────────────────────────────────────────────────────────┐
│ Item (encoded v1)                                       │
├──────────────────────┬───────┬──────────────────────────┤
│ Field                │ Bytes │ Notes                    │
├──────────────────────┼───────┼──────────────────────────┤
│ info byte            │   1   │ flags + content type     │
│ origin ID            │  1-12 │ varint encoded, optional │
│ right_origin ID      │  1-12 │ varint encoded, optional │
│ parent_sub (map key) │  1+N  │ length-prefixed string   │
│ content value        │  1+N  │ the actual value bytes   │
├──────────────────────┼───────┼──────────────────────────┤
│ TOTAL per Item       │ ~20-35│ for a typical map entry  │
│                      │ bytes │                          │
└──────────────────────┴───────┴──────────────────────────┘
```

**The ID itself (`client_id`, `clock`) is NOT written per item.** The update
encoder groups items by client: it writes the client_id once, then a count,
then all that client's items sequentially. Each item's clock is implicit from
the previous item's clock + len.

### What is NOT stored in the encoded form

| RAM field          | In encoded? | Why not                           |
|--------------------|-------------|-----------------------------------|
| `left` pointer     | **No**      | Reconstructed when loading        |
| `right` pointer    | **No**      | Reconstructed when loading        |
| `parent` pointer   | **No**      | Derived from origin/type info     |
| `redone`           | **No**      | Runtime undo state only           |
| `moved`            | **No**      | Runtime move tracking only        |
| `len`              | **No**      | Derived from content              |
| `info` (full u16)  | **No**      | Only a 1-byte derived subset      |
| HashMap buckets    | **No**      | Runtime indexing structure         |
| Box/Arc headers    | **No**      | Rust heap allocation metadata     |
| Alignment padding  | **No**      | CPU alignment requirement only    |
| Vec capacity slack | **No**      | Over-allocated buffer space       |

The encoding also uses **varint** for integers, so a clock value of `5` takes
1 byte instead of 4, and a client_id is variable-length instead of always 8.

---

## The Branch (Map/Array/Text Container) Overhead

Each y.Map / y.Array / y.Text is a `Branch` struct:

```rust
struct Branch {
    start: Option<ItemPtr>,           //  8 bytes — head of linked list
    map: HashMap<Arc<str>, ItemPtr>,  // 48+ bytes — key→item index (Maps only)
    item: Option<ItemPtr>,            //  8 bytes
    name: Option<Arc<str>>,           //  8 bytes
    block_len: u32,                   //  4 bytes
    content_len: u32,                 //  4 bytes
    type_ref: TypeRef,                //  8 bytes
    observers: Observer<...>,         //  variable
    deep_observers: Observer<...>,    //  variable
}
```

For a Map with 10,000 entries, `Branch.map` is a `HashMap<Arc<str>, ItemPtr>`
with 10,000 entries. HashMap overhead:

- ~48 bytes base struct (pointer, length, capacity, hasher)
- Each bucket: ~40-56 bytes (key Arc<str> clone + value ItemPtr + hash + metadata)
- Load factor ~87.5% → ~12.5% wasted buckets
- **Total HashMap cost for 10K entries: ~500-600 KB just for the index**

This HashMap does NOT exist in the encoded form. It's rebuilt from scratch
when you load the doc.

---

## The BlockStore: More Indexing Overhead

All Items live in a `BlockStore`:

```rust
struct BlockStore {
    clients: HashMap<ClientID, ClientBlockList>,
}

struct ClientBlockList {
    list: Vec<BlockCell>,  // Vec of Box<Item> or GC tombstones
}
```

The `clients` HashMap maps each client_id to a Vec of all their items
(sorted by clock). For a single-client doc with 10K items:

- `Vec<BlockCell>` with 10K entries × 8 bytes each = 80 KB
- Plus Vec over-allocation (capacity > len) typically 2x = 160 KB
- Plus the HashMap bucket for 1 client entry

All runtime-only. Not encoded.

---

## Why y.Map Is The Worst Case

### Map: 1 Item per entry, no merging possible

Every `map.insert("key", "value")` creates a separate Item. Even if the same
client inserts 1 million keys in sequence, **each key is a separate Item**
because:

1. Each Item has a different `parent_sub` (the map key)
2. Items can only squash if they're linked-list neighbors with consecutive
   clocks, same `origin`, same `right_origin`, same `parent_sub`, etc.
3. Map entries have different keys → different `parent_sub` → **never squash**

So 1M map entries = 1M Items = 1M × ~300 bytes = **~300 MB minimum**.

### Text: Items merge aggressively

When you type "hello" one character at a time into a y.Text, Yrs creates 5
individual Items initially, but then **squashes** them:

```
Before squash: [Item('h'), Item('e'), Item('l'), Item('l'), Item('o')]
After squash:  [Item("hello")]  ← ONE Item!
```

Squashing conditions (from `Item::try_squash`):
- Same client_id
- Consecutive clocks
- Direct left/right neighbors
- Same deleted status
- Contents are squash-compatible (`String + String → longer String`)

A user typing 10,000 characters sequentially creates **~1 Item** (or a small
handful after pauses/cursor moves). That's ~300 bytes total vs 10,000 × 300.

**For text-heavy docs, RAM overhead is ~1-5x encoded size.**
**For map-heavy docs (sheets), RAM overhead is ~15-25x encoded size.**

### Array: In between

y.Array inserts at positions. Consecutive inserts by the same client at
adjacent positions DO squash (`ItemContent::Any` is squashable). So:

```rust
array.push_back(&mut txn, "a");
array.push_back(&mut txn, "b");
array.push_back(&mut txn, "c");
// May squash to: [Item(["a", "b", "c"])]  ← depends on timing
```

But each `push_back` in a separate transaction won't squash (different txn
means a commit between them, and squashing happens during integration). If
done in a single transaction, they squash. Regardless, Array is better
than Map because entries don't have unique `parent_sub` keys consuming extra
memory.

---

## What Happens When You Load a Merged Update Back?

Your question: "when I load it back, will it be one block?"

**No.** Loading a merged update recreates the same Items. 10,000 map
entries → still 10,000 Items in RAM. `merge_updates_v1` merges the
*encoding*, not the CRDT items. It:

1. Decodes all updates into `Update` structs (lists of Items)
2. Sorts and deduplicates by ID
3. Re-encodes into a single contiguous blob

The encoded blob is smaller because:
- Client ID written once instead of per-update
- No duplicate items across updates
- Delete sets are merged

But when you `apply_update(merged)` into a Doc, each Item gets unpacked back
into full in-memory structs with all pointers, HashMap entries, etc.

**The RAM cost is O(number of items), regardless of how they were encoded.**

This is confirmed by the test output:

```
Doc RAM (apply N updates one by one):  3.85 MB
Doc RAM (apply 1 merged update):       3.85 MB   ← same!
```

---

## Summary: Where The Memory Goes

For a Map with 10,000 entries (encoded ~222 KB, RAM ~3.85 MB):

```
                       Encoded    |    In RAM
─────────────────────────────────────────────────
Item content (values)    ~100 KB  |    ~100 KB      (same)
Item keys (parent_sub)    ~60 KB  |    ~250 KB      (Arc<str> headers)
Origin/right_origin IDs   ~50 KB  |    ~320 KB      (Option<ID> padding)
Info/metadata              ~12 KB |    ~20 KB
─── ENCODING TOTAL ─── ~222 KB   |
                                  |
left/right pointers          —    |    ~160 KB      (linked list)
parent pointers              —    |    ~240 KB      (TypePtr enum)
redone/moved fields          —    |    ~240 KB      (Option<ID/Ptr>)
Branch HashMap index         —    |    ~500 KB      (key→item lookup)
BlockStore Vec               —    |    ~160 KB      (client block list)
Box<Item> heap headers       —    |     ~80 KB      (allocator overhead)
Alignment padding            —    |    ~200 KB      (struct layout)
Vec/HashMap capacity slack   —    |    ~400 KB      (over-allocated)
Allocator metadata           —    |    ~150 KB      (malloc bookkeeping)
─── TOTAL ───────────  ~222 KB    |  ~2,800 KB
                                  |  + ~1 MB misc runtime
```

**~85% of the RAM is metadata that doesn't exist in the encoded form:**
pointers, padding, HashMap indexes, allocator headers, capacity slack.

---

## Practical Implications

1. **Don't hold Docs in RAM** if you can avoid it. Use stateless relay
   (forward raw update bytes) and construct Docs only transiently.

2. **y.Map is the worst data structure for memory** because every entry is
   a separate Item with no squashing. If your sheet has 1M cells, that's
   1M Items regardless.

3. **y.Text is the best** — consecutive edits by the same client merge into
   single Items. A 100K-character document might have only a few hundred Items.

4. **Encoded size is a good approximation of "useful data"** — the ~15-25x
   blowup is pure CRDT bookkeeping overhead needed for conflict resolution.

5. **merge_updates reduces encoded size but NOT RAM cost.** Applying a merged
   update into a Doc costs the exact same RAM as applying the individual ones.

6. **GC doesn't help for maps.** Deleted map entries become `GC { start, end }`
   tombstones (8 bytes each) instead of full Items, but the current/live
   entries always have full Item overhead.
