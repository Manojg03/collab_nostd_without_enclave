# Blocks, Items, and y.Map: A Deep Dive

## Does each cell in a y.Map create a Block?

**Yes.** Every single `map.insert("R0C0", "hello")` creates exactly one `Item`
(a.k.a. Block). There is a 1:1 correspondence:

```
map.insert("R0C0", "hello")   →  Item #0
map.insert("R0C1", "world")   →  Item #1
map.insert("R0C2", "foo")     →  Item #2
```

If you later overwrite a key:

```
map.insert("R0C0", "updated") →  Item #3  (Item #0 gets marked DELETED)
```

Now there are **4 Items** total: Item #0 (deleted), Item #1, Item #2, Item #3.
The deleted Item #0 still exists — it's just flagged. GC can compress it later
to a `GC { start, end }` tombstone (8 bytes instead of ~300).

---

## Every Field of an Item, Explained

Let's say we have this code (single client, client_id = `42`):

```rust
let doc = Doc::with_client_id(42);
let map = doc.get_or_insert_map("sheet");
let mut txn = doc.transact_mut();

map.insert(&mut txn, "R0C0", "hello");   // → Item A
map.insert(&mut txn, "R0C1", "world");   // → Item B
map.insert(&mut txn, "R0C2", "foo");     // → Item C
```

Here is **Item A** in full detail:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Item A                                                                  │
├────────────────┬─────────────────────┬──────────────────────────────────┤
│ Field          │ Value               │ What it means                   │
├────────────────┼─────────────────────┼──────────────────────────────────┤
│ id             │ { client:42,        │ Globally unique identity.       │
│                │   clock:0 }         │ Client 42's 0th operation.      │
│                │                     │                                  │
│ len            │ 1                   │ This item covers 1 logical unit.│
│                │                     │ (For text, could be 5 if it     │
│                │                     │ holds "hello" after squashing.) │
│                │                     │                                  │
│ left           │ None                │ ★ POINTER to the Item           │
│                │                     │ physically to our left in the   │
│                │                     │ linked list. For map entries,   │
│                │                     │ this is usually None (each key  │
│                │                     │ has its own slot, no ordering). │
│                │                     │                                  │
│ right          │ None                │ ★ POINTER to the Item physically│
│                │                     │ to our right. For the current   │
│                │                     │ (latest) value of a map key,    │
│                │                     │ this is None.                   │
│                │                     │                                  │
│ origin         │ None                │ The ID of whatever was to our   │
│                │                     │ LEFT when we were CREATED.      │
│                │                     │ Used for conflict resolution.   │
│                │                     │ For first value of a map key,   │
│                │                     │ there's nothing to the left →   │
│                │                     │ None.                           │
│                │                     │                                  │
│ right_origin   │ None                │ The ID of whatever was to our   │
│                │                     │ RIGHT when we were CREATED.     │
│                │                     │ Same idea, but right side.      │
│                │                     │                                  │
│ content        │ Any(["hello"])      │ The actual value. Wrapped in    │
│                │                     │ ItemContent::Any(Vec<Any>).     │
│                │                     │                                  │
│ parent         │ Branch(→ sheet map) │ ★ POINTER to the Branch struct  │
│                │                     │ that owns us (the "sheet" map). │
│                │                     │                                  │
│ parent_sub     │ Some("R0C0")       │ The MAP KEY. This is what makes │
│                │                     │ it a map entry vs a list entry. │
│                │                     │ Array/Text items have None here.│
│                │                     │                                  │
│ redone         │ None                │ If this item was undone and then│
│                │                     │ redone, points to the redo item.│
│                │                     │ Runtime undo-manager state only.│
│                │                     │                                  │
│ moved          │ None                │ If this item was moved (via     │
│                │                     │ y.Move), points to the Move     │
│                │                     │ item. Runtime tracking only.    │
│                │                     │                                  │
│ info           │ KEEP | COUNTABLE    │ Bit flags:                      │
│                │                     │  KEEP=don't GC, COUNTABLE=yes,  │
│                │                     │  DELETED=no, MARKED=no          │
└────────────────┴─────────────────────┴──────────────────────────────────┘
```

### The key distinction: `origin` vs `left`

These are the most confusing fields. They look similar but serve completely
different purposes:

| Field            | Set when?        | Changes later?  | Purpose                     |
|------------------|------------------|-----------------|-----------------------------|
| **`origin`**     | At creation time | **Never**       | Conflict resolution (YATA)  |
| **`left`**       | At integration   | Yes, on insert  | Current linked-list pointer |
| **`right_origin`** | At creation    | **Never**       | Conflict resolution (YATA)  |
| **`right`**      | At integration   | Yes, on insert  | Current linked-list pointer |

`origin` is an **ID** (immutable snapshot of "who was to my left when I was
born"). `left` is a **pointer** (mutable, updated as items are inserted/deleted
around us).

For map entries, both `origin` and `left` are usually `None` because map
entries don't really have a meaningful "left neighbor" — each key is
independent.

---

## What Gets Encoded vs What Doesn't

Here's Item A being encoded:

```
ENCODED (written to bytes):           NOT ENCODED (RAM only):
─────────────────────────             ─────────────────────────
✓ info byte (1 byte)                  ✗ left pointer
✓ origin ID (if present)              ✗ right pointer
✓ right_origin ID (if present)        ✗ parent pointer (Branch)
✓ parent name "sheet" (if root)       ✗ redone
✓ parent_sub "R0C0"                   ✗ moved
✓ content "hello"                     ✗ len (derived from content)
                                      ✗ info flags (full u16)
                                      ✗ HashMap bucket entry
                                      ✗ BlockStore Vec slot
                                      ✗ Box<Item> header
                                      ✗ Arc refcount headers
                                      ✗ alignment padding
```

The ID (`client:42, clock:0`) is written **once per client** at the start of
that client's block run, not repeated per Item.

### Why can those fields be skipped?

**`left` / `right` pointers**: These are just linked-list navigation. When you
load the item back, the integration algorithm rebuilds these by walking the
existing items and inserting the new item at the right position. The `origin`
and `right_origin` fields tell the algorithm *where* to insert.

**`parent` pointer**: The encoded form stores either the root type name
(`"sheet"`) or the parent item's ID. When loading, the name is resolved to the
actual `Branch` struct in memory. No need to encode a raw memory address.

**`redone` / `moved`**: These are undo-manager and move-tracking state. They
exist only during a live editing session. When you save and reload, undo
history is gone — this is expected behavior.

**`len`**: Derived from the content. `"hello"` → len 1 (for map). For text
`"hello"` → len 5. The decoder calculates this.

---

## Before vs After Encode→Decode: Are the Blocks the Same?

### Setup: 3 map entries, then encode and reload

```rust
// ORIGINAL DOC
let doc1 = Doc::with_client_id(42);
let map = doc1.get_or_insert_map("sheet");
{
    let mut txn = doc1.transact_mut();
    map.insert(&mut txn, "R0C0", "hello");   // Item A: id=(42,0)
    map.insert(&mut txn, "R0C1", "world");   // Item B: id=(42,1)
    map.insert(&mut txn, "R0C2", "foo");     // Item C: id=(42,2)
}

// ENCODE
let encoded = doc1.transact().encode_state_as_update_v1(&StateVector::default());

// LOAD INTO NEW DOC
let doc2 = Doc::with_client_id(99);  // different client
let map2 = doc2.get_or_insert_map("sheet");
{
    let mut txn = doc2.transact_mut();
    let update = Update::decode_v1(&encoded).unwrap();
    txn.apply_update(update).unwrap();
}
```

### What happens during `apply_update` — step by step:

```
1. DECODE: Read bytes → Create Items with:
   ┌──────────┬──────────┬──────┬───────┬──────────┬──────────────┬───────────┬────────────┐
   │ id       │ content  │ left │ right │ origin   │ right_origin │ parent    │ parent_sub │
   ├──────────┼──────────┼──────┼───────┼──────────┼──────────────┼───────────┼────────────┤
   │ (42, 0)  │ "hello"  │ None │ None  │ None     │ None         │ Named     │ "R0C0"     │
   │          │          │      │       │          │              │ ("sheet") │            │
   │ (42, 1)  │ "world"  │ None │ None  │ None     │ None         │ Named     │ "R0C1"     │
   │          │          │      │       │          │              │ ("sheet") │            │
   │ (42, 2)  │ "foo"    │ None │ None  │ None     │ None         │ Named     │ "R0C2"     │
   │          │          │      │       │          │              │ ("sheet") │            │
   └──────────┴──────────┴──────┴───────┴──────────┴──────────────┴───────────┴────────────┘
   Note: left, right = None. Parent = Named("sheet"), not a pointer yet.

2. REPAIR (for each Item):
   - origin is None → left stays None ✓
   - right_origin is None → right stays None ✓
   - parent = Named("sheet") → resolved to Branch pointer:
     parent = Branch(→ actual sheet Branch in doc2's Store)

3. INTEGRATE (for each Item):
   - For Item A (R0C0): left=None, right=None, no existing entry for "R0C0"
     → Insert into Branch.map: "R0C0" → Item A
     → left stays None, right stays None
   
   - For Item B (R0C1): Same — new key, no conflicts
     → Insert into Branch.map: "R0C1" → Item B
   
   - For Item C (R0C2): Same
     → Insert into Branch.map: "R0C2" → Item C

4. PUSH TO BLOCKSTORE:
   BlockStore.clients[42] = [Item A, Item B, Item C]  (Vec of BlockCell)

5. COMMIT (try_squash):
   Can Item A squash with Item B?
   - Same client (42) ✓
   - Consecutive clocks (0+1 == 1) ✓
   - Item B's origin == Item A's last_id? → B.origin=None, A.last_id=(42,0) → None ≠ Some((42,0)) ✗
   → NO SQUASH. Map items almost never squash because their origins don't chain.
```

### Final state comparison:

```
════════════════════════════════════════════════════════════════════════
  BEFORE encoding (doc1)              AFTER decode + load (doc2)
════════════════════════════════════════════════════════════════════════

  Item A                              Item A'
  ├ id: (42, 0)          ← SAME →    ├ id: (42, 0)
  ├ len: 1               ← SAME →    ├ len: 1
  ├ left: None           ← SAME →    ├ left: None
  ├ right: None          ← SAME →    ├ right: None
  ├ origin: None         ← SAME →    ├ origin: None
  ├ right_origin: None   ← SAME →    ├ right_origin: None
  ├ content: "hello"     ← SAME →    ├ content: "hello"
  ├ parent: Branch(→X)   ≈ EQUIV →   ├ parent: Branch(→Y)    ★
  ├ parent_sub: "R0C0"  ← SAME →    ├ parent_sub: "R0C0"
  ├ redone: None         ← SAME →    ├ redone: None
  ├ moved: None          ← SAME →    ├ moved: None
  └ info: KEEP|COUNT     ← SAME →    └ info: KEEP|COUNT

  Branch "sheet"                      Branch "sheet"
  ├ map: {"R0C0"→A,      ≈ EQUIV →   ├ map: {"R0C0"→A',
  │       "R0C1"→B,                   │       "R0C1"→B',
  │       "R0C2"→C}                   │       "R0C2"→C'}
  └ start: None          ← SAME →    └ start: None

  BlockStore                          BlockStore
  └ clients[42]: [A,B,C] ≈ EQUIV →   └ clients[42]: [A',B',C']

════════════════════════════════════════════════════════════════════════
  ★ = different memory address, same logical thing (points to the
      Branch struct for "sheet" in the respective Doc instance)
════════════════════════════════════════════════════════════════════════
```

**Answer: YES, the block structures are logically identical.** The only
differences are:

1. **Raw memory addresses** — pointers (left, right, parent) point to
   different heap locations because it's a different Doc instance. But they
   point to the equivalent logical items.

2. **Doc metadata** — doc2 has `client_id=99` (vs 42), and its Store is a
   fresh instance. But the Items themselves are bit-for-bit identical in all
   their logical fields.

3. **Transient fields** — if `redone` or `moved` were set in doc1, they'd
   be **lost** after encoding/decoding. But for a freshly-created doc they're
   None anyway.

---

## Now: What If We OVERWRITE a Key?

This is where it gets interesting. Let's update `R0C0`:

```rust
// In doc1, after the initial inserts:
{
    let mut txn = doc1.transact_mut();
    map.insert(&mut txn, "R0C0", "UPDATED");  // Item D: id=(42,3)
}
```

### What happens in memory:

```
Item D is created:
  id: (42, 3)
  origin: None
  right_origin: None
  parent: Branch(→ sheet)
  parent_sub: Some("R0C0")
  content: "UPDATED"

During integration:
  1. Right is None, parent_sub is "R0C0"
     → So D becomes the CURRENT value: Branch.map["R0C0"] = D
  2. The OLD value (Item A) gets DELETED:
     → A.info |= DELETED flag
     → But A still exists in BlockStore!

After integration:
  BlockStore.clients[42] = [A(deleted), B, C, D]
  Branch.map = {"R0C0"→D, "R0C1"→B, "R0C2"→C}
```

### Encoding this:

```
ENCODED bytes contain ALL 4 items:
  Item A: id=(42,0), content="hello",   info has DELETED bit set
  Item B: id=(42,1), content="world"
  Item C: id=(42,2), content="foo"
  Item D: id=(42,3), content="UPDATED"

The encoder writes Item A with a content-ref that indicates "deleted"
(or keeps the original content with the deleted flag encoded in info byte).
```

### Loading it back:

```
All 4 Items are recreated in doc2.
Item A' is created, integrated, then during D' integration:
  - D' becomes Branch.map["R0C0"]
  - A' is deleted (same as original)

Final state: IDENTICAL to doc1. 4 Items. A' deleted, D' is current R0C0 value.
```

---

## Map vs Text vs Array: Squashing Behavior

### Map: Items NEVER squash

```
map.insert("k1", "a");   →  Item(id=0, parent_sub="k1", origin=None)
map.insert("k2", "b");   →  Item(id=1, parent_sub="k2", origin=None)
```

Can these squash? **No.** Different `parent_sub` keys → `try_squash` fails.
Even if same client, consecutive clocks. The `origin` check also fails:
Item 1's `origin` is `None`, but squash requires `other.origin == Some(self.last_id())`.

**Each map entry is a separate Item forever.** 10,000 keys = 10,000 Items.

### Text: Items DO squash

```
text.insert(0, "h");   →  Item(id=0, content="h", parent_sub=None, origin=None)
text.insert(1, "e");   →  Item(id=1, content="e", parent_sub=None, origin=Some(42,0))
text.insert(2, "l");   →  Item(id=2, content="l", parent_sub=None, origin=Some(42,1))
```

On commit, try_squash:
- Same client? ✓
- Consecutive clocks? 0+1=1 ✓
- `Item1.origin == Some(Item0.last_id())`? `Some(42,0) == Some(42,0)` ✓
- Same right_origin? Both None ✓
- Direct neighbors? ✓
- Content squashable? String + String → ✓

**Result: [Item(id=0, content="hel", len=3)]** — one Item!

After encoding and reloading: still one Item with "hel". **Text survives
round-trip with significantly fewer Items.**

### Array: Items CAN squash (sometimes)

```
// In SAME transaction:
array.push_back(&mut txn, "a");  →  Item(id=0, origin=None)
array.push_back(&mut txn, "b");  →  Item(id=1, origin=Some(42,0))
// These CAN squash: consecutive, same parent_sub (None), origin chains correctly.
// Result: [Item(id=0, content=Any(["a","b"]), len=2)]

// In SEPARATE transactions:
// Each txn commits independently. Whether they squash depends on whether
// the commit's try_squash pass sees them as neighbors with chaining origins.
// Usually they DO squash if no other client interleaved.
```

Array is better than Map because items share the same `parent_sub` (`None`)
and their origins chain naturally when appending sequentially.

---

## Summary

| Question | Answer |
|----------|--------|
| Does each map cell = one Item/Block? | **Yes, always.** |
| Are blocks identical after encode→decode→load? | **Logically yes.** Same id, origin, right_origin, content, parent_sub, info flags. Just different memory addresses for pointers. |
| Are `redone`/`moved` preserved? | **No.** Lost on encode. They're runtime-only. |
| Are `left`/`right` pointers the same? | **Logically equivalent.** They're rebuilt by the integration algorithm using `origin`/`right_origin` to find the correct position. For map items with no origin, they end up `None` again. |
| Can map Items squash? | **No.** Different `parent_sub` keys prevent squashing. |
| Can text Items squash? | **Yes.** Consecutive same-client chars merge into one Item. |
| Does overwriting a key delete the old Item? | **Yes**, but the old Item stays in BlockStore with DELETED flag. It's needed for conflict resolution with concurrent edits from other clients. |
