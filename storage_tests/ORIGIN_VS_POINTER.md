# Origin vs Pointer: How Yrs Rebuilds left/right After Loading

## The Confusion

We have four fields that look like two pairs of the same thing:

```
origin       ←→  left
right_origin ←→  right
```

If `left`/`right` aren't saved during encoding, how do we know what's to the
left and right after loading? And if `origin`/`right_origin` tell us the same
thing, why do we need both?

**They are NOT the same thing.** They start out equal, but diverge as other
items get inserted between them.

---

## The Fundamental Difference

| | `origin` / `right_origin` | `left` / `right` |
|---|---|---|
| **What** | ID (client_id + clock) | Pointer (memory address) |
| **Set when** | At creation time, ONCE | At integration, then UPDATED |
| **Changes?** | **NEVER** | **YES** — every time something is inserted next to us |
| **Purpose** | Conflict resolution (YATA) | Linked-list traversal |
| **Encoded?** | **YES** | **NO** |

Think of it this way:

- **`origin`** = "who was to my left **when I was born**" (birth certificate)
- **`left`** = "who is to my left **right now**" (current neighbor)

---

## Example: Watching Them Diverge

### Step 1: Client A types "A"

```
Document: [ A ]

Item_A:
  id:           (1, 0)
  origin:       None        ← nothing was to my left when I was born
  right_origin: None        ← nothing was to my right when I was born
  left:         None        ← nothing to my left right now
  right:        None        ← nothing to my right right now
```

At this point, origin == left and right_origin == right. All good.

### Step 2: Client A types "B" after "A"

```
Document: [ A  B ]

Item_B:
  id:           (1, 1)
  origin:       (1, 0)      ← "A" was to my left when I was born
  right_origin: None        ← nothing was to my right when I was born
  left:         →Item_A     ← "A" is to my left right now
  right:        None        ← nothing to my right right now

Item_A (updated):
  left:         None
  right:        →Item_B     ← B is now to A's right (was None before!)
```

Still, origin matches left for both items.

### Step 3: Client A types "C" BETWEEN "A" and "B"

```
Document: [ A  C  B ]

Item_C:
  id:           (1, 2)
  origin:       (1, 0)      ← "A" was to my left when I was born
  right_origin: (1, 1)      ← "B" was to my right when I was born
  left:         →Item_A     ← "A" is to my left right now
  right:        →Item_B     ← "B" is to my right right now
```

But now look at **Item_B** — its pointers got UPDATED:

```
Item_B (after C was inserted):
  origin:       (1, 0)      ← STILL "A" — my birth certificate never changes
  right_origin: None        ← STILL None
  left:         →Item_C     ← NOW "C" is to my left! (was Item_A before)
  right:        None
```

**HERE'S THE DIVERGENCE:**
- Item_B's `origin` = `(1,0)` → points to "A"
- Item_B's `left` = `→Item_C` → points to "C"

They're **different**. "A" was B's neighbor when B was created, but "C" was
inserted between them later. The origin remembers the original neighbor;
left tracks the current one.

---

## Why Do We Need the Original (origin) At All?

`origin` is needed for **conflict resolution** when two clients insert at the
same position concurrently. The current `left`/`right` pointers are useless for
this because they only reflect the local state.

### Concurrent insertion example

Starting state on BOTH clients:

```
Document: [ A   B ]
Item_A: id=(1,0)
Item_B: id=(1,1), origin=(1,0)
```

**Client 1** (client_id=1) inserts "X" between A and B:
```
Item_X: id=(1,2), origin=(1,0), right_origin=(1,1)
         left=→A, right=→B
```

**Client 2** (client_id=2) CONCURRENTLY inserts "Y" between A and B:
```
Item_Y: id=(2,0), origin=(1,0), right_origin=(1,1)
         left=→A, right=→B
```

Now they sync. Client 1 receives Item_Y. Client 2 receives Item_X.

**On Client 1**, trying to integrate Item_Y:

```
Item_Y arrives with:
  origin:       (1,0)  → A was to Y's left at birth
  right_origin: (1,1)  → B was to Y's right at birth

repair() runs:
  origin (1,0) → look up in store → found Item_A → left = →Item_A
  right_origin (1,1) → look up in store → found Item_B → right = →Item_B

But wait: Item_A's current right is Item_X, not Item_B!
  Item_A.right = →Item_X (because X was already inserted)

So: left.right (→Item_X) ≠ this.right (→Item_B)
  → left_has_other_right_than_self = TRUE
  → Enter CONFLICT RESOLUTION
```

### The YATA Conflict Resolution Algorithm

The algorithm scans items between `left` and `right` (the original neighbors
from our origin/right_origin). It encounters items that were also inserted
between those same two positions — these are "conflicts."

```
Current linked list: ... A → X → B ...

We want to insert Y. Its origin is A, right_origin is B.
So the "conflict zone" is everything between A and B: just X.

Scanning:
  o = left.right = Item_X

  Check: is o == this.right (Item_B)?  No → continue.

  Check: this.origin == o.origin?
    Y.origin = (1,0), X.origin = (1,0)  → YES, same origin!

  Case 1: Both were inserted after A. Tie-break by client_id.
    o.id.client = 1, this.id.client = 2
    1 < 2?  YES → left = Item_X, clear conflicts.

  This means: Y goes AFTER X.

  Next: o = X.right = Item_B = this.right → loop ends.

Result: left = →Item_X
```

Reconnect linked list:

```
... A → X → Y → B ...
```

**On Client 2**, the same algorithm integrates Item_X:

```
Item_X arrives: origin=(1,0), right_origin=(1,1)

repair():
  left = →Item_A
  right = →Item_B

But Item_A.right is currently →Item_Y (because Y was inserted locally).

Conflict zone between A and B: just Y.

Scanning:
  o = Item_Y

  this.origin == o.origin?
    X.origin = (1,0), Y.origin = (1,0) → YES, same origin.

  Case 1: o.id.client = 2, this.id.client = 1
    2 < 1?  NO.

  Check: this.right_origin == o.right_origin?
    X.right_origin = (1,1), Y.right_origin = (1,1) → YES → BREAK.

  left stays as →Item_A.

Result: left = →Item_A → X goes RIGHT AFTER A, BEFORE Y.
```

Reconnect:

```
... A → X → Y → B ...
```

**Both clients converge to the same order: A X Y B** ✓

---

## The Tie-Breaking Rule

When two items have the **same origin** (were both inserted after the same
item), the YATA algorithm uses this rule:

```
if conflicting_item.client_id < this.client_id:
    // conflicting item goes first (to the left)
    // → move our left pointer past it
else if same right_origin:
    // we go first (to the left)
    // → break, keep current left
```

**Lower client_id wins the left position.** This is deterministic: every
replica will make the same decision regardless of message ordering.

---

## After Encode → Decode → Load: Reconstruction

Let's trace what happens when we encode `[A, X, Y, B]` and load it into a
fresh Doc.

### What's encoded (per item):

```
Item_A: info_byte, origin=None, right_origin=None, content="A"
Item_X: info_byte, origin=(1,0), right_origin=(1,1), content="X"
Item_Y: info_byte, origin=(1,0), right_origin=(1,1), content="Y"
Item_B: info_byte, origin=(1,0), right_origin=None, content="B"
```

Note: `left`, `right` are NOT in there. Only `origin`, `right_origin`.

### After decode (before integration):

```
All items have left=None, right=None.
origin and right_origin are populated from bytes.
```

### Integration of Item_A (id=(1,0)):

```
repair():
  origin=None → left stays None
  right_origin=None → right stays None
  parent=Named("text") → resolved to Branch pointer

integrate():
  left=None, right=None. No conflicts.
  Insert at start: Branch.start = →Item_A
  
Linked list: [A]
```

### Integration of Item_B (id=(1,1)):

```
repair():
  origin=(1,0) → look up → found Item_A → left = →Item_A
  right_origin=None → right stays None

integrate():
  left=→Item_A. Item_A.right is currently None.
  left.right (None) == this.right (None) → no conflict.
  Insert after A: A.right = →Item_B, B.right = None (what A.right was)

Linked list: [A → B]
```

### Integration of Item_X (id=(1,2)):

```
repair():
  origin=(1,0) → left = →Item_A
  right_origin=(1,1) → right = →Item_B

integrate():
  left=→Item_A. Item_A.right is currently →Item_B.
  left.right (→Item_B) == this.right (→Item_B)?
    YES → no conflict! (the space between A and B is empty)
  Insert after A: A.right = →Item_X, X.right = →Item_B (what A.right was)
  B.left = →Item_X

Linked list: [A → X → B]
```

### Integration of Item_Y (id=(2,0)):

```
repair():
  origin=(1,0) → left = →Item_A
  right_origin=(1,1) → right = →Item_B

integrate():
  left=→Item_A. Item_A.right is currently →Item_X.
  left.right (→Item_X) ≠ this.right (→Item_B)
    → left_has_other_right_than_self = TRUE
    → ENTER CONFLICT RESOLUTION

  Scan: o = Item_A.right = Item_X
    Is o == this.right (Item_B)?  No.
    this.origin == o.origin?  (1,0) == (1,0) → YES
    
    Case 1: o.client=1, this.client=2. 1 < 2? YES.
      → left = →Item_X, clear conflicts.
    
    Next: o = Item_X.right = Item_B = this.right → loop ends.

  Result: left = →Item_X

  Insert after X: X.right = →Item_Y, Y.right = →Item_B
  B.left = →Item_Y

Linked list: [A → X → Y → B]
```

**Final state is IDENTICAL to what we had before encoding.** The `origin` and
`right_origin` fields carry enough information to reconstruct the exact
position of every item, even resolving conflicts the same way.

---

## Now the Twist: What If Items Were Squashed?

In our text editor, Items B and X might get squashed if they're from the same
client and consecutive:

```
Before squash: [A(id=1,0), B(id=1,1)]
After squash:  [AB(id=1,0, len=2, content="AB")]
```

Now Item_AB's last_id is `(1, 1)` (clock 0 + len 2 - 1).

When another item has `origin=(1,0)` (meaning "was inserted right after A"),
`repair()` calls `get_item_clean_end(&(1,0))`:

```
get_item_clean_end((1,0)):
  Found squashed item AB with id=(1,0), len=2.
  Clock 0 is INSIDE this item (0..2).
  SPLIT AB at the boundary:
    Item_A_part: id=(1,0), len=1, content="A"
    Item_B_part: id=(1,1), len=1, content="B"
  Return pointer to Item_A_part (the "end" of clock 0)
```

The squashed item gets **split back** on demand so the origin resolution
is exact. This is why the methods are called `get_item_clean_end` and
`get_item_clean_start` — they "clean" (split) blocks at the exact boundary.

---

## Summary

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│   origin / right_origin          left / right                       │
│   ═══════════════════            ════════════                       │
│                                                                      │
│   • Immutable IDs                • Mutable pointers                 │
│   • Set at creation              • Set at integration               │
│   • Never change                 • Change on every insert nearby    │
│   • ENCODED (saved)              • NOT encoded (rebuilt)            │
│   • Used for conflict            • Used for linked-list traversal   │
│     resolution (YATA)              (reading doc content)            │
│                                                                      │
│   "Who was my neighbor           "Who is my neighbor                │
│    when I was born"               right now"                        │
│                                                                      │
│   origin ──repair()──→ left      (initial value)                    │
│   right_origin ──repair()──→ right  (initial value)                │
│                                                                      │
│   Then integrate() may change left/right via                        │
│   conflict resolution, but origin/right_origin stay the same.       │
│                                                                      │
│   On reload: origins are decoded → repair resolves to pointers     │
│   → integrate reconnects the linked list → same result as before.  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Why we can skip left/right in encoding:

1. `origin`/`right_origin` are **sufficient** to determine where an item
   belongs. The YATA algorithm uses them to find the exact insertion point.

2. `left`/`right` are **derived state** — they're the *result* of running
   the integration algorithm. Saving them would be redundant.

3. Even when other items have been inserted between the original neighbors,
   the conflict resolution algorithm handles it correctly by scanning the
   items in the conflict zone and applying deterministic tie-breaking rules.

4. If items were squashed (merged), the `get_item_clean_end/start` methods
   split them at the exact boundary to resolve the origin pointer correctly.

**The encode/decode cycle is lossless for the CRDT semantics.** The only
things lost are transient runtime state (`redone`, `moved`) that don't
affect document content.

---

## Origins Can Point to ANY Client's Block (Not Just Your Own)

A common misconception: "origin must refer to a block from the same client."

**Wrong.** The origin field is set to **whichever block happens to be to the
left (or right) at the moment of insertion** — and that block could belong to
any client in the system.

### Why this is obvious once you think about it

Imagine a collaborative text editor. Client A types "Hello", Client B types
"World" after it, and now Client C places their cursor between "Hello" and
"World" and types a space:

```
Before C types:
  [H(A:0)] [e(A:1)] [l(A:2)] [l(A:3)] [o(A:4)] [W(B:0)] [o(B:1)] ...

Client C inserts " " between "o" and "W":
  Item_space:
    id:           (C, 0)        ← C's own ID
    origin:       (A, 4)        ← "o" from Client A was to the left
    right_origin: (B, 0)        ← "W" from Client B was to the right
```

C's origin points to A's block. C's right_origin points to B's block.
**Three different clients involved in one insertion.**

### The code that proves it

In `transaction.rs`, the `create_item` function:

```rust
pub(crate) fn create_item<T: Prelim>(
    &mut self,
    pos: &ItemPosition,
    value: T,
    parent_sub: Option<Arc<str>>,
) -> Option<ItemPtr> {
    let (left, right, origin, id) = {
        let store = self.store_mut();
        let left = pos.left;
        let right = pos.right;

        // ORIGIN IS SET FROM THE LEFT NEIGHBOR — WHICH CAN BE ANY CLIENT
        let origin = if let Some(item) = pos.left.as_deref() {
            Some(item.last_id())  // <-- last_id() returns ID { client, clock }
                                  //     of WHATEVER block is to the left
        } else {
            None
        };

        let client_id = store.client_id;
        let id = ID::new(client_id, store.get_local_state());
        (left, right, origin, id)
    };

    let mut block = Item::new(
        id,                                  // OUR client_id + clock
        left,
        origin,                              // LEFT neighbor's ID — any client
        right,
        right.map(|r| r.id().clone()),       // RIGHT neighbor's ID — any client
        pos.parent.clone(),
        parent_sub,
        content,
    )?;
```

Notice: `origin` is `pos.left.last_id()`. The code doesn't check "is this
the same client?" — it doesn't care. The left neighbor is whatever block
occupies that position in the linked list right now.

### Why this works

The `ID { client, clock }` is a **globally unique identifier**. Every block in
the entire system has a unique (client_id, clock) pair. So when we store
`origin = (A, 4)`, any replica can look up that exact block in its BlockStore
(which is a `HashMap<ClientID, Vec<Block>>`). Client A's blocks, Client B's
blocks — they're all there.

---

## The Update Flow: Local-First, Then Broadcast

### How insertion actually works (the full pipeline)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLIENT SIDE                                  │
│                                                                     │
│  1. User types "X"                                                  │
│     ↓                                                               │
│  2. doc.transact_mut() → opens a write transaction                  │
│     ↓                                                               │
│  3. text.insert(&mut txn, pos, "X")                                 │
│     ↓                                                               │
│  4. create_item() builds the Item, sets origin from neighbors       │
│     ↓                                                               │
│  5. block_ptr.integrate(self, 0)  ← DIRECTLY into local Doc        │
│     store.blocks.push_block(block)                                  │
│     ↓                                                               │
│  6. Transaction commits (drops)                                     │
│     ↓                                                               │
│  7. observe_update_v1 callback fires → encodes the new blocks as    │
│     an Update binary message                                        │
│     ↓                                                               │
│  8. Sends the Update to the server via WebSocket                    │
│                                                                     │
│  THE CHARACTER "X" IS ALREADY VISIBLE LOCALLY AT STEP 5.            │
│  Steps 7-8 happen AFTER. This is why it feels instant.              │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        SERVER SIDE                                  │
│                                                                     │
│  9. Server receives the Update bytes                                │
│     ↓                                                               │
│  10. handle_sync_step2() decodes the Update                         │
│      ↓                                                              │
│  11. txn.apply_update(update)                                       │
│      → update.integrate() on the server's Doc                       │
│      → repair() resolves origins to pointers                        │
│      → integrate() links into the server's linked list              │
│      ↓                                                              │
│  12. Server Doc's observe_update_v1 callback fires                  │
│      ↓                                                              │
│  13. BroadcastGroup pushes the Update to its channel                │
│      ↓                                                              │
│  14. ALL subscribers receive the message (including the sender!)    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                 OTHER CLIENTS (AND THE SENDER)                      │
│                                                                     │
│  15. Client receives the Update via WebSocket                       │
│      ↓                                                              │
│  16. txn.apply_update(update)                                       │
│      → update.integrate() checks the state vector                   │
│      ↓                                                              │
│  17a. OTHER clients: blocks are new → integrate normally            │
│  17b. SENDER client: blocks already exist → SKIP (no-op)           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key insight: It's LOCAL-FIRST

The insertion happens in the **local Doc immediately** at step 5. The user sees
the result before anything goes to the network. The network is purely for
**replication** — syncing the change to other replicas.

This is why collaborative editors feel responsive even on slow connections:
you're typing into your local CRDT, not waiting for server round-trips.

### The server code (from `protocol.rs`)

```rust
fn handle_sync_step2(
    &self,
    awareness: &Awareness,
    update: Update,
) -> Result<Option<Message>, Error> {
    let mut txn = awareness.doc().transact_mut();
    txn.apply_update(update)?;    // applies to server's Doc
    Ok(None)                      // no reply needed
}
```

Applying the update to the server's Doc triggers `observe_update_v1`, which is
how the BroadcastGroup picks it up:

```rust
let doc_sub = awareness
    .doc()
    .observe_update_v1(move |_txn, u| {
        let mut encoder = EncoderV1::new();
        encoder.write_var(MSG_SYNC);
        encoder.write_var(MSG_SYNC_UPDATE);
        encoder.write_buf(&u.update);
        let msg = encoder.to_vec();
        let _ = sink.send(msg);   // pushes to broadcast channel
    })
    .unwrap();
```

---

## Does the Sender Receive Its Own Update Back?

**Yes.** The broadcast channel has no sender-exclusion logic. Every subscriber
gets every message.

```rust
// BroadcastGroup::broadcast — sends to ALL, no filtering
pub fn broadcast(&self, msg: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
    self.sender.send(msg)?;  // tokio::sync::broadcast — all receivers get it
    Ok(())
}

// Each client subscribes:
let mut receiver = self.sender.subscribe();
// and forwards everything to its WebSocket:
while let Ok(msg) = receiver.recv().await {
    if write_sink.send(msg).await.is_err() { break; }
}
```

**No sender ID is tracked, no "skip if same client" logic.**

### Why this is safe (and actually elegant)

When the sender's client receives its own update back, `apply_update` calls
`update.integrate()`, which checks the **state vector**:

```
State vector before applying: { client_1: 5 }  ← we already have clocks 0..4

Incoming update contains: client_1, clock=4 (the block we just created)

State vector says: we already have clock 4 (5 means next expected is 5).
→ This block is a DUPLICATE. Skip it.
```

The state vector acts as a natural deduplication mechanism. Already-integrated
blocks are simply ignored. Zero wasted work beyond the initial check.

This is a deliberate design choice: it's simpler and more robust than trying
to track which client sent which message on the server side. The CRDT handles
duplicates by design, so why add complexity?

---

## The Two-Level Architecture: BlockStore vs CRDT Linked List

(This matches the diagram with "Root types", "Block store", "Clients",
"Client block list", and "Pointer to a CRDT list head")

### Level 1: BlockStore — Physical Storage (by Client)

```
BlockStore = HashMap<ClientID, ClientBlockList>

┌──────────────────────────────────────────────────────────────┐
│  BlockStore                                                  │
│                                                              │
│  Client A ──→ [ A:0 ][ A:1 ][ A:2 ]    (Vec<BlockCell>)    │
│  Client B ──→ [ B:0 ][ B:1 ]            (Vec<BlockCell>)    │
│  Client C ──→ [ C:0 ][ C:1 ][ C:2 ]    (Vec<BlockCell>)    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

Each client's blocks are stored in a **sorted Vec**, ordered by clock.
Looking up an ID like `(B, 1)` → go to Client B's list → binary search for
clock 1 → O(log n).

```rust
pub(crate) struct BlockStore {
    clients: HashMap<ClientID, ClientBlockList, BuildHasherDefault<ClientHasher>>,
}

pub(crate) struct ClientBlockList {
    list: Vec<BlockCell>,  // sorted by clock
}
```

### Level 2: Branch — Logical Document Order (the CRDT linked list)

```
Branch ("text")
  start: →Item ───→  First item in the document

The doubly-linked list (document order):
  ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐
  │ A:0  │←──→│ C:0  │←──→│ A:1  │←──→│ B:0  │←──→│ C:2  │
  │ "H"  │    │ " "  │    │ "e"  │    │ "!"  │    │ "y"  │
  └──────┘    └──────┘    └──────┘    └──────┘    └──────┘
     ↑
  Branch.start

Document reads as: "H ey!"
```

Notice how items from **different clients are interleaved** in the linked list.
The linked list represents the **logical document order** — the text you
actually see. The BlockStore just stores blocks grouped by who created them.

### How they connect

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│ Root types  │     │ BlockStore                                       │
│             │     │                                                  │
│ "text" ─→ BRANCH  │  Client A ──→ [A:0][A:1][A:2]                  │
│         │  │     │  Client B ──→ [B:0][B:1]                         │
│         │  │     │  Client C ──→ [C:0][C:1][C:2]                   │
│    start│  │     │           ↑                                      │
│         ↓  │     │           │                                      │
│         ┼──┼─────┼───────────┘                                      │
│             │     │  Branch.start points to some item in the         │
│             │     │  BlockStore — the head of the CRDT linked list   │
└─────────────┘     └──────────────────────────────────────────────────┘
```

The Branch's `start` field holds a pointer to the **first Item in the CRDT
sequence**. That Item has a `right` pointer to the next, which has `right` to
the next, and so on — forming the doubly-linked list that gives you the
document's text in reading order.

```rust
pub struct Branch {
    /// Pointer to the first block of the sequence (the list head):
    pub(crate) start: Option<ItemPtr>,

    /// For Map types — stores last-writer-wins entries by key:
    pub(crate) map: HashMap<Arc<str>, ItemPtr>,

    pub(crate) name: Option<Arc<str>>,
    pub(crate) type_ref: TypeRef,
    ...
}
```

### Two different traversal patterns

| Want to... | Use... | How |
|---|---|---|
| Read the document text in order | CRDT linked list | `branch.start` → follow `item.right` → ... |
| Look up a specific block by ID | BlockStore | `store.blocks.get_item(&ID{client, clock})` |
| Apply an incoming update | Both | Decode blocks → `repair()` uses BlockStore to resolve origin IDs → `integrate()` splices into the linked list |
| Encode the document | BlockStore | Iterate all clients, write each client's blocks in clock order |

The BlockStore is the **source of truth** for what blocks exist. The linked
list is the **derived structure** that gives you the document order. Both are
kept in sync: when a new block is integrated, it's added to both the linked
list (via `integrate()`) and the BlockStore (via `push_block()`).

### The sync protocol handshake (how two replicas catch up)

When a client first connects, the Yrs sync protocol runs a three-step
handshake:

```
Client                              Server
  │                                    │
  │──── SyncStep1(my_state_vector) ──→│  "Here's what I have"
  │                                    │
  │←── SyncStep2(missing_updates) ────│  "Here's what you're missing"
  │←── SyncStep1(server_state_vec) ───│  "What do YOU have that I don't?"
  │                                    │
  │──── SyncStep2(missing_updates) ──→│  "Here's what you're missing"
  │                                    │
  │       (fully synced now)           │
  │                                    │
  │←── live updates (broadcast) ──────│  (ongoing)
  │──── live updates ─────────────────→│  (ongoing)
```

The StateVector is just `HashMap<ClientID, Clock>` — it says "I have all
blocks from Client A up to clock 7, Client B up to clock 3, etc." The other
side computes the diff and sends only the missing blocks.

```rust
// From protocol.rs — the start of a sync session:
fn start<E>(&self, awareness: &Awareness, encoder: &mut E) -> Result<(), Error> {
    let sv = awareness.doc().transact().state_vector();
    let update = awareness.update()?;
    Message::Sync(SyncMessage::SyncStep1(sv)).encode(encoder);
    Message::Awareness(update).encode(encoder);
    Ok(())
}
```

---

## Putting It All Together: Complete Lifecycle of a Single Keystroke

Let's trace what happens when Client C (client_id=3) presses "Z" between
two characters that belong to Client A and Client B:

```
Current document on Client C's local Doc:

Branch "text":
  start → [H(A:0)] ←→ [e(A:1)] ←→ [!(B:0)] ←→ [y(B:1)]

BlockStore:
  Client A → [A:0, A:1]
  Client B → [B:0, B:1]
  Client C → []  (empty — C hasn't typed anything yet)

State vector: { A: 2, B: 2, C: 0 }
```

**Step 1: Local insertion**

C's cursor is between `e(A:1)` and `!(B:0)`. C types "Z".

```rust
// text.insert(&mut txn, 2, "Z") eventually calls create_item:

origin       = pos.left.last_id()  = A:1   ← Client A's block!
right_origin = pos.right.id()      = B:0   ← Client B's block!
id           = (3, 0)                      ← Client C's own ID

Item_Z = Item {
    id:           (3, 0),
    origin:       Some((1, 1)),   // A:1 — from a DIFFERENT client
    right_origin: Some((2, 0)),   // B:0 — from ANOTHER different client
    left:         →Item_e,
    right:        →Item_!,
    content:      "Z",
}
```

**Step 2: Integration into local doc**

```rust
block_ptr.integrate(self, 0);        // splice into linked list
self.store_mut().blocks.push_block(block);  // add to BlockStore
```

After integration:

```
Linked list: [H] ←→ [e] ←→ [Z] ←→ [!] ←→ [y]
                              ↑ new

BlockStore:
  Client A → [A:0, A:1]
  Client B → [B:0, B:1]
  Client C → [C:0]          ← NEW

State vector: { A: 2, B: 2, C: 1 }
```

The text reads "HeZ!y" immediately. No network needed.

**Step 3: Transaction commit → update event fires**

The update bytes contain:

```
[client=3, clock=0, len=1, origin=(1,1), right_origin=(2,0), content="Z"]
```

**Step 4: Sent to server via WebSocket**

**Step 5: Server applies the update**

Server's Doc runs `apply_update()`:
- Decodes Item_Z
- `repair()`: looks up origin `(1,1)` in server's BlockStore → finds A:1 → sets
  `left = →Item_e`. Looks up right_origin `(2,0)` → finds B:0 → sets
  `right = →Item_!`.
- `integrate()`: checks if `left.right == right` → yes → no conflict → splice in.

Server's linked list is now: `[H] ←→ [e] ←→ [Z] ←→ [!] ←→ [y]`

**Step 6: Server broadcasts to ALL connected clients**

Including Client C (the sender).

**Step 7a: Client A and B receive the update**

They run `apply_update()` → `repair()` + `integrate()`. `Z` is new to them,
so it gets integrated. Their docs now show "HeZ!y".

**Step 7b: Client C receives its own update back**

Client C runs `apply_update()`:

```
Incoming: client=3, clock=0
My state vector: { ..., C: 1 }   ← clock 0 already integrated!

Result: SKIP. Already have this block. No-op.
```

**Done.** All three clients now have identical documents.
