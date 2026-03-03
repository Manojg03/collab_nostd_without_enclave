# Backpressure & Broadcast Architecture Explained

This document explains the system architecture, the channel topology, and why "Bounded Channels + Backpressure" are critical for stability.

## 1. System Architecture & Channel Topology

The following diagram illustrates the exact path a message takes from one client to all other clients in a Room.

**Key Components:**
1.  **TcpReadStream**: Reads raw bytes from the socket.
2.  **WsReader**: Parses raw bytes into WebSocket frames (and updates).
3.  **Client Read Channel**: Carries decoded updates to the BroadcastGroup.
4.  **BroadcastGroup**: The central hub that maintains the document state and fans out updates.
5.  **Client Write Channel**: Carries updates from the hub to the client's writer.
6.  **WsWriter**: Encodes frames and writes to `TcpSink`.

```mermaid
graph TD
    subgraph "Per Client Connection (Task A)"
    Socket[(TCP Socket)]
    Socket -->|Raw Bytes| Reader[TcpReadStream]
    Reader -->|Parse WS| WsRead[WsReader Task]
    WsRead -->|1. Read Channel| ReadChan{Channel}
    end

    subgraph "Room (Shared State)"
    ReadChan -->|Consume| BG[BroadcastGroup Hub]
    BG -->|2. Fan Out (Clone)| WriteChan1{Client A Write Q}
    BG -->|Fan Out| WriteChan2{Client B Write Q}
    BG -->|Fan Out| WriteChanN{Client... Write Q}
    end

    subgraph "Per Client Connection (Task B)"
    WriteChan1 -->|Consume| WsWrite[WsWriter Task]
    WsWrite -->|Encode WS| Sink[TcpSink]
    Sink -->|Raw Bytes| Socket
    end

    style ReadChan fill:#f9f,stroke:#333,stroke-width:2px
    style WriteChan1 fill:#f9f,stroke:#333,stroke-width:2px
```

### The Channels Involved
1.  **Read Channel**: `mpsc::channel<Vec<u8>>`
    -   Connecting `WsReader` -> `BroadcastGroup`.
    -   **Backpressure**: If `BroadcastGroup` is busy (locked or processing slow), this queue fills. If bounded, `WsReader` stops reading TCP.
2.  **Write Channel**: `mpsc::channel<Outbound>`
    -   Connecting `BroadcastGroup` -> `WsWriter`.
    -   **Backpressure**: If `TcpSink` is slow (Network congestion), this queue fills. If bounded, `BroadcastGroup` waits/stops processing.

---

## 2. The Numerical Scenario (The "Why")

### **The Input**
- **Concurrent Users**: `400`
- **Typing Speed**: `100 WPM`
- **Duration**: `80 seconds`

### **The Rate Calculation**
1.  **Keystrokes per Second**:
    - Average word = 6 keystrokes.
    - 100 WPM = 600 keystrokes/min = **10 updates / second / user**.
2.  **Total Incoming Traffic**:
    - 400 users × 10 updates/sec = **4,000 incoming messages / second**.

### **The Fan-Out (The Multiplier)**
Broadcast systems are **N × (N-1)**.
- **Fan-Out Factor**: 399 (send to everyone else).
- **Total Generated Message Rate**:
    - 4,000 input/sec × 399 outputs = **1,596,000 internal messages / second**.

### **Memory Impact (Unbounded)**
Protocol overhead + Payload ≈ **100 bytes** per message.

*   **Throughput Generated**: 1.6 Million msgs/sec × 100 bytes ≈ **160 MB / second**.
*   **Total RAM Usage (80s test)**:
    - 160 MB/sec × 80 seconds = **12.8 GB** (Theoretical Max).
    - **Observed**: 1GB - 2GB spikes (Partial network drain).

| Metric | Without Backpressure (Unbounded) | With Backpressure (Bounded: 32) |
| :--- | :--- | :--- |
| **Queue Capacity** | Infinite (Limited by RAM) | **32 messages** |
| **Max Memory (400 users)** | **~12.8 GB** (Linear Growth) | **~1.2 MB** (Constant) |
| **Latency** | Increases infinitely | Limited / Stable |
| **Result** | **Server Crash (OOM)** | **Senders Slow Down** |

---

## 3. How The Fix Works

We replace infinite queues with **Bounded Channels (Capacity 32)** at both critical points shown in the Architecture diagram.

### A. The Write Path Valve
1.  **Network Slows**: `TcpSink` cannot write fast enough.
2.  **Write Channel Fills**: The `Client Write Queue` hits 32 items.
3.  **BroadcastGroup Waits**: The hub tries to push message #33, sees 'Pending', and sleeps.
4.  **Fan-Out Pauses**: The entire broadcast loop pauses for that room.

### B. The Read Path Valve
1.  **Hub Paused**: Because `BroadcastGroup` is sleeping (waiting on write), it stops reading from `Client Read Channels`.
2.  **Read Channel Fills**: The input queue hits 32 items.
3.  **WsReader Waits**: The reader task tries to push packet #33, sees 'Pending', and sleeps.
4.  **TCP Stops**: `WsReader` stops calling `socket.read()`. 
5.  **TCP Window Closes**: The Server's OS buffer fills, closing the TCP Window.
6.  **Client Stops**: The Client's OS sees window 0 and blocks the application from sending.

This chain ensures the Server **never accepts data faster than it can send it**.
