# Reliable UDP Transport Protocol

A custom, reliable transport layer implementation built on top of UDP, inspired by TCP. Developed in C++ for the Communication Networks course.

This project implements a reliable data transfer protocol that handles packet loss, out-of-order delivery, flow control, and dynamic window scaling using a three-way handshake and sliding window mechanism.

---

## Features

- **Three-Way Handshake:** Connection setup using SYN, SYN-ACK, and ACK control segments.
- **Flow Control & BDP Scaling:** Dynamic window size calculation based on Bandwidth-Delay Product (32 <= W <= 512 packets).
- **Reliable Data Transfer:**
  - Sequence numbers with circular comparisons (`seq_less`).
  - Timeout-based retransmissions.
  - **Fast Retransmit:** Triggered upon receiving 3 duplicate ACKs.
  - **Out-of-Order Handling:** Receiver buffers out-of-order segments and delivers them sequentially.
- **Multithreading & Synchronization:** Separate sender and receiver background handlers with mutexes and condition variables.

---

## Protocol Architecture & Design

### 1. Connection Establishment (Handshake)

```text
  Client (Sender)                         Server (Receiver)
        |                                         |
        | -------------- SYN -------------------> |  (Listen port: POLI_LISTEN_PORT)
        | <--- SYN-ACK (with assigned port) ----  |  (Spawns new data socket)
        | -------------- ACK -------------------> |
        |                                         |
        | ============ DATA TRANSFER ============ |
```

### 2. Sender Architecture (`LIBSEND.CPP`)

- **Connection Management:** Active connections are tracked using map structures and mapped to poll file descriptors (`data_fds`, `timer_fds`).
- **Flow & Transmission Control:**
  - `compute_window()` adjusts the sending window based on BDP, capped between 32 and 512 packets (512 KB transmission buffer).
  - Tracks in-flight bytes and sequence numbers via 16-bit circular arithmetic (`seq_less`).
- **Sender Thread (`sender_handler`):**
  - Monitors timeout events to retransmit all in-flight unacknowledged packets.
  - Detects Fast Retransmit conditions (3 duplicate ACKs) to resend the first lost segment immediately.

### 3. Receiver Architecture (`LIBRECV.CPP`)

- **Listening & Handshake (`wait4connect`):**
  - Listens on `POLI_LISTEN_PORT`.
  - Upon receiving SYN, binds a new socket to a system-assigned dynamic port and responds with SYN-ACK.
- **Receiver Thread (`receiver_handler`):**
  - Processes incoming DATA segments.
  - Places in-order packets directly into the stream buffer; out-of-order packets are cached until missing gaps are filled.
  - Sends ACKs updated with the expected sequence number and available receiver buffer capacity.

---

## Building and Running

### Prerequisites

- GCC / G++ compiler with C++11 support (or higher)
- POSIX-compliant environment (Linux / WSL)
- `pthread` library support

### Compilation

Build the sender and receiver executables using `make`:

```bash
make
```

### Usage Example

1. **Run Receiver (Server):**
   ```bash
   ./receiver
   ```

2. **Run Sender (Client):**
   ```bash
   ./sender <server_ip> <speed_mbps> <delay_ms>
   ```
