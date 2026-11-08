# Reliable UDP Transport (POLI-TCP)

A user-space reliable byte-stream protocol built on UDP. The stack provides TCP-like semantics (connection setup, in-order delivery, retransmission, flow control) while remaining a custom protocol with packed headers, 16-bit sequence numbers, and a sliding window sized from the bandwidth-delay product.

The application layer is a file transfer: the client (`client.cpp`) pushes a 4-byte length prefix followed by file bytes; the server (`server.cpp`) writes the stream to `file.out` (single client) or `fileN.out` (multiple clients). All reliability lives in `lib/`, not in the applications.

---

## Layout

| Path | Role |
| --- | --- |
| `lib/include/protocol.h` | Wire format: packed data/control headers, types, listen port |
| `lib/include/lib.h` | Connection control block and public API |
| `lib/include/utils.h` | `TIMEOUT_SEND(delay)` and debug print |
| `lib/libcommon.cpp` | `poll` + `timerfd` multiplex (`recv_message_or_timeout`) |
| `lib/libsend.cpp` | Client: handshake, windowed send, ACK processing, retransmit |
| `lib/librecv.cpp` | Server: handshake, receive window, out-of-order buffer, ACKs |
| `client.cpp` / `server.cpp` | File-transfer applications |
| `Makefile` | Builds `client` (send + common) and `server` (recv + common) |
| `checker/` | Mininet topology and test harness |

`g++` compiles with `-Wall -Werror`. Sender and receiver objects are linked separately so each binary only contains one side of the protocol.

---

## Wire format

Every segment starts with `protocol_id = 42` (`POLI_PROTOCOL_ID`). Unknown IDs are ignored.

**Data segment** (`poli_tcp_data_hdr`, packed):

| Field | Size | Meaning |
| --- | --- | --- |
| `protocol_id` | 1 B | Must be 42 |
| `conn_id` | 1 B | Connection identifier assigned by the server |
| `type` | 1 B | `POLI_DATA` (4) |
| `seq_num` | 2 B | Sequence number, network byte order |
| `len` | 2 B | Payload length in bytes, network byte order |

Payload follows immediately. Maximum payload is `MAX_DATA_SIZE` (512). Maximum on-wire size is `MAX_SEGMENT_SIZE` = 512 + `sizeof(poli_tcp_data_hdr)`.

**Control segment** (`poli_tcp_ctrl_hdr`, packed):

| Field | Size | Meaning |
| --- | --- | --- |
| `protocol_id` | 1 B | Must be 42 |
| `conn_id` | 1 B | Connection id (0 on SYN, assigned after SYN-ACK) |
| `type` | 1 B | `POLI_SYN` (1), `POLI_SYN_ACK` (2), `POLI_ACK` (3) |
| `ack_num` | 2 B | Cumulative next-expected sequence number |
| `recv_window` | 2 B | Advertised free receive-buffer space in bytes |

SYN-ACK carries an extra `uint16_t` after the header: the server data port (already in network byte order from `getsockname`). Handshake packets use `sendto`/`recvfrom` on the listen socket; data and ACKs use a per-connection UDP socket.

Sequence and ACK numbers are `uint16_t` and wrap at 65536. Comparisons use modular arithmetic: `a < b` iff `(uint16_t)(b - a) < 32768` and `a != b` (`seq_less` on the sender, `seq_greater` on the receiver). This keeps sliding-window logic correct after wraparound.

---

## Connection model

A `struct connection` is the protocol control block. Shared fields: UDP `sockfd`, `conn_id`, peer `sockaddr_in`, `pthread_mutex_t con_lock`.

**Sender-only state**

- `next_send_seq_num` / `send_base`: next seq to assign vs. oldest unacked seq
- `max_window_seq`: cap on inflight **packets**
- `empty_size_recv_window`: last advertised receive window (bytes)
- `app_send_buf`: application bytes not yet packetized (`send_data` appends here)
- `inflight`: `map<seq, full on-wire packet>` for retransmission
- `inflight_count`: number of unacked packets

**Receiver-only state**

- `next_recv_seq_num`: next in-order seq expected
- `recv_buf_cap` / `recv_buf_used`: receive buffer budget (payload + out-of-order)
- `app_recv_buf`: in-order bytes ready for `recv_data`
- `out_of_order`: `map<seq, payload>` for gaps
- `pthread_cond_t con_cond`: wakes a blocked `recv_data`

Up to `MAX_CONNECTIONS` (32) concurrent connections.

`init_sender(speed, delay)` stores Mbps and millisecond one-way delay, then starts `sender_handler`. `init_receiver(recv_buffer_bytes)` binds UDP `POLI_LISTEN_PORT` (8032) on `INADDR_ANY` and starts `receiver_handler`. Both handlers run for the lifetime of the process.

Application I/O (`send_data` / `recv_data`) and the handler threads share the PCB under `con_lock`. The receiver uses `pthread_cond_wait` when `app_recv_buf` is empty.

---

## Three-way handshake

Listen port is always **8032**. Data never stays on that port: the server allocates an ephemeral UDP port per connection and tells the client in SYN-ACK.

```
Client (setup_connection)                 Server (wait4connect)
        SYN, conn_id=0  ----------------->  listen socket :8032
        (SO_RCVTIMEO 100 ms; resend SYN on timeout)
                         <----------------  SYN-ACK + data_port
                                            conn_id assigned, recv_window advertised
        switch dest port to data_port
        ACK ------------>                   data socket (timeout 100 ms; resend SYN-ACK)
                                            also accepts first DATA as handshake complete
```

**Client.** Builds a SYN on a new UDP socket, destination `ip:8032`. Waits until a SYN-ACK of at least `sizeof(ctrl_hdr) + sizeof(uint16_t)` arrives. Extracts `conn_id`, `recv_window`, and the data port, then sends ACK to that port. The handshake socket timeout is then cleared (`tv_sec = tv_usec = 0`). The same socket is registered in `data_fds` / `timer_fds` for the data phase.

**Server.** `wait4connect` loops on `listen_sockfd` until a valid SYN arrives. It allocates `conn_id = next_conn_id++`, creates a new UDP socket, `bind(..., port 0)` so the kernel picks a port, and reads it with `getsockname`. SYN-ACK is sent **from the listen socket** to the client address (so the client’s outstanding `recvfrom` on the handshake socket sees the reply). The server then waits on the **data** socket for ACK or DATA (the client may already be sending). SYN-ACK is retransmitted every 100 ms until that happens.

After both sides succeed, `poll_conn_map[fdmax] = conn_id` so later `poll` events map back to the PCB, not the poll index.

---

## Sender: segmentation, window, and reliability

### Window size (`compute_window`)

Bandwidth-delay product, in bytes:

```
BDP = (speed_Mbps * 1e6 / 8) * (delay_ms * 2 / 1000)
```

The `* 2` treats `delay` as one-way and uses RTT. BDP is floored at one MSS (512). Packet window:

```
max_window_seq = BDP / MAX_DATA_SIZE + 4
```

then clamped to `[32, 512]`. The application send buffer is 512 KiB. `empty_size_recv_window` starts at BDP and is overwritten by every ACK’s `recv_window`.

`init_sender(8, 2)` in the sample client therefore assumes 8 Mb/s and 2 ms one-way delay.

### Packetization (`try_send`)

Called from `send_data` and after ACK processing. While `inflight_count < max_window_seq` and `app_send_buf` is non-empty:

1. Take up to 512 bytes from the head of `app_send_buf`.
2. Stop if `bytes_in_flight + chunk > empty_size_recv_window` (byte-level flow control).
3. Build a DATA header (`seq_num = htons(next_send_seq_num)`, `len = htons(chunk)`), `sendto` the peer, store the full datagram in `inflight[seq]`, increment seq and inflight count, erase the chunk from the app buffer.

`send_data` copies the caller’s buffer into `app_send_buf` only if `size + len <= send_buf_cap`; otherwise it returns `-1` without blocking. The sample client retries after `usleep(500000)` when that happens.

### Cumulative ACKs and duplicate ACK fast retransmit

`sender_handler` treats a datagram of at least `sizeof(poli_tcp_ctrl_hdr)` with `type == POLI_ACK` as an ACK:

- `empty_size_recv_window = ntohs(recv_window)`.
- If `ack_num == last_acks_arrived[conn_id]`, increment a per-connection duplicate counter. On the **third** duplicate ACK, retransmit **only the first** inflight packet (`inflight.begin()`, i.e. the lowest seq). This is fast retransmit of the likely lost head of the window, not a full Go-Back-N.
- If `ack_num` is modularly greater than `last_acks_arrived`, it is a new cumulative ACK: reset the dup counter, drop every inflight seq `s` with `s < ack_num`, set `send_base = ack_num`, then `try_send` again.

SYN-ACK received on the data path (late handshake) is answered with another ACK.

### Timeout retransmission

Per connection, `timerfd_create(CLOCK_REALTIME)` is armed with:

```
timer_ms = TIMEOUT_SEND(delay) * 2
TIMEOUT_SEND(delay) = (delay == 0 ? 100 : delay / 2 + 30)
```

The timer is periodic (`it_interval = it_value`). When `recv_message_or_timeout` returns `-1`, `retransmit` sends **every** packet still in `inflight` (Go-Back-N style recovery). Combined with selective fast retransmit on 3 dup ACKs, loss is recovered either by timeout of the whole window or by a single head retransmission.

---

## Receiver: reordering, delivery, and ACKs

`receiver_handler` only processes `POLI_DATA` with a valid `len` and datagram size.

- **`seq == next_recv_seq_num`:** if `recv_buf_used + data_len <= recv_buf_cap`, append payload to `app_recv_buf`, bump `recv_buf_used` and `next_recv_seq_num`, then `deliver_in_order_packets`: while `out_of_order` contains `next_recv_seq_num`, append that payload and advance. `pthread_cond_broadcast` wakes `recv_data`.
- **`seq > next_recv_seq_num`:** store payload in `out_of_order[seq]` if the seq is new and the buffer still has room. `recv_buf_used` includes out-of-order payload so the advertised window shrinks for holes as well.
- **`seq < next_recv_seq_num`:** duplicate; ignored except that an ACK is still sent.

Every accepted DATA (in-order, out-of-order, or duplicate that passed the header checks) triggers `send_ack`: `ack_num = next_recv_seq_num`, `recv_window = min(max(cap - used, 0), 65535)`. That is a **cumulative ACK**: the sender may release everything below `next_recv_seq_num`. Gaps do not advance `ack_num`, so the sender sees duplicate ACKs until the missing seq arrives.

On timer expiry the receiver **re-ACKs** the current `next_recv_seq_num`. That recovers lost ACKs without the sender waiting for a full timeout of data.

`recv_data` blocks on `con_cond` until `app_recv_buf` is non-empty, copies `min(len, buf.size())` bytes out, and decreases `recv_buf_used` by that amount so later ACKs advertise more window.

The sample server’s receive buffer is 9 KiB (`init_receiver(9 * 1024)`).

---

## Event loop (`recv_message_or_timeout`)

Shared by both handlers in `libcommon.cpp`:

1. `poll(data_fds, fdmax, 10)` — up to 10 ms for UDP readability.
2. `poll(timer_fds, fdmax, 0)` — non-blocking check for expired `timerfd`s.
3. First `POLLIN` on a data fd: `recvfrom(..., MSG_WAITALL)`, set `*conn_id = poll_conn_map[i]`, return byte count.
4. First `POLLIN` on a timer fd: read 8 bytes to clear the timer, set `conn_id`, return `-1`.
5. Otherwise return `-14`. Handlers loop on `-14` so they idle without spinning on CPU-heavy work beyond the 10 ms poll.

`conn_id` is the protocol id, not the poll slot. That is required because server connection ids are assigned independently of the order sockets were registered.

---

## Application protocol

Both applications treat the reliable stream as a pipe.

1. Client `send_data`s a `uint32_t` file size (host endianness, as a raw 4-byte prefix).
2. Client reads the file in 1024-byte chunks and `send_data`s until the file is consumed. Partial acceptance (`-1`) is retried.
3. Server `recv_data`s until 4 bytes of size are collected, then reads until that many payload bytes are written.

Single-client server: one `wait4connect`, output `file.out`, prints elapsed time with `clock_gettime(CLOCK_MONOTONIC_RAW)`.

Multi-client server (`./server N`): accepts N connections, writes `file0.out` … `file{N-1}.out`, sleeps 2 seconds, then round-robins `recv_data` across connections until all sizes reach zero.

The client never closes the connection; it busy-waits so the sender thread can drain `inflight`. There is no FIN/RST in the protocol.

Default client peer: `172.16.0.100:8032` (Mininet server address in the checker topology).

---

## Build and run

```bash
make
./server                 # one client, writes file.out
./server 3               # three clients, writes file0.out …
./client path/to/file    # optional second arg: seconds to sleep after connect
```

`make clean` removes objects and binaries.

Automated tests:

```bash
./checker/checker.sh
```

That rebuilds, then runs `checker/topo.py` under Mininet (`sudo`). `create_archive.sh` packages `lib/`, sources, `Makefile`, and a `README` file for submission.

---

## Design notes

- **UDP only.** Reliability, ordering, and flow control are entirely in user space.
- **Two header types.** Handshake and ACKs stay small; data carries `seq_num`/`len` instead of `ack_num`/`recv_window`. ACKs are standalone control datagrams, not piggybacked on DATA (the transfer is client-to-server).
- **Hybrid recovery.** Timeout retransmits the whole inflight map; three duplicate ACKs retransmit only the window head.
- **Flow control is byte-oriented; congestion window is packet-oriented.** Sending stops if either `inflight_count` hits `max_window_seq` or in-flight payload would exceed the advertised receive window.
- **Out-of-order is payload-only.** The receiver does not store full datagrams in the hole buffer, only application bytes keyed by seq.
- **No selective ACK bitmap.** Recovery of a hole depends on duplicate ACKs plus timeout, and on the receiver keeping later segments until the gap fills.
- **Endianness.** Multi-byte header fields use `htons`/`ntohs`. The 4-byte file-size prefix is not converted; client and server are assumed to share endianness (typical for the lab).
