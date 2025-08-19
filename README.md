# Paladin RPC Client

JSON-RPC client for sending Solana transactions to Paladin validators.

## Overview

Paladin RPC Client provides a high-performance JSON-RPC server and client implementation for interacting with Paladin validators on the Solana network. It lets you submit transactions, check server health, and interact with validator scheduling.

## Getting Started

### Prerequisites

- Rust toolchain
- Solana CLI tools
- Valid Paladin identity file (see `identity_paths` below)

### Configuration

You must provide a `config.json` file as an argument when running the server. Example:

```json
{
  "leader_look_ahead": 10,
  "rpc_bind_address": "0.0.0.0:5009",
  "max_slot_offset": 1,
  "identity_paths": [
    "/home/sol/paladin.json"
  ],
  "rpc_url": "http://astralane-rpc:8899",
  "ws_urls": [
    "ws://astralane-rpc:8900"
  ],
  "grpc_urls": [
   "http://astralane-rpc:10000"
  ],
  "quic_bind_address": "0.0.0.0:0",
  "quic_max_reconnection_attempts": 5,
  "quic_txn_max_batch_size": 100,
  "quic_worker_queue_size": 512,
  "auction_interval_ms": 5
}
```

### Usage

Start the RPC server with:

```bash
cargo run --release -- config.json
```

### Example: Sending a Transaction

The RPC client exposes two main methods:

- `getHealth` — returns server health status.
- `sendTransaction(txn, revert_protect)` — submits a transaction to a Paladin slot, with revert protection if needed.

Sample Rust test client (see `tests/client.rs`):

```rust
let pal_rpc = HttpClient::builder()
    .build("http://localhost:8899")
    .unwrap();
let signature = pal_rpc.send_transaction(encoded_tx, true).await?;
```


## Contributing

Open issues or pull requests for feature requests, bug reports, or questions.

## Maintainers

- [Astralane Organization](https://github.com/Astralane)

---

For more information, see the source code and `tests/client.rs` for usage examples.
