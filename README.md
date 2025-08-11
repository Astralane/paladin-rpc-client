# Paladin Suite

JSON-RPC server for accepting Paladin transactions.
A config.json must be provided as an argument while running the code

Sample config.json
```
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
