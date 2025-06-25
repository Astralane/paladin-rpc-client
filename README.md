# Paladin Transaction Auctioneer

A high-frequency auction engine for Paladin-based transaction delivery on Solana.

## Features
- Tracks validator leader schedules
- Sends txs with slot targeting
- 20–50ms auctions with tip awareness
- Stores results in PostgreSQL
- Optional tip streaming view

## Run
```sh
cargo run
```