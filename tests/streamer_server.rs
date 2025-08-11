use itertools::Itertools;
use paladin_rpc_server::constants::POOL_KEY;
use paladin_rpc_server::utils::{get_stakes, try_deserialize_lockup_pool};
use quinn::{VarInt, VarIntBoundsExceeded};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_quic_definitions::{
    QUIC_MAX_STAKED_CONCURRENT_STREAMS, QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO,
    QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS, QUIC_MIN_STAKED_CONCURRENT_STREAMS,
    QUIC_MIN_STAKED_RECEIVE_WINDOW_RATIO, QUIC_TOTAL_STAKED_CONCURRENT_STREAMS,
    QUIC_UNSTAKED_RECEIVE_WINDOW_RATIO,
};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::signer::EncodableKey;
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use solana_streamer::packet::PACKET_DATA_SIZE;
use solana_streamer::quic::{QuicServerParams, QuicVariant};
use solana_streamer::streamer::StakedNodes;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

mod common;

use crate::common::init_tracing;
const MAX_STAKED_CONNECTIONS: usize = 256;

pub fn compute_max_allowed_uni_streams(peer_type: ConnectionPeerType, total_stake: u64) -> usize {
    match peer_type {
        ConnectionPeerType::Staked(peer_stake)
        | ConnectionPeerType::P3(peer_stake)
        | ConnectionPeerType::Mev(peer_stake) => {
            // No checked math for f64 type. So let's explicitly check for 0 here
            if total_stake == 0 || peer_stake > total_stake {
                warn!(
                    "Invalid stake values: peer_stake: {:?}, total_stake: {:?}",
                    peer_stake, total_stake,
                );

                QUIC_MIN_STAKED_CONCURRENT_STREAMS
            } else {
                let delta = (QUIC_TOTAL_STAKED_CONCURRENT_STREAMS
                    - QUIC_MIN_STAKED_CONCURRENT_STREAMS) as f64;

                (((peer_stake as f64 / total_stake as f64) * delta) as usize
                    + QUIC_MIN_STAKED_CONCURRENT_STREAMS)
                    .clamp(
                        QUIC_MIN_STAKED_CONCURRENT_STREAMS,
                        QUIC_MAX_STAKED_CONCURRENT_STREAMS,
                    )
            }
        }
        ConnectionPeerType::Unstaked => QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS,
    }
}

fn calculate_stake_stats(
    stakes: &Arc<HashMap<Pubkey, u64>>,
    overrides: &HashMap<Pubkey, u64>,
) -> (u64, u64, u64) {
    let values = stakes
        .iter()
        .filter(|(pubkey, _)| !overrides.contains_key(pubkey))
        .map(|(_, &stake)| stake)
        .chain(overrides.values().copied())
        .filter(|&stake| stake > 0);
    let total_stake = values.clone().sum();
    let (min_stake, max_stake) = values.minmax().into_option().unwrap_or_default();
    (total_stake, min_stake, max_stake)
}

fn compute_recieve_window(
    max_stake: u64,
    min_stake: u64,
    peer_type: ConnectionPeerType,
) -> Result<VarInt, VarIntBoundsExceeded> {
    match peer_type {
        ConnectionPeerType::Unstaked => {
            VarInt::from_u64(PACKET_DATA_SIZE as u64 * QUIC_UNSTAKED_RECEIVE_WINDOW_RATIO)
        }
        ConnectionPeerType::Staked(peer_stake)
        | ConnectionPeerType::P3(peer_stake)
        | ConnectionPeerType::Mev(peer_stake) => {
            let ratio =
                compute_receive_window_ratio_for_staked_node(max_stake, min_stake, peer_stake);
            VarInt::from_u64(PACKET_DATA_SIZE as u64 * ratio)
        }
    }
}

/// Calculate the ratio for per connection receive window from a staked peer
fn compute_receive_window_ratio_for_staked_node(max_stake: u64, min_stake: u64, stake: u64) -> u64 {
    // Testing shows the maximum througput from a connection is achieved at receive_window =
    // PACKET_DATA_SIZE * 10. Beyond that, there is not much gain. We linearly map the
    // stake to the ratio range from QUIC_MIN_STAKED_RECEIVE_WINDOW_RATIO to
    // QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO. Where the linear algebra of finding the ratio 'r'
    // for stake 's' is,
    // r(s) = a * s + b. Given the max_stake, min_stake, max_ratio, min_ratio, we can find
    // a and b.

    if stake > max_stake {
        return QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO;
    }

    let max_ratio = QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO;
    let min_ratio = QUIC_MIN_STAKED_RECEIVE_WINDOW_RATIO;
    if max_stake > min_stake {
        let a = (max_ratio - min_ratio) as f64 / (max_stake - min_stake) as f64;
        let b = max_ratio as f64 - ((max_stake as f64) * a);
        let ratio = (a * stake as f64) + b;
        ratio.round() as u64
    } else {
        QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO
    }
}
#[tokio::test]
pub async fn run_test_p3_server() {
    init_tracing();
    info!("Starting test");
    let rpc_url = "http://rpc:8899";
    let rpc = RpcClient::new(rpc_url.to_string());
    let identity = Keypair::read_from_file("/Users/nuel/.config/solana/pal.json").unwrap();
    let pubkey = identity.pubkey();
    info!("identity pubkey: {:?}", pubkey);

    let account = rpc.get_account_data(&POOL_KEY).await.unwrap();
    let lookup_pool = try_deserialize_lockup_pool(&account).unwrap();
    let stakes = Arc::new(get_stakes(&lookup_pool));
    let (total_stake, min_stake, max_stake) = calculate_stake_stats(&stakes, &HashMap::new());
    let staked_nodes = Arc::new(RwLock::new(StakedNodes::new(
        stakes.clone(),
        HashMap::new(),
    )));
    let our_stake = stakes.get(&pubkey).unwrap();

    info!("our stake: {:?}", our_stake);
    info!(
        "total stake, min_stake, max_stake : {:?}",
        (total_stake, min_stake, max_stake)
    );
    let max_uni_streams = compute_max_allowed_uni_streams(
        ConnectionPeerType::P3(*our_stake),
        staked_nodes.read().unwrap().total_stake(),
    );
    let max_mev_uni_stream = compute_max_allowed_uni_streams(
        ConnectionPeerType::Mev(*our_stake),
        staked_nodes.read().unwrap().total_stake(),
    );
    info!("max uni streams: {:?}", max_uni_streams);
    info!("max_mev_uni_stream: {:?}", max_mev_uni_stream);
    let p3_mev_socket = "0.0.0.0:4819";
    let (mev_packet_tx, mev_packet_rx) = crossbeam_channel::unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let socket_mev = UdpSocket::bind(p3_mev_socket).unwrap();
    let ctrl_c = tokio::signal::ctrl_c();

    let streamer = solana_streamer::quic::spawn_server(
        "p3Quic-streamer",
        "p3_quic",
        socket_mev.try_clone().unwrap(),
        &identity,
        // NB: Packets are verified using the usual TPU lane.
        mev_packet_tx,
        exit.clone(),
        staked_nodes.clone(),
        QuicServerParams {
            variant: QuicVariant::Mev,
            max_staked_connections: MAX_STAKED_CONNECTIONS,
            max_unstaked_connections: 0,
            // NB: This must be 1 second for the `P3_RATE_LIMIT` const to be valid.
            stream_throttling_interval_ms: 1000,
            ..Default::default()
        },
    )
    .unwrap();
    info!("started p3 quic server on port {:?}", socket_mev);
    ctrl_c.await.unwrap();
    //doggo validator ip: 149.248.51.171
}

#[tokio::test]
pub async fn run_test_default_server() {
    init_tracing();
    info!("Starting test");
    let rpc_url = "http://rpc:8899";
    let rpc = RpcClient::new(rpc_url.to_string());
    let identity = Keypair::read_from_file("/Users/nuel/.config/solana/pal.json").unwrap();
    let pubkey = identity.pubkey();
    info!("identity pubkey: {:?}", pubkey);
    let socket = "0.0.0.0:12333";

    let (mev_packet_tx, mev_packet_rx) = crossbeam_channel::unbounded();
    let exit = Arc::new(AtomicBool::new(false));
    let vote_accounts = rpc.get_vote_accounts().await.unwrap();
    let shared_staked_nodes = Arc::new(RwLock::new(StakedNodes::new(
        Arc::new(HashMap::new()),
        HashMap::new(),
    )));
    let stake_map = Arc::new(
        vote_accounts
            .current
            .iter()
            .chain(vote_accounts.delinquent.iter())
            .filter_map(|vote_account| {
                Some((
                    Pubkey::from_str(&vote_account.node_pubkey).ok()?,
                    vote_account.activated_stake,
                ))
            })
            .collect::<HashMap<Pubkey, u64>>(),
    );
    shared_staked_nodes
        .write()
        .unwrap()
        .update_stake_map(stake_map);
    // stakes have values right now
    let (total_stake, min_stake, max_stake) =
        calculate_stake_stats(&shared_staked_nodes.read().unwrap().stakes, &HashMap::new());
    info!(
        "total stake, min_stake, max_stake : {:?}",
        (total_stake, min_stake, max_stake)
    );

    let doggo_key = pubkey!("Awes4Tr6TX8JDzEhCZY2QVNimT6iD1zWHzf1vNyGvpLM");
    let our_stake = *shared_staked_nodes
        .read()
        .unwrap()
        .stakes
        .get(&doggo_key)
        .unwrap();
    let peer_type = ConnectionPeerType::Staked(our_stake);
    let max_uni_streams = compute_max_allowed_uni_streams(
        peer_type,
        shared_staked_nodes.read().unwrap().total_stake(),
    );
    let recieve_window = compute_recieve_window(max_stake, min_stake, peer_type);
    info!("max uni streams: {:?}", max_uni_streams);
    info!("recieve_window: {:?} bytes", recieve_window);

    let socket_mev = UdpSocket::bind(socket).unwrap();
    let ctrl_c = tokio::signal::ctrl_c();

    let streamer = solana_streamer::quic::spawn_server(
        "p3Quic-streamer",
        "p3_quic",
        socket_mev.try_clone().unwrap(),
        &identity,
        // NB: Packets are verified using the usual TPU lane.
        mev_packet_tx,
        exit.clone(),
        shared_staked_nodes.clone(),
        QuicServerParams {
            variant: QuicVariant::Regular,
            max_staked_connections: QUIC_MAX_STAKED_CONCURRENT_STREAMS,
            max_unstaked_connections: QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS,
            // NB: This must be 1 second for the `P3_RATE_LIMIT` const to be valid.
            stream_throttling_interval_ms: 1000,
            ..Default::default()
        },
    )
    .unwrap();
    info!("started regular quic server on port {:?}", socket_mev);
    ctrl_c.await.unwrap();
}
