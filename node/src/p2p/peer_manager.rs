use std::collections::HashMap;
use std::task::{Context, Poll};
use std::time::Duration;

use blockstore::Blockstore;
use futures::StreamExt;
use libp2p::swarm::NetworkInfo;
use libp2p::{
    autonat,
    connection_limits::ConnectionLimits,
    core::{
        transport::{ListenerId, PortUse},
        ConnectedPoint, Endpoint,
    },
    identify,
    identity::Keypair,
    kad::{self, RecordKey},
    multiaddr::Protocol,
    ping,
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        dummy, ConnectionDenied, ConnectionId, DialError, FromSwarm, NetworkBehaviour, SwarmEvent,
        THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId, Swarm,
};
use lumina_utils::time::Interval;
use multihash_codetable::{Code, MultihashDigest};
use smallvec::SmallVec;
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, error, instrument, trace, warn};
use void::Void;

use crate::events::{EventPublisher, NodeEvent};
use crate::p2p::connection_control;
use crate::p2p::swarm::new_swarm;
use crate::p2p::{Behaviour, BehaviourEvent, Result};
use crate::peer_tracker::{PeerTracker, PeerTrackerInfo};
use crate::store::Store;
use crate::utils::{celestia_protocol_id, MultiaddrExt};

// Minimal number of peers that we want to maintain connection to.
// If we have fewer peers than that, we will try to reconnect / discover
// more aggresively.
const MIN_CONNECTED_PEERS: u64 = 4;

#[derive(NetworkBehaviour)]
pub(crate) struct SwarmBehaviour<B>
where
    B: NetworkBehaviour + 'static,
{
    connection_control: connection_control::Behaviour,
    autonat: autonat::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    behaviour: B,
}

pub(crate) struct SwarmManager<B>
where
    B: NetworkBehaviour + 'static,
{
    swarm: Swarm<SwarmBehaviour<B>>,
    peer_tracker: PeerTracker,
    peer_tracker_info_watcher: watch::Receiver<PeerTrackerInfo>,
    event_pub: EventPublisher,
    bootnodes: HashMap<PeerId, Vec<Multiaddr>>,
    listeners: SmallVec<[ListenerId; 1]>,
    kademlia_interval: Interval,
}

pub(crate) struct SwarmContext<'a, B>
where
    B: NetworkBehaviour,
{
    pub(crate) behaviour: &'a mut B,
    pub(crate) peer_tracker: &'a PeerTracker,
}

impl<B> SwarmManager<B>
where
    B: NetworkBehaviour,
{
    pub(crate) async fn new(
        network_id: &str,
        keypair: &Keypair,
        bootnodes: &[Multiaddr],
        listen_on: &[Multiaddr],
        mut peer_tracker: PeerTracker,
        event_pub: EventPublisher,
        behaviour: B,
    ) -> Result<SwarmManager<B>> {
        let local_peer_id = PeerId::from(keypair.public());

        let connection_control = connection_control::Behaviour::new();
        let autonat = autonat::Behaviour::new(local_peer_id, autonat::Config::default());
        let ping = ping::Behaviour::new(ping::Config::default());
        let kademlia = init_kademlia(network_id, keypair, bootnodes, listen_on)?;

        let agent_version = format!("lumina/{}/{}", network_id, env!("CARGO_PKG_VERSION"));
        let identify_config = identify::Config::new(String::new(), keypair.public())
            .with_agent_version(agent_version);
        let identify = identify::Behaviour::new(identify_config);

        let behaviour = SwarmBehaviour {
            connection_control,
            autonat,
            ping,
            identify,
            kademlia,
            behaviour,
        };

        let mut swarm = new_swarm(keypair.to_owned(), behaviour).await?;
        let mut listeners = SmallVec::new();

        for addr in listen_on {
            match swarm.listen_on(addr.clone()) {
                Ok(id) => listeners.push(id),
                Err(e) => error!("Failed to listen on {addr}: {e}"),
            }
        }

        let mut bootnodes_map = HashMap::<_, Vec<_>>::new();

        for addr in bootnodes {
            let peer_id = addr.peer_id().expect("multiaddr already validated");
            bootnodes_map
                .entry(peer_id)
                .or_default()
                .push(addr.to_owned());
        }

        for (peer_id, addrs) in bootnodes_map.iter_mut() {
            addrs.sort();
            addrs.dedup();
            addrs.shrink_to_fit();

            // Bootstrap peers are always trusted
            peer_tracker.set_trusted(*peer_id, true);
        }

        let peer_tracker_info_watcher = peer_tracker.info_watcher();
        let kademlia_interval = Interval::new(Duration::from_secs(30)).await;

        let mut manager = SwarmManager {
            swarm,
            peer_tracker,
            peer_tracker_info_watcher,
            event_pub,
            bootnodes: bootnodes_map,
            listeners,
            kademlia_interval,
        };

        manager.bootstrap();

        Ok(manager)
    }

    pub(crate) fn context<'a>(&'a mut self) -> SwarmContext<'a, B> {
        SwarmContext {
            behaviour: &mut self.swarm.behaviour_mut().behaviour,
            peer_tracker: &self.peer_tracker,
        }
    }

    fn bootstrap(&mut self) {
        self.event_pub.send(NodeEvent::ConnectingToBootnodes);

        for (peer_id, addrs) in &self.bootnodes {
            let dial_opts = DialOpts::peer_id(*peer_id)
                .addresses(addrs.clone())
                // Tell Swarm not to dial if peer is already connected or there
                // is an ongoing dialing.
                .condition(PeerCondition::DisconnectedAndNotDialing)
                .build();

            if let Err(e) = self.swarm.dial(dial_opts) {
                if !matches!(e, DialError::DialPeerConditionFalse(_)) {
                    warn!("Failed to dial on {addrs:?}: {e}");
                }
            }
        }

        // trigger kademlia bootstrap
        if self.swarm.behaviour_mut().kademlia.bootstrap().is_err() {
            warn!("Can't run kademlia bootstrap, no known peers");
        }
    }

    pub(crate) fn network_info(&self) -> NetworkInfo {
        self.swarm.network_info()
    }

    pub(crate) fn local_peer_id(&self) -> PeerId {
        self.swarm.local_peer_id().to_owned()
    }

    pub(crate) fn listeners(&self) -> Vec<Multiaddr> {
        let local_peer_id = self.local_peer_id();

        self.swarm
            .listeners()
            .cloned()
            .map(|mut ma| {
                if !ma.protocol_stack().any(|protocol| protocol == "p2p") {
                    ma.push(Protocol::P2p(local_peer_id))
                }
                ma
            })
            .collect()
    }

    pub(crate) fn set_peer_trust(&mut self, peer_id: PeerId, is_trusted: bool) {
        if *self.swarm.local_peer_id() != peer_id {
            self.peer_tracker.set_trusted(peer_id, is_trusted);
        }
    }

    pub(crate) async fn poll(&mut self) -> Result<B::ToSwarm> {
        /*
                     *

        let key = peer_manager::topic_to_dht_key("/full/v0.1.0");
        let key = peer_manager::topic_to_dht_key("/archival/v0.1.0");
                    let id = self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .get_closest_peers(key.to_vec());


                    let id = self.swarm.behaviour_mut().kademlia.get_providers(key.clone());
                     *
                     */

        loop {
            select! {
                // We use info watcher here in order to act only once when the connected
                // peers are zero.
                // TODO
                _ = self.peer_tracker_info_watcher.changed() => {
                    if self.peer_tracker_info_watcher.borrow().num_connected_peers == 0 {
                        warn!("All peers disconnected");
                        self.bootstrap();
                    }
                }
                _ = self.kademlia_interval.tick() => {
                    if self.peer_tracker.info().num_connected_peers < MIN_CONNECTED_PEERS
                    {
                        self.bootstrap();
                    }
                }
                ev = self.swarm.select_next_some() => {
                    if let Some(ev) = self.on_swarm_event(ev).await {
                        return Ok(ev);
                    }
                }
            }
        }
    }

    async fn on_swarm_event(
        &mut self,
        ev: SwarmEvent<SwarmBehaviourEvent<B>>,
    ) -> Option<B::ToSwarm> {
        match ev {
            SwarmEvent::Behaviour(ev) => match ev {
                SwarmBehaviourEvent::Identify(ev) => self.on_identify_event(ev),
                SwarmBehaviourEvent::Kademlia(ev) => self.on_kademlia_event(ev),
                SwarmBehaviourEvent::Ping(ev) => self.on_ping_event(ev),
                SwarmBehaviourEvent::ConnectionControl(_) | SwarmBehaviourEvent::Autonat(_) => {}
                SwarmBehaviourEvent::Behaviour(ev) => return Some(ev),
            },
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            } => {
                self.on_peer_connected(peer_id, connection_id, endpoint);
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                ..
            } => {
                self.on_peer_disconnected(peer_id, connection_id);
            }
            _ => {}
        }

        None
    }

    #[instrument(skip_all, fields(peer_id = %peer_id))]
    pub(crate) fn peer_maybe_discovered(&mut self, peer_id: PeerId) {
        if !self.peer_tracker.set_maybe_discovered(peer_id) {
            return;
        }

        debug!("Peer discovered");
    }

    #[instrument(skip_all, fields(peer_id = %peer_id))]
    fn on_peer_connected(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        endpoint: ConnectedPoint,
    ) {
        debug!("Peer connected");

        // Inform PeerTracker about the dialed address.
        //
        // We do this because Kademlia send commands to Swarm
        // for dialing a peer and we may not have that address
        // in PeerTracker.
        let dialed_addr = match endpoint {
            ConnectedPoint::Dialer {
                address,
                role_override: Endpoint::Dialer,
                ..
            } => Some(address),
            _ => None,
        };

        self.peer_tracker
            .set_connected(peer_id, connection_id, dialed_addr);
    }

    #[instrument(skip_all, fields(peer_id = %peer_id))]
    fn on_peer_disconnected(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        if self
            .peer_tracker
            .set_maybe_disconnected(peer_id, connection_id)
        {
            debug!("Peer disconnected");
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn on_identify_event(&mut self, ev: identify::Event) {
        match ev {
            identify::Event::Received { peer_id, info, .. } => {
                // Inform Kademlia about the listening addresses
                // TODO: Remove this when rust-libp2p#5103 is implemented
                for addr in info.listen_addrs {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr);
                }
            }
            _ => trace!("Unhandled identify event"),
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn on_kademlia_event(&mut self, ev: kad::Event) {
        match ev {
            kad::Event::RoutingUpdated {
                peer, addresses, ..
            } => {
                self.peer_tracker.add_addresses(peer, addresses.iter());
            }
            kad::Event::OutboundQueryProgressed { result, .. } => {
                if let kad::QueryResult::GetProviders(Ok(providers)) = result {
                    if let kad::GetProvidersOk::FoundProviders { providers, .. } = providers {
                        for p in providers {
                            // TODO
                        }
                    }
                }
            }
            _ => trace!("Unhandled Kademlia event"),
        }
    }

    #[instrument(level = "debug", skip_all)]
    fn on_ping_event(&mut self, ev: ping::Event) {
        match ev.result {
            Ok(dur) => debug!(
                "Ping success: peer: {}, connection_id: {}, time: {:?}",
                ev.peer, ev.connection, dur
            ),
            Err(e) => {
                debug!(
                    "Ping failure: peer: {}, connection_id: {}, error: {}",
                    &ev.peer, &ev.connection, e
                );
                self.swarm.close_connection(ev.connection);
            }
        }
    }

    pub(crate) async fn stop(&mut self) {
        self.swarm
            .behaviour_mut()
            .connection_control
            .set_stopping(true);

        for listener in self.listeners.drain(..) {
            self.swarm.remove_listener(listener);
        }

        for (_, ids) in self.peer_tracker.connections() {
            for id in ids {
                self.swarm.close_connection(id);
            }
        }

        // Waiting until all established connections closed.
        while self
            .swarm
            .network_info()
            .connection_counters()
            .num_established()
            > 0
        {
            match self.swarm.select_next_some().await {
                // We may receive this if connection was established just before we trigger stop.
                SwarmEvent::ConnectionEstablished { connection_id, .. } => {
                    // We immediately close the connection in this case.
                    self.swarm.close_connection(connection_id);
                }
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    connection_id,
                    ..
                } => {
                    // This will generate the PeerDisconnected events.
                    self.on_peer_disconnected(peer_id, connection_id);
                }
                _ => {}
            }
        }
    }
}

fn init_kademlia(
    network_id: &str,
    keypair: &Keypair,
    bootnodes: &[Multiaddr],
    listen_on: &[Multiaddr],
) -> Result<kad::Behaviour<kad::store::MemoryStore>> {
    let local_peer_id = PeerId::from(keypair.public());
    let store = kad::store::MemoryStore::new(local_peer_id);

    let protocol_id = celestia_protocol_id(network_id, "/kad/1.0.0");
    let config = kad::Config::new(protocol_id);

    let mut kademlia = kad::Behaviour::with_config(local_peer_id, store, config);

    for addr in bootnodes {
        if let Some(peer_id) = addr.peer_id() {
            kademlia.add_address(&peer_id, addr.to_owned());
        }
    }

    if !listen_on.is_empty() {
        kademlia.set_mode(Some(kad::Mode::Server));
    }

    Ok(kademlia)
}

/// Converts a topic to `RecordKey`.
///
/// This is the equivalent of `nsToCid` that is used in [`RoutingDiscovery.FindPeers`][1]
/// of go-libp2p which later on is unwraped to the internal hash in [`IpfsDHT.FindProvidersAsync`][2].
///
/// [1]: https://github.com/libp2p/go-libp2p/blob/f6c14a215b2012f3839f1b7157dfec70a772143a/p2p/discovery/routing/routing.go#L75
/// [2]: https://github.com/libp2p/go-libp2p-kad-dht/blob/944883ea5a55102c8950478645d89183901859b4/routing.go#L504
pub(crate) fn topic_to_dht_key(topic: &str) -> RecordKey {
    Code::Sha2_256.digest(topic.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dht_key() {
        let key = topic_to_dht_key("/full/v0.1.0");
        let key_vec = topic_to_dht_key_vec("/full/v0.1.0");
        let expected = "bafkreidjoruznlfsmvecpvipnfpoe4jehgjjd753qob53bo77se6whba34"
            .parse::<Cid>()
            .unwrap();

        assert_eq!(key.as_ref(), &expected.hash().to_bytes());
        assert_eq!(key_vec.as_slice(), &expected.hash().to_bytes());
    }
}
