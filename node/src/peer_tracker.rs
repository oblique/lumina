//! Primitives related to tracking the state of peers in the network.

use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use libp2p::{swarm::ConnectionId, Multiaddr, PeerId};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tokio::sync::watch;

use crate::events::{EventPublisher, NodeEvent};

/// Keeps track various information about peers.
#[derive(Debug)]
pub(crate) struct PeerTracker {
    peers: HashMap<PeerId, Peer>,
    connection_to_peer: HashMap<ConnectionId, PeerId>,
    info_tx: watch::Sender<PeerTrackerInfo>,
    event_pub: EventPublisher,
}

/// Statistics of the connected peers
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerTrackerInfo {
    /// Number of the connected peers.
    pub num_connected_peers: u64,
    /// Number of the connected trusted peers.
    pub num_connected_trusted_peers: u64,
}

#[derive(Clone, Debug, Default)]
struct Peer {
    addrs: SmallVec<[Multiaddr; 4]>,
    connections: SmallVec<[ConnectionId; 1]>,
    trusted: bool,
    archival: bool,
    kind: NodeKind,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum NodeKind {
    #[default]
    Unknown,
    Bridge,
    Full,
    Light,
}

impl NodeKind {
    fn from_agent_version(s: &str) -> NodeKind {
        let mut s = s.split('/');

        match s.next() {
            Some("lumina") => NodeKind::Light,
            Some("celestia-node") => match s.nth(1) {
                Some("bridge") => NodeKind::Bridge,
                Some("full") => NodeKind::Full,
                Some("light") => NodeKind::Light,
                _ => NodeKind::Unknown,
            },
            _ => NodeKind::Unknown,
        }
    }
}

impl Peer {
    fn is_connected(&self) -> bool {
        !self.connections.is_empty()
    }

    fn add_address(&mut self, addr: Multiaddr) {
        if !self.addrs.contains(&addr) {
            self.addrs.push(addr);
        }
    }
}

impl PeerTracker {
    /// Constructs an empty PeerTracker.
    pub(crate) fn new(event_pub: EventPublisher) -> Self {
        PeerTracker {
            peers: HashMap::new(),
            connection_to_peer: HashMap::new(),
            info_tx: watch::channel(PeerTrackerInfo::default()).0,
            event_pub,
        }
    }

    /// Returns the current [`PeerTrackerInfo`].
    pub(crate) fn info(&self) -> PeerTrackerInfo {
        self.info_tx.borrow().to_owned()
    }

    /// Returns a watcher for any [`PeerTrackerInfo`] changes.
    pub(crate) fn info_watcher(&self) -> watch::Receiver<PeerTrackerInfo> {
        self.info_tx.subscribe()
    }

    /// Adds a peer ID.
    ///
    /// Returns `true` if peer was not known from before.
    pub(crate) fn add_peer_id(&mut self, peer_id: PeerId) -> bool {
        match self.peers.entry(peer_id.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(Peer::default());
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Add addresses of a peer.
    pub(crate) fn add_addresses<I, A>(&mut self, peer_id: PeerId, addrs: I)
    where
        I: IntoIterator<Item = A>,
        A: Borrow<Multiaddr>,
    {
        let peer = self.peers.entry(peer_id.to_owned()).or_default();

        for addr in addrs {
            peer.add_address(addr.borrow().to_owned());
        }
    }

    /// Sets peer as trusted.
    pub(crate) fn set_trusted(&mut self, peer_id: PeerId, is_trusted: bool) {
        let peer = self.peers.entry(peer_id.to_owned()).or_default();

        if peer.trusted == is_trusted {
            // Nothing to be done
            return;
        }

        peer.trusted = is_trusted;

        // If peer was already connected, then `num_connected_trusted_peers`
        // needs to be adjusted based on the new information.
        if peer.is_connected() {
            self.info_tx.send_modify(|tracker_info| {
                if is_trusted {
                    tracker_info.num_connected_trusted_peers += 1;
                } else {
                    tracker_info.num_connected_trusted_peers -= 1;
                }
            });
        }
    }

    /// Add an active connection of a peer.
    pub(crate) fn add_connection(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        address: impl Into<Option<Multiaddr>>,
    ) {
        let peer = self.peers.entry(peer_id.to_owned()).or_default();
        let prev_connected = peer.is_connected();

        if let Some(address) = address.into() {
            peer.add_address(address);
        }

        peer.connections.push(connection_id);
        self.connection_to_peer.insert(connection_id, peer_id);

        // If peer was not already connected from before
        if !prev_connected {
            increment_connected_peers(&self.info_tx, peer.trusted);

            self.event_pub.send(NodeEvent::PeerConnected {
                id: peer_id.to_owned(),
                trusted: peer.trusted,
            });
        }
    }

    /// Remove a connection from a peer.
    pub(crate) fn remove_connection(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };

        peer.connections.retain(|id| *id != connection_id);
        self.connection_to_peer.remove(&connection_id);

        // If this is the last connection from the peer
        if peer.is_connected() {
            decrement_connected_peers(&self.info_tx, peer.trusted);

            self.event_pub.send(NodeEvent::PeerDisconnected {
                id: peer_id.to_owned(),
                trusted: peer.trusted,
            });
        }
    }

    pub(crate) fn on_agent_version(&mut self, peer_id: PeerId, agent_version: &str) {
        let peer = self.peers.entry(peer_id.to_owned()).or_default();
        peer.kind = NodeKind::from_agent_version(agent_version);
    }

    /// Returns true if peer is connected.
    #[allow(dead_code)]
    pub(crate) fn is_connected(&self, peer_id: PeerId) -> bool {
        self.peers
            .get(&peer_id)
            .map(|peer| peer.is_connected())
            .unwrap_or(false)
    }

    /// Returns the addresses of the peer.
    #[allow(dead_code)]
    pub(crate) fn addresses(&self, peer_id: PeerId) -> Option<&[Multiaddr]> {
        Some(&self.peers.get(&peer_id)?.addrs[..])
    }

    /// Removes a peer.
    #[allow(dead_code)]
    pub(crate) fn remove(&mut self, peer_id: PeerId) {
        if let Some(peer) = self.peers.remove(&peer_id) {
            for connection_id in peer.connections {
                self.connection_to_peer.remove(&connection_id);
            }
        }
    }

    /// Returns connected peers.
    pub(crate) fn connected_peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(|(peer_id, peer)| {
            if peer.is_connected() {
                Some(*peer_id)
            } else {
                None
            }
        })
    }

    /// Returns all connections.
    pub(crate) fn connections(&self) -> impl Iterator<Item = (ConnectionId, PeerId)> + '_ {
        self.connection_to_peer
            .iter()
            .map(|(&connection_id, &peer_id)| (connection_id, peer_id))
    }

    /// Returns one of the best peers.
    pub(crate) fn best_peer(&self) -> Option<PeerId> {
        const MAX_PEER_SAMPLE: usize = 128;

        // TODO: Implement peer score and return the best.
        let mut peers = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.is_connected())
            .take(MAX_PEER_SAMPLE)
            .map(|(peer_id, peer)| peer_id)
            .collect::<SmallVec<[_; MAX_PEER_SAMPLE]>>();

        peers.shuffle(&mut rand::thread_rng());

        peers.first().copied().copied()
    }

    /// Returns trusted peers
    pub(crate) fn trusted_peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(|(peer_id, peer)| {
            if peer.is_connected() && peer.trusted {
                Some(*peer_id)
            } else {
                None
            }
        })
    }
}

fn increment_connected_peers(info_tx: &watch::Sender<PeerTrackerInfo>, trusted: bool) {
    info_tx.send_modify(|tracker_info| {
        tracker_info.num_connected_peers += 1;

        if trusted {
            tracker_info.num_connected_trusted_peers += 1;
        }
    });
}

fn decrement_connected_peers(info_tx: &watch::Sender<PeerTrackerInfo>, trusted: bool) {
    info_tx.send_modify(|tracker_info| {
        tracker_info.num_connected_peers -= 1;

        if trusted {
            tracker_info.num_connected_trusted_peers -= 1;
        }
    });
}

#[cfg(test)]
mod tests {
    use crate::events::EventChannel;

    use super::*;

    #[test]
    fn trust_before_connect() {
        let event_channel = EventChannel::new();
        let mut tracker = PeerTracker::new(event_channel.publisher());
        let mut watcher = tracker.info_watcher();
        let peer = PeerId::random();

        assert!(!watcher.has_changed().unwrap());

        tracker.set_trusted(peer, true);
        assert!(!watcher.has_changed().unwrap());

        tracker.add_connection(peer, ConnectionId::new_unchecked(1), None);
        assert!(tracker.is_connected(peer));
        assert!(watcher.has_changed().unwrap());
        let info = watcher.borrow_and_update().to_owned();
        assert_eq!(info.num_connected_peers, 1);
        assert_eq!(info.num_connected_trusted_peers, 1);
    }

    #[test]
    fn trust_after_connect() {
        let event_channel = EventChannel::new();
        let mut tracker = PeerTracker::new(event_channel.publisher());
        let mut watcher = tracker.info_watcher();
        let peer = PeerId::random();

        assert!(!watcher.has_changed().unwrap());

        tracker.add_connection(peer, ConnectionId::new_unchecked(1), None);
        assert!(tracker.is_connected(peer));
        assert!(watcher.has_changed().unwrap());
        let info = watcher.borrow_and_update().to_owned();
        assert_eq!(info.num_connected_peers, 1);
        assert_eq!(info.num_connected_trusted_peers, 0);

        tracker.set_trusted(peer, true);
        assert!(watcher.has_changed().unwrap());
        let info = watcher.borrow_and_update().to_owned();
        assert_eq!(info.num_connected_peers, 1);
        assert_eq!(info.num_connected_trusted_peers, 1);
    }

    #[test]
    fn untrust_after_connect() {
        let event_channel = EventChannel::new();
        let mut tracker = PeerTracker::new(event_channel.publisher());
        let mut watcher = tracker.info_watcher();
        let peer = PeerId::random();

        assert!(!watcher.has_changed().unwrap());

        tracker.set_trusted(peer, true);
        assert!(!watcher.has_changed().unwrap());

        tracker.add_connection(peer, ConnectionId::new_unchecked(1), None);
        assert!(tracker.is_connected(peer));
        assert!(watcher.has_changed().unwrap());
        let info = watcher.borrow_and_update().to_owned();
        assert_eq!(info.num_connected_peers, 1);
        assert_eq!(info.num_connected_trusted_peers, 1);

        tracker.set_trusted(peer, false);
        assert!(watcher.has_changed().unwrap());
        let info = watcher.borrow_and_update().to_owned();
        assert_eq!(info.num_connected_peers, 1);
        assert_eq!(info.num_connected_trusted_peers, 0);
    }

    #[test]
    fn node_kind() {
        assert_eq!(
            NodeKind::from_agent_version("lumina/celestia/0.14.0"),
            NodeKind::Light
        );

        assert_eq!(
            NodeKind::from_agent_version("celestia-node/celestia/bridge/v0.24.1/fb95d45"),
            NodeKind::Bridge
        );

        assert_eq!(
            NodeKind::from_agent_version("celestia-node/celestia/full/v0.24.1/fb95d45"),
            NodeKind::Full
        );

        assert_eq!(
            NodeKind::from_agent_version("celestia-node/celestia/light/v0.24.1/fb95d45"),
            NodeKind::Light
        );

        assert_eq!(
            NodeKind::from_agent_version("probelab-node/celestia/ant/v0.1.0"),
            NodeKind::Unknown
        );
    }
}
