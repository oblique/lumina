use std::task::{Context, Poll};

use blockstore::Blockstore;
use libp2p::{
    connection_limits::ConnectionLimits,
    core::{transport::PortUse, Endpoint},
    identify,
    kad::{self, RecordKey},
    swarm::{
        dummy, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId, Swarm,
};
use multihash_codetable::{Code, MultihashDigest};
use void::Void;

use crate::p2p::Behaviour;
use crate::peer_tracker::PeerTracker;
use crate::store::Store;

/// NOTE: This does not implement `NetworkBehaviour` on purpose.
pub(crate) struct PeerManager {
    //kademlia: kad::Behaviour<kad::store::MemoryStore>,
    //    inner: imp::Behaviour,
    pub(crate) peer_tracker: PeerTracker,
}

impl PeerManager {
    pub(crate) fn new(peer_tracker: PeerTracker) -> PeerManager {
        PeerManager { peer_tracker }
    }

    pub(crate) fn on_kademlia_event(&mut self, ev: kad::Event) {}

    pub(crate) fn on_identify_event(&mut self, ev: identify::Event) {}

    pub(crate) async fn poll<B, S>(&mut self, swarm: &mut Swarm<Behaviour<B, S>>)
    where
        B: Blockstore,
        S: Store,
    {
    }
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
