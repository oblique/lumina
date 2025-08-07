use std::task::{Context, Poll};

use libp2p::{
    connection_limits::ConnectionLimits,
    core::{transport::PortUse, Endpoint},
    kad::{self, RecordKey},
    swarm::{
        dummy, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use multihash_codetable::{Code, MultihashDigest};
use void::Void;

use crate::peer_tracker::PeerTracker;

// TODO: Wrap ConnectionLimits in it and exclude limits from trusted peers
pub(crate) struct Behaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub(crate) peer_tracker: PeerTracker,
}

impl Behaviour {
    pub(crate) fn new() -> Behaviour {
        todo!();
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = THandler<kad::Behaviour<kad::store::MemoryStore>>;
    type ToSwarm = Void;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.kademlia
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr);
        Ok(())
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.kademlia.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.kademlia.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.kademlia.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.kademlia
            .on_connection_handler_event(peer_id, connection_id, event);
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.kademlia.on_swarm_event(event);
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // TODO
        self.kademlia.poll(cx);

        Poll::Pending
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
