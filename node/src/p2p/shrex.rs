// TODO: remove this
#![allow(unused)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::{Context, Poll};

use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use futures::AsyncWrite;
use libp2p::core::Endpoint;
use libp2p::core::transport::PortUse;
use libp2p::identity::Keypair;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::request_response::{
    InboundFailure, InboundRequestId, OutboundFailure, OutboundRequestId,
};
use libp2p::swarm::handler::ConnectionEvent;
use libp2p::swarm::{
    ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent, ConnectionId, FromSwarm,
    NetworkBehaviour, SubstreamProtocol, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId, gossipsub};
use thiserror::Error;
use tokio::sync::oneshot;

mod client;
mod codec;

use self::client::{Client, ClientEndpointHandler};
use self::codec::{ReqRespCodec, RowCodec, SampleCodec};

use crate::p2p::P2pError;
use crate::peer_tracker::PeerTracker;
use crate::store::Store;
use crate::utils::protocol_id;

type RowReqRespEvent = request_response::Event<RowId, RawRow>;
type RowReqRespMessage = request_response::Message<RowId, RawRow>;
type SampleReqRespEvent = request_response::Event<SampleId, Sample>;
type SampleReqRespMessage = request_response::Message<SampleId, Sample>;

type ReqRespBehaviour<TReq, TResp> = request_response::Behaviour<ReqRespCodec<TReq, TResp>>;

pub(crate) struct Config<'a, S> {
    pub network_id: &'a str,
    pub local_keypair: &'a Keypair,
    pub header_store: Arc<S>,
}

pub(crate) struct Behaviour<S>
where
    S: Store + 'static,
{
    inner: InnerBehaviour,
    client: Client,
    _da_pools: HashMap<u64, HashSet<PeerId>>,
    _store: Arc<S>,
}

#[derive(NetworkBehaviour)]
pub(crate) struct InnerBehaviour {
    // TODO: this is a workaround, should be replaced with a real floodsub
    // rust-libp2p implementation of the floodsub isn't compliant with a spec,
    // so we work that around by using a gossipsub configured with floodsub support.
    // Gossipsub will always receive from and forward messages to all the floodsub peers.
    // Since we always maintain a connection with a few bridge (and possibly archival)
    // nodes, then if we assume those nodes correctly use only floodsub protocol,
    // we cannot be isolated in a way described in a shrex-sub spec:
    // https://github.com/celestiaorg/celestia-node/blob/76db37cc4ac09e892122a081b8bea24f87899f11/specs/src/shrex/shrex-sub.md#why-not-gossipsub
    shrex_sub: gossipsub::Behaviour,
    row_req_resp: request_response::Behaviour<ReqRespCodec<RowId, RawRow>>,
    sample_req_resp: request_response::Behaviour<ReqRespCodec<SampleId, RawSample>>,
}

#[derive(Debug)]
pub(crate) enum Event {
    SchedulePendingRequests,
}

#[derive(Debug, Error)]
pub enum ShrExError {
    /// Error when handling connection to the client.
    #[error("Outbound failure: {0}")]
    OutboundFailure(OutboundFailure),

    /// Request cancelled because [`Node`] is stopping.
    ///
    /// [`Node`]: crate::node::Node
    #[error("Request cancelled because `Node` is stopping")]
    RequestCancelled,
}

impl<S> Behaviour<S>
where
    S: Store,
{
    pub fn new(config: Config<'_, S>) -> Result<Self, P2pError> {
        let message_authenticity =
            gossipsub::MessageAuthenticity::Signed(config.local_keypair.clone());

        let shrex_sub_config = gossipsub::ConfigBuilder::default()
            // replace default meshsub protocols with something that won't match
            // note: this may create an additional, exclusive lumina-only gossip mesh :)
            .protocol_id_prefix("/nosub")
            // add floodsub protocol
            .support_floodsub()
            // and floodsub publish behaviour
            .flood_publish(true)
            .validation_mode(gossipsub::ValidationMode::Strict)
            .validate_messages()
            .build()
            .map_err(|e| P2pError::GossipsubInit(e.to_string()))?;

        // build a gossipsub network behaviour
        let mut shrex_sub: gossipsub::Behaviour =
            gossipsub::Behaviour::new(message_authenticity, shrex_sub_config)
                .map_err(|e| P2pError::GossipsubInit(e.to_string()))?;

        let topic = format!("{}/eds-sub/v0.2.0", config.network_id);
        shrex_sub
            .subscribe(&gossipsub::IdentTopic::new(topic))
            .map_err(|e| P2pError::GossipsubInit(e.to_string()))?;

        /*
        let x = format!("{}/shrex/v0.1.0/sample_v0", config.network_id);
        let x = libp2p::StreamProtocol::try_from_owned(x).expect("does not start from '/'");
        */

        Ok(Self {
            inner: InnerBehaviour {
                shrex_sub,
                row_req_resp: request_response::Behaviour::new(
                    [(
                        protocol_id(config.network_id, "/shrex/v0.1.0/row_v0"),
                        ProtocolSupport::Outbound,
                    )],
                    request_response::Config::default(),
                ),
                sample_req_resp: request_response::Behaviour::new(
                    [(
                        dbg!(protocol_id(config.network_id, "/shrex/v0.1.0/sample_v0")),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                ),
            },
            client: Client::new(),
            _da_pools: HashMap::new(),
            _store: config.header_store,
        })
    }

    fn on_to_swarm(
        &mut self,
        ev: ToSwarm<InnerBehaviourEvent, THandlerInEvent<InnerBehaviour>>,
    ) -> Option<ToSwarm<Event, THandlerInEvent<Self>>> {
        match ev {
            ToSwarm::GenerateEvent(InnerBehaviourEvent::ShrexSub(ev)) => {
                // TODO: Handle shrex sub events and do not propagate them
                None
            }
            ToSwarm::GenerateEvent(InnerBehaviourEvent::RowReqResp(ev)) => {
                self.client.row.on_event(ev);
                None
            }
            ToSwarm::GenerateEvent(InnerBehaviourEvent::SampleReqResp(ev)) => {
                self.client.sample.on_event(ev);
                None
            }
            _ => Some(ev.map_out(|_| unreachable!("GenerateEvent handled"))),
        }
    }

    pub(crate) fn on_stop(&mut self) {
        self.client.on_stop();
    }

    pub(crate) fn schedule_pending_requests(&mut self, peer_tracker: &PeerTracker) {
        self.client
            .schedule_pending_requests(&mut self.inner, peer_tracker);
    }

    pub(crate) fn get_row(
        &mut self,
        height: u64,
        index: u16,
        respond_to: oneshot::Sender<Result<Row, P2pError>>,
    ) {
        let row_id = RowId::new(index, height).expect("todo");
        self.client.row.send_request(row_id, respond_to);
    }

    pub(crate) fn get_sample(
        &mut self,
        height: u64,
        row_index: u16,
        column_index: u16,
        respond_to: oneshot::Sender<Result<Sample, P2pError>>,
    ) {
        let sample_id = SampleId::new(row_index, column_index, height).expect("todo");
        self.client.sample.send_request(sample_id, respond_to);
    }
}

impl<S> NetworkBehaviour for Behaviour<S>
where
    S: Store + 'static,
{
    type ConnectionHandler = InnerConnectionHandler;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<Self::ConnectionHandler, ConnectionDenied> {
        self.inner.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<Self::ConnectionHandler, ConnectionDenied> {
        self.inner.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.inner
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.inner.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.inner.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.inner
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Poll::Ready(ev) = self.inner.poll(cx)
            && let Some(ev) = self.on_to_swarm(ev)
        {
            return Poll::Ready(ev);
        }

        if let Poll::Ready(ev) = self.client.poll(cx) {
            return Poll::Ready(ToSwarm::GenerateEvent(ev));
        }

        Poll::Pending
    }
}

type InnerConnectionHandler = <InnerBehaviour as NetworkBehaviour>::ConnectionHandler;

pub(crate) struct ConnHandler(InnerConnectionHandler);

impl ConnectionHandler for ConnHandler {
    type ToBehaviour = <InnerConnectionHandler as ConnectionHandler>::ToBehaviour;
    type FromBehaviour = <InnerConnectionHandler as ConnectionHandler>::FromBehaviour;
    type InboundProtocol = <InnerConnectionHandler as ConnectionHandler>::InboundProtocol;
    type InboundOpenInfo = <InnerConnectionHandler as ConnectionHandler>::InboundOpenInfo;
    type OutboundProtocol = <InnerConnectionHandler as ConnectionHandler>::OutboundProtocol;
    type OutboundOpenInfo = <InnerConnectionHandler as ConnectionHandler>::OutboundOpenInfo;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.0.listen_protocol()
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        self.0.poll(cx)
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        self.0.on_behaviour_event(event)
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            '_,
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        self.0.on_connection_event(event)
    }

    fn connection_keep_alive(&self) -> bool {
        self.0.connection_keep_alive()
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::ToBehaviour>> {
        self.0.poll_close(cx)
    }
}
