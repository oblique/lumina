use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use beetswap::multihasher::{Multihasher, MultihasherError};
use beetswap::QueryId;
use blockstore::block::CidError;
use blockstore::Blockstore;
use celestia_proto::bitswap::Block;
use celestia_types::nmt::Namespace;
use celestia_types::row::{Row, RowId, ROW_ID_MULTIHASH_CODE};
use celestia_types::row_namespace_data::{
    RowNamespaceData, RowNamespaceDataId, ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE,
};
use celestia_types::sample::{Sample, SampleId, SAMPLE_ID_MULTIHASH_CODE};
use celestia_types::DataAvailabilityHeader;
use cid::CidGeneric;
use dashmap::DashMap;
use libp2p::core::transport::PortUse;
use libp2p::core::Endpoint;
use libp2p::multihash::Multihash;
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use prost::Message;
use tokio::sync::RwLock;
use tracing::instrument;

use crate::p2p::{P2pError, Result, MAX_MH_SIZE};
use crate::store::Store;
use crate::utils::{celestia_protocol_id, OneshotResultSender, OneshotResultSenderExt};

pub(super) type Cid = CidGeneric<MAX_MH_SIZE>;

pub(super) struct ShwapBehaviour<B>
where
    B: Blockstore + 'static,
{
    bitswap: beetswap::Behaviour<MAX_MH_SIZE, B>,
    dah_table: Arc<DashMap<Cid, Arc<DataAvailabilityHeader>>>,
    queries: HashMap<beetswap::QueryId, OneshotResultSender<Vec<u8>, P2pError>>,
}

impl<B> ShwapBehaviour<B>
where
    B: Blockstore + 'static,
{
    pub fn new(blockstore: Arc<B>, network_id: &str) -> Result<ShwapBehaviour<B>> {
        let dah_table = Arc::new(DashMap::new());
        let protocol_prefix = celestia_protocol_id(network_id, "shwap");

        let bitswap = beetswap::Behaviour::builder(blockstore)
            .protocol_prefix(protocol_prefix.as_ref())?
            .register_multihasher(ShwapMultihasher {
                dah_table: dah_table.clone(),
            })
            .client_set_send_dont_have(false)
            .build();

        Ok(ShwapBehaviour {
            bitswap,
            dah_table,
            queries: HashMap::new(),
        })
    }

    pub fn needs_dah(&self, cid: &Cid) -> bool {
        !self.dah_table.contains_key(cid)
    }

    pub fn get(
        &mut self,
        cid: &Cid,
        dah: Option<DataAvailabilityHeader>,
        respond_to: OneshotResultSender<Vec<u8>, P2pError>,
    ) {
        if let Some(dah) = dah {
            self.dah_table.insert(cid.to_owned(), Arc::new(dah));
        }

        let query_id = self.bitswap.get(cid);
        self.queries.insert(query_id, respond_to);
    }

    #[instrument(level = "trace", skip(self))]
    fn on_beetswap_event(&mut self, ev: beetswap::Event) {
        match ev {
            beetswap::Event::GetQueryResponse { query_id, data } => {
                if let Some(respond_to) = self.queries.remove(&query_id) {
                    respond_to.maybe_send_ok(data);
                }
            }
            beetswap::Event::GetQueryError { query_id, error } => {
                if let Some(respond_to) = self.queries.remove(&query_id) {
                    let error: P2pError = error.into();
                    respond_to.maybe_send_err(error);
                }
            }
        }
    }
}

impl<B> NetworkBehaviour for ShwapBehaviour<B>
where
    B: Blockstore + 'static,
{
    type ConnectionHandler =
        <beetswap::Behaviour<MAX_MH_SIZE, B> as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = ();

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.bitswap
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.bitswap.handle_established_inbound_connection(
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
        self.bitswap.handle_pending_outbound_connection(
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
        self.bitswap.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.bitswap.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.bitswap
            .on_connection_handler_event(peer_id, connection_id, event);
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<(), THandlerInEvent<Self>>> {
        // Remove closed channels and cancel their queries.
        self.queries
            .retain(|&query_id, chan| match chan.poll_closed(cx) {
                Poll::Ready(_) => {
                    self.bitswap.cancel(query_id);
                    false
                }
                Poll::Pending => true,
            });

        // Poll for events
        match ready!(self.bitswap.poll(cx)) {
            ToSwarm::GenerateEvent(ev) => {
                self.on_beetswap_event(ev);
                Poll::Ready(ToSwarm::GenerateEvent(()))
            }
            ev => Poll::Ready(ev.map_out(|_| ())),
        }
    }
}

/// Multihasher for Shwap types.
pub(super) struct ShwapMultihasher {
    dah_table: Arc<DashMap<Cid, Arc<DataAvailabilityHeader>>>,
}

impl Multihasher<MAX_MH_SIZE> for ShwapMultihasher {
    async fn hash(
        &self,
        multihash_code: u64,
        input: &[u8],
    ) -> Result<Multihash<MAX_MH_SIZE>, MultihasherError> {
        macro_rules! hash_shwap_block {
            ($id_type:ty, $container_type:ty) => {{
                let block = Block::decode(input).map_err(MultihasherError::custom_fatal)?;
                let cid = Cid::read_bytes(block.cid.as_slice())
                    .map_err(MultihasherError::custom_fatal)?;

                let id = <$id_type>::try_from(&cid).map_err(MultihasherError::custom_fatal)?;
                let container = <$container_type>::decode(id, block.container.as_slice())
                    .map_err(MultihasherError::custom_fatal)?;

                // There are three cases were a CID will not exists in the DAH table:
                //
                // 1. A peer replied with an unknown CID, without us requesting it.
                // 2. Multiple peers replied.
                // 3. A peer replied just before it received our cancellation request.
                //
                // Because no. 2 can happen often and we want to avoid log spamming,
                // we decided to just ignore the reply.
                let dah = self
                    .dah_table
                    .get(&cid)
                    .ok_or(MultihasherError::Ignore)?
                    .value()
                    .clone();

                container
                    .verify(id, &dah)
                    .map_err(MultihasherError::custom_fatal)?;

                // We got a valid reply, DAH is not needed anymore.
                self.dah_table.remove(&cid);

                Ok(cid.hash().to_owned())
            }};
        }

        match multihash_code {
            ROW_ID_MULTIHASH_CODE => hash_shwap_block!(RowId, Row),
            ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE => {
                hash_shwap_block!(RowNamespaceDataId, RowNamespaceData)
            }
            SAMPLE_ID_MULTIHASH_CODE => hash_shwap_block!(SampleId, Sample),
            _ => Err(MultihasherError::UnknownMultihashCode),
        }
    }
}

pub(crate) fn sample_cid(row_index: u16, column_index: u16, block_height: u64) -> Result<Cid> {
    let sample_id = SampleId::new(row_index, column_index, block_height).map_err(P2pError::Cid)?;
    convert_cid(&sample_id.into())
}

pub(crate) fn convert_cid<const S: usize>(cid: &CidGeneric<S>) -> Result<Cid> {
    beetswap::utils::convert_cid(cid).ok_or(P2pError::Cid(celestia_types::Error::CidError(
        CidError::InvalidMultihashLength(S),
    )))
}

/// extracts the `container` part from shwaps `Block` wrapper if the cid matches expected one
pub(crate) fn get_block_container(expected_cid: &Cid, block: &[u8]) -> Result<Vec<u8>> {
    let block = Block::decode(block)?;
    let block_cid = Cid::read_bytes(block.cid.as_slice())?;
    if block_cid != *expected_cid {
        return Err(P2pError::Shwap(format!(
            "cid in block ({}) different than expected ({})",
            block_cid, expected_cid
        )));
    }

    Ok(block.container)
}

pub(crate) fn get_block_number(cid: &Cid) -> Option<u64> {
    match cid.hash().code() {
        ROW_ID_MULTIHASH_CODE => RowId::try_from(cid).map(|id| id.block_height()).ok(),
        ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE => RowNamespaceDataId::try_from(cid)
            .map(|id| id.block_height())
            .ok(),
        SAMPLE_ID_MULTIHASH_CODE => SampleId::try_from(cid).map(|id| id.block_height()).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::InMemoryStore;
    use crate::test_utils::async_test;
    use bytes::BytesMut;
    use celestia_types::consts::appconsts::AppVersion;
    use celestia_types::test_utils::{generate_dummy_eds, ExtendedHeaderGenerator};
    use celestia_types::{AxisType, DataAvailabilityHeader};

    #[async_test]
    async fn hash() {
        let store = Arc::new(InMemoryStore::new());

        let eds = generate_dummy_eds(4, AppVersion::V2);
        let dah = DataAvailabilityHeader::from_eds(&eds);

        let mut gen = ExtendedHeaderGenerator::new();
        let header = gen.next_with_dah(dah.clone());

        let sample = Sample::new(0, 0, AxisType::Row, &eds).unwrap();
        let mut sample_bytes = BytesMut::new();
        sample.encode(&mut sample_bytes);

        let cid = sample_cid(0, 0, 1).unwrap();
        let sample_id = SampleId::new(0, 0, 1).unwrap();

        sample.verify(sample_id, &dah).unwrap();
        store.insert(header).await.unwrap();

        let block = Block {
            cid: cid.to_bytes(),
            container: sample_bytes.to_vec(),
        };

        let hash = ShwapMultihasher::new(store)
            .hash(SAMPLE_ID_MULTIHASH_CODE, &block.encode_to_vec())
            .await
            .unwrap();

        assert_eq!(hash, *cid.hash());
    }
}
