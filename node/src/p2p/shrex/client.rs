use std::collections::HashMap;

use celestia_proto::shwap::{Row as RawRow, Share as RawShare};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use libp2p::PeerId;
use libp2p::request_response::{OutboundFailure, OutboundRequestId};
use tokio::sync::oneshot;

pub(super) struct Client {
    row_reqs: HashMap<OutboundRequestId, RowReq>,
    sample_reqs: HashMap<OutboundRequestId, SampleReq>,
}

struct RowReq {
    id: RowId,
    respond_to: oneshot::Sender<Row>,
}

struct SampleReq {
    id: SampleId,
    respond_to: oneshot::Sender<Sample>,
}

impl Client {
    pub(super) fn new() -> Client {
        Client {
            row_reqs: HashMap::new(),
            sample_reqs: HashMap::new(),
        }
    }

    pub(super) fn on_row_request(&mut self, row_id: RowId, respond_to: oneshot::Sender<Row>) {
        //
    }

    pub(super) fn on_row_response(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        raw_row: RawRow,
    ) {
        //
    }

    pub(super) fn on_row_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    ) {
        //
    }

    pub(super) fn on_sample_request(
        &mut self,
        sample_id: SampleId,
        respond_to: oneshot::Sender<Sample>,
    ) {
        //
    }

    pub(super) fn on_sample_response(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        sample: Sample,
    ) {
        //
    }

    pub(super) fn on_sample_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    ) {
        //
    }
}
