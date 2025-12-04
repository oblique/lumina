use std::collections::HashMap;

use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use libp2p::PeerId;
use libp2p::request_response::{self, OutboundFailure, OutboundRequestId};
use tokio::sync::oneshot;

struct RowReq {
    id: RowId,
    respond_to: oneshot::Sender<Row>,
}

struct SampleReq {
    id: SampleId,
    respond_to: oneshot::Sender<Sample>,
}

pub(super) trait ClientHandler {
    type Request;
    type Response;
    type RawResponse;

    fn send_request(&mut self, req: Self::Request, respond_to: oneshot::Sender<Self::Response>);

    fn on_event(
        &mut self,
        ev: request_response::Event<Self::Request, Self::RawResponse>,
    ) -> Option<request_response::Event<Self::Request, Self::RawResponse>> {
        match ev {
            // Received a response for an ongoing outbound request
            request_response::Event::Message {
                message:
                    request_response::Message::Response {
                        request_id,
                        response,
                    },
                peer,
                ..
            } => {
                self.on_response(peer, request_id, response);
                None
            }

            // Failure while client requests
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                self.on_outbound_failure(peer, request_id, error);
                None
            }

            // Event could not be handled from here.
            ev => Some(ev),
        }
    }

    fn on_response(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        response: Self::RawResponse,
    );

    fn on_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    );
}

/*
pub(super) struct Client<TReq, TResp, TRawResp> {
    reqs: HashMap<OutboundRequestId, RowReq>,
}
*/

pub(super) struct RowClient {
    reqs: HashMap<OutboundRequestId, RowReq>,
}

impl RowClient {
    pub(super) fn new() -> RowClient {
        RowClient {
            reqs: HashMap::new(),
        }
    }
}

impl ClientHandler for RowClient {
    type Request = RowId;
    type Response = Row;
    type RawResponse = RawRow;

    fn send_request(&mut self, req: RowId, respond_to: oneshot::Sender<Row>) {
        //
    }

    fn on_response(&mut self, peer_id: PeerId, request_id: OutboundRequestId, response: RawRow) {
        //
    }

    fn on_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    ) {
        //
    }
}

pub(super) struct SampleClient {
    reqs: HashMap<OutboundRequestId, SampleReq>,
}

impl SampleClient {
    pub(super) fn new() -> SampleClient {
        SampleClient {
            reqs: HashMap::new(),
        }
    }
}

impl ClientHandler for SampleClient {
    type Request = SampleId;
    type Response = Sample;
    type RawResponse = RawSample;

    fn send_request(&mut self, req: SampleId, respond_to: oneshot::Sender<Sample>) {
        //
    }

    fn on_response(&mut self, peer_id: PeerId, request_id: OutboundRequestId, response: RawSample) {
        //
    }

    fn on_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    ) {
        //
    }
}
