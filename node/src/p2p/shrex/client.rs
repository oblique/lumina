use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::task::{Context, Poll};

use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use libp2p::PeerId;
use libp2p::request_response::{self, Codec, OutboundFailure, OutboundRequestId};
use tokio::sync::oneshot;

use crate::p2p::shrex::codec::{ByteCodec, ReqRespCodec};
use crate::peer_tracker::PeerTracker;

pub(super) struct Client {
    pub(super) row: ClientEndpoint<RowId, Row, RawRow>,
    pub(super) sample: ClientEndpoint<SampleId, Sample, RawSample>,
}

impl Client {
    pub(super) fn new() -> Client {
        Client {
            row: ClientEndpoint::new(),
            sample: ClientEndpoint::new(),
        }
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

struct State<TReq, TResp> {
    req: TReq,
    respond_to: oneshot::Sender<TResp>,
}

pub(super) struct ClientEndpoint<TReq, TResp, TRawResp>
where
    TResp: FromRawResponse<TReq, TRawResp>,
{
    reqs: HashMap<OutboundRequestId, State<TReq, TResp>>,
    pending_reqs: VecDeque<State<TReq, TResp>>,
    _raw_resp: PhantomData<TRawResp>,
}

impl<TReq, TResp, TRawResp> ClientEndpoint<TReq, TResp, TRawResp>
where
    TResp: FromRawResponse<TReq, TRawResp>,
{
    pub(super) fn new() -> Self {
        Self {
            reqs: HashMap::new(),
            pending_reqs: VecDeque::new(),
            _raw_resp: PhantomData,
        }
    }
}

impl<TReq, TResp, TRawResp> ClientEndpointHandler for ClientEndpoint<TReq, TResp, TRawResp>
where
    TReq: ByteCodec,
    TResp: FromRawResponse<TReq, TRawResp>,
    TRawResp: ByteCodec,
{
    type TReq = TReq;
    type TResp = TResp;
    type TRawResp = TRawResp;

    fn send_request(&mut self, req: TReq, respond_to: oneshot::Sender<TResp>) {
        //
    }

    fn on_response(&mut self, peer_id: PeerId, request_id: OutboundRequestId, response: TRawResp) {
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

    fn has_pending_requests(&self) -> bool {
        !self.pending_reqs.is_empty()
    }

    fn schedule_pending_requests(
        &mut self,
        sender: &mut request_response::Behaviour<ReqRespCodec<TReq, TRawResp>>,
        peer_tracker: &PeerTracker,
    ) {
        if self.pending_reqs.is_empty() {
            return;
        }
        //
    }
}

pub(super) trait ClientEndpointHandler {
    type TReq: ByteCodec;
    type TResp;
    type TRawResp: ByteCodec;

    fn send_request(&mut self, req: Self::TReq, respond_to: oneshot::Sender<Self::TResp>);

    fn on_event(
        &mut self,
        ev: request_response::Event<Self::TReq, Self::TRawResp>,
    ) -> Option<request_response::Event<Self::TReq, Self::TRawResp>> {
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
        response: Self::TRawResp,
    );

    fn on_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    );

    fn has_pending_requests(&self) -> bool;

    fn schedule_pending_requests(
        &mut self,
        sender: &mut request_response::Behaviour<ReqRespCodec<Self::TReq, Self::TRawResp>>,
        peer_tracker: &PeerTracker,
    );
}

pub(super) trait FromRawResponse<TReq, TRawResp>: Sized {
    fn from_raw_response(req: TReq, raw_resp: TRawResp) -> Self;
}

impl FromRawResponse<RowId, RawRow> for Row {
    fn from_raw_response(req: RowId, raw_resp: RawRow) -> Row {
        Row::from_raw(req, raw_resp).expect("todo")
    }
}

impl FromRawResponse<SampleId, RawSample> for Sample {
    fn from_raw_response(req: SampleId, raw_resp: RawSample) -> Sample {
        Sample::from_raw(req, raw_resp).expect("todo")
    }
}
