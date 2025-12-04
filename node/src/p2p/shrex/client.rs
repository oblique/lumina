use std::collections::HashMap;
use std::marker::PhantomData;

use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use libp2p::PeerId;
use libp2p::request_response::{self, OutboundFailure, OutboundRequestId};
use tokio::sync::oneshot;

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

pub(super) trait FromRawResponse<TReq, TRawResp>: Sized {
    fn from_raw_response(req: TReq, raw_resp: TRawResp) -> Self;
}

impl FromRawResponse<RowId, RawRow> for Row {
    fn from_raw_response(req: RowId, raw_resp: RawRow) -> Row {
        Row::from_raw(req, raw_resp).unwrap()
    }
}

impl FromRawResponse<SampleId, RawSample> for Sample {
    fn from_raw_response(req: SampleId, raw_resp: RawSample) -> Sample {
        Sample::from_raw(req, raw_resp).unwrap()
    }
}

struct ReqInfo<TReq, TResp> {
    id: OutboundRequestId,
    req: TReq,
    respond_to: oneshot::Sender<TResp>,
}

pub(super) struct Client<TReq, TResp, TRawResp>
where
    TResp: FromRawResponse<TReq, TRawResp>,
{
    reqs: HashMap<OutboundRequestId, ReqInfo<TReq, TResp>>,
    _raw_resp: PhantomData<TRawResp>,
}

impl<TReq, TResp, TRawResp> Client<TReq, TResp, TRawResp>
where
    TResp: FromRawResponse<TReq, TRawResp>,
{
    pub(super) fn new() -> Self {
        Self {
            reqs: HashMap::new(),
            _raw_resp: PhantomData,
        }
    }
}

impl<TReq, TResp, TRawResp> ClientHandler for Client<TReq, TResp, TRawResp>
where
    TResp: FromRawResponse<TReq, TRawResp>,
{
    type Request = TReq;
    type Response = TResp;
    type RawResponse = TRawResp;

    fn send_request(&mut self, req: TReq, respond_to: oneshot::Sender<TResp>) {
        //
    }

    fn on_response(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        response: Self::RawResponse,
    ) {
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
