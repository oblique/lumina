use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use std::time::Duration;

use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::eds::{EdsId, ExtendedDataSquare};
use celestia_types::namespace_data::{NamespaceData, NamespaceDataId};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use libp2p::PeerId;
use libp2p::request_response::{self, Codec, OutboundFailure, OutboundRequestId};
use lumina_utils::time::Interval;
use rand::seq::SliceRandom;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::p2p::P2pError;
use crate::p2p::shrex::codec::{RequestCodec, ResponseCodec};
use crate::p2p::shrex::req_resp::{Response, ShrexBytesCodec};
use crate::p2p::shrex::{Event, InnerBehaviour, Result, ShrExError};
use crate::p2p::utils::OneshotSender;
use crate::peer_tracker::PeerTracker;

const MAX_PEERS: usize = 10;
const SCHEDULE_PENDING_INTERVAL: Duration = Duration::from_millis(100);

pub(super) struct Client {
    // WARNING!: When you add a new `ClientEndpoint` here, you need to adjust the
    // following methods:
    //
    // * Client::has_pending_requests
    // * Client::schedule_pending_requests
    // * Client::on_stop
    // * shrex::Behaviour::on_to_swarm
    pub(super) row: ClientEndpoint<RowId, Row>,
    pub(super) sample: ClientEndpoint<SampleId, Sample>,
    pub(super) nd: ClientEndpoint<NamespaceDataId, NamespaceData>,
    pub(super) eds: ClientEndpoint<EdsId, ExtendedDataSquare>,
    schedule_pending_interval: Option<Interval>,
}

impl Client {
    pub(super) fn new() -> Client {
        Client {
            row: ClientEndpoint::new(),
            sample: ClientEndpoint::new(),
            nd: ClientEndpoint::new(),
            eds: ClientEndpoint::new(),
            schedule_pending_interval: None,
        }
    }

    fn has_pending_requests(&self) -> bool {
        self.row.has_pending_requests()
            || self.sample.has_pending_requests()
            || self.nd.has_pending_requests()
            || self.eds.has_pending_requests()
    }

    pub(super) fn schedule_pending_requests(
        &mut self,
        behaviour: &mut InnerBehaviour,
        peer_tracker: &PeerTracker,
    ) {
        self.row
            .schedule_pending_requests(&mut behaviour.row_req_resp, peer_tracker);
        self.sample
            .schedule_pending_requests(&mut behaviour.sample_req_resp, peer_tracker);
        self.nd
            .schedule_pending_requests(&mut behaviour.nd_req_resp, peer_tracker);
        self.eds
            .schedule_pending_requests(&mut behaviour.nd_req_resp, peer_tracker);
    }

    pub(super) fn on_stop(&mut self) {
        self.row.on_stop();
        self.sample.on_stop();
        self.nd.on_stop();
        self.eds.on_stop();
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Event> {
        // If we have pending requests then initialize interval.
        //
        // We use this mechanism to give some buffer for more requests to
        // be accumulated and avoid calling `schedule_pending_requests` on
        // each iteration.
        if self.schedule_pending_interval.is_none() && self.has_pending_requests() {
            self.schedule_pending_interval = Some(Interval::new(SCHEDULE_PENDING_INTERVAL));
        }

        if let Some(interval) = self.schedule_pending_interval.as_mut()
            && interval.poll_tick(cx).is_ready()
        {
            return Poll::Ready(Event::SchedulePendingRequests);
        }

        Poll::Pending
    }
}

struct State<TReq, TResp> {
    req: TReq,
    respond_to: OneshotSender<TResp>,
}

pub(super) struct ClientEndpoint<TReq, TResp>
where
    TReq: RequestCodec + Clone,
    TResp: ResponseCodec<Request = TReq>,
{
    cancellation_token: CancellationToken,
    reqs: HashMap<OutboundRequestId, State<TReq, TResp>>,
    pending_reqs: VecDeque<State<TReq, TResp>>,
}

impl<TReq, TResp> ClientEndpoint<TReq, TResp>
where
    TReq: RequestCodec + Clone,
    TResp: ResponseCodec<Request = TReq>,
{
    pub(super) fn new() -> Self {
        Self {
            cancellation_token: CancellationToken::new(),
            reqs: HashMap::new(),
            pending_reqs: VecDeque::new(),
        }
    }
}

impl<TReq, TResp> ClientEndpointHandler for ClientEndpoint<TReq, TResp>
where
    TReq: RequestCodec + Clone,
    TResp: ResponseCodec<Request = TReq>,
{
    type TReq = TReq;
    type TResp = TResp;

    fn send_request(&mut self, req: TReq, respond_to: oneshot::Sender<Result<TResp, P2pError>>) {
        let respond_to = OneshotSender::new(respond_to, ShrExError::RequestCancelled);

        if self.cancellation_token.is_cancelled() {
            return;
        }

        self.pending_reqs.push_back(State { req, respond_to });
    }

    fn on_response(&mut self, peer_id: PeerId, request_id: OutboundRequestId, response: Response) {
        let Some(mut state) = self.reqs.remove(&request_id) else {
            return;
        };

        match response {
            Response::Ok(raw_data) => {
                let resp = TResp::decode_and_verify(&raw_data, &state.req, todo!()).expect("todo");
                state.respond_to.maybe_send_ok(resp);
            }
            _ => todo!(),
        }
    }

    fn on_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    ) {
        let Some(mut state) = self.reqs.remove(&request_id) else {
            return;
        };

        state
            .respond_to
            .maybe_send_err(ShrExError::OutboundFailure(error));
    }

    fn on_stop(&mut self) {
        self.cancellation_token.cancel();
        self.reqs.clear();
        self.pending_reqs.clear();
    }

    fn has_pending_requests(&self) -> bool {
        !self.pending_reqs.is_empty()
    }

    fn schedule_pending_requests(
        &mut self,
        sender: &mut request_response::Behaviour<ShrexBytesCodec>,
        peer_tracker: &PeerTracker,
    ) {
        if self.pending_reqs.is_empty() {
            return;
        }

        // TODO: we can do this in the caller
        let mut peers = peer_tracker
            .peers()
            .filter(|peer| peer.is_full()) // TODO
            .collect::<Vec<_>>();

        if !peers.is_empty() {
            // TODO: We can add a parameter for what kind of sorting we want for the peers.
            // For example we can sort by peer scoring or by ping latency etc.
            peers.shuffle(&mut rand::thread_rng());
            peers.truncate(MAX_PEERS);

            for (i, mut state) in self
                .pending_reqs
                .drain(..)
                // We filter before enumerate, just for keeping `i` correct
                .filter(|state| !state.respond_to.is_closed())
                .enumerate()
            {
                // Choose different peer for each request
                let peer = peers[i % peers.len()];
                let req_id = sender.send_request(peer.id(), state.req.encode());
                self.reqs.insert(req_id, state);
            }
        }
    }
}

pub(super) trait ClientEndpointHandler {
    type TReq;
    type TResp;

    fn on_event(
        &mut self,
        ev: request_response::Event<Vec<u8>, Response>,
    ) -> Option<request_response::Event<Vec<u8>, Response>> {
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

    fn send_request(
        &mut self,
        req: Self::TReq,
        respond_to: oneshot::Sender<Result<Self::TResp, P2pError>>,
    );

    fn on_response(&mut self, peer_id: PeerId, request_id: OutboundRequestId, response: Response);

    fn on_outbound_failure(
        &mut self,
        peer_id: PeerId,
        request_id: OutboundRequestId,
        error: OutboundFailure,
    );

    fn on_stop(&mut self);

    fn has_pending_requests(&self) -> bool;

    fn schedule_pending_requests(
        &mut self,
        sender: &mut request_response::Behaviour<ShrexBytesCodec>,
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
