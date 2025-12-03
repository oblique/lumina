pub(super) struct Client {
    row_reqs: HashMap<OutboundRequestId, RowReq>,
    sample_reqs: HashMap<OutboundRequestId, SampleReq>,
}

struct RowReq {}

struct SampleReq {}

impl Client {
    pub(super) fn new() -> Client {
        Client {}
    }

    pub(super) fn on_row_request(&mut self, row_id: RowId) {
        //
    }

    pub(super) fn on_row_response(&mut self, request_id: OutboundRequestId, row: Row) {
        //
    }

    pub(super) fn on_row_outbound_failure(&mut self, request_id: OutboundRequestId) {
        //
    }

    pub(super) fn on_sample_request(&mut self, row_id: SampleId) {
        //
    }

    pub(super) fn on_sample_response(&mut self, request_id: OutboundRequestId, row: SampleId) {
        //
    }

    pub(super) fn on_sample_outbound_failure(&mut self, request_id: OutboundRequestId) {
        //
    }
}
