// TODO: remove this
#![allow(unused)]

use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::ExtendedHeader;
use celestia_types::row::{EDS_ID_SIZE, EdsId, ROW_ID_SIZE, Row, RowId};
use celestia_types::sample::{Sample, SampleId};

use crate::p2p::shrex::{Result, ShrExError};

pub(crate) trait RequestCodec: Send + Sized {
    fn is_valid(&self) -> bool;
    fn encode(&self) -> Vec<u8>;
    fn decode(raw_data: &[u8]) -> Result<Self>;
}

pub(crate) trait ResponseCodec: Send + Sized {
    type Request;

    fn encode(&self) -> Vec<u8>;

    fn decode_and_verify(
        raw_data: &[u8],
        req: &Self::Request,
        header: &ExtendedHeader,
    ) -> Result<Self>;
}

impl RequestCodec for RowId {
    fn is_valid(&self) -> bool {
        todo!();
    }

    fn encode(&self) -> Vec<u8> {
        todo!();
    }

    fn decode(raw_data: &[u8]) -> Result<RowId> {
        todo!();
    }
}

impl ResponseCodec for Row {
    type Request = RowId;

    fn encode(&self) -> Vec<u8> {
        todo!();
    }

    fn decode_and_verify(raw_data: &[u8], req: &RowId, header: &ExtendedHeader) -> Result<Row> {
        todo!();
    }
}

impl RequestCodec for SampleId {
    fn is_valid(&self) -> bool {
        todo!();
    }

    fn encode(&self) -> Vec<u8> {
        todo!();
    }

    fn decode(raw_data: &[u8]) -> Result<SampleId> {
        todo!();
    }
}

impl ResponseCodec for Sample {
    type Request = SampleId;

    fn encode(&self) -> Vec<u8> {
        todo!();
    }

    fn decode_and_verify(
        raw_data: &[u8],
        req: &SampleId,
        header: &ExtendedHeader,
    ) -> Result<Sample> {
        todo!();
    }
}
