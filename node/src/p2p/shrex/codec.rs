// TODO: remove this
#![allow(unused)]

use bytes::{Buf, BufMut, BytesMut};
use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::consts::appconsts::SHARE_SIZE;
use celestia_types::eds::{EdsId, ExtendedDataSquare};
use celestia_types::row::{ROW_ID_SIZE, Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use celestia_types::{DataAvailabilityHeader, ExtendedHeader};
use integer_encoding::VarInt;
use prost::Message;

use crate::p2p::shrex::{Result, ShrExError};

pub(crate) trait RequestCodec: Send + Sized {
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
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        self.encode(&mut bytes);
        bytes.into()
    }

    fn decode(raw_data: &[u8]) -> Result<RowId> {
        RowId::decode(raw_data).map_err(ShrExError::request_decode_failed)
    }
}

impl ResponseCodec for Row {
    type Request = RowId;

    fn encode(&self) -> Vec<u8> {
        let raw = RawRow::from(self.clone());
        raw.encode_length_delimited_to_vec()
    }

    fn decode_and_verify(raw_data: &[u8], req: &RowId, header: &ExtendedHeader) -> Result<Row> {
        let raw_row = RawRow::decode_length_delimited(raw_data)
            .map_err(ShrExError::response_decode_failed)?;

        let row =
            Row::from_raw(req.to_owned(), raw_row).map_err(ShrExError::response_decode_failed)?;

        row.verify(req.to_owned(), &header.dah)
            .map_err(ShrExError::ResponseVerificationFailed)?;

        Ok(row)
    }
}

impl RequestCodec for SampleId {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        self.encode(&mut bytes);
        bytes.into()
    }

    fn decode(raw_data: &[u8]) -> Result<SampleId> {
        SampleId::decode(raw_data).map_err(ShrExError::request_decode_failed)
    }
}

impl ResponseCodec for Sample {
    type Request = SampleId;

    fn encode(&self) -> Vec<u8> {
        let raw = RawSample::from(self.clone());
        raw.encode_length_delimited_to_vec()
    }

    fn decode_and_verify(
        raw_data: &[u8],
        req: &SampleId,
        header: &ExtendedHeader,
    ) -> Result<Sample> {
        let raw_sample = RawSample::decode_length_delimited(raw_data)
            .map_err(ShrExError::response_decode_failed)?;

        let sample = Sample::from_raw(req.to_owned(), raw_sample)
            .map_err(ShrExError::response_decode_failed)?;

        sample
            .verify(req.to_owned(), &header.dah)
            .map_err(ShrExError::ResponseVerificationFailed)?;

        Ok(sample)
    }
}

impl RequestCodec for EdsId {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        self.encode(&mut bytes);
        bytes.into()
    }

    fn decode(raw_data: &[u8]) -> Result<EdsId> {
        EdsId::decode(raw_data).map_err(ShrExError::request_decode_failed)
    }
}

impl ResponseCodec for ExtendedDataSquare {
    type Request = EdsId;

    fn encode(&self) -> Vec<u8> {
        let ods_width = self.square_width() / 2;
        let mut bytes =
            BytesMut::with_capacity(usize::from(ods_width) * usize::from(ods_width) * SHARE_SIZE);

        for row in 0..ods_width {
            for col in 0..ods_width {
                let share = self.share(row, col).expect("Invalid square_width");
                debug_assert!(!share.is_parity());
                bytes.put_slice(&share.data()[..]);
            }
        }

        bytes.into()
    }

    fn decode_and_verify(
        raw_data: &[u8],
        req: &EdsId,
        header: &ExtendedHeader,
    ) -> Result<ExtendedDataSquare> {
        if raw_data.len() == 0 {
            return Err(ShrExError::response_decode_failed("Empty raw data"));
        }

        if raw_data.len() % SHARE_SIZE != 0 {
            return Err(ShrExError::response_decode_failed(
                "Number of shares not divisible by SHARE_SIZE",
            ));
        }

        let mut ods_shares = Vec::new();

        for raw_share in raw_data.chunks(SHARE_SIZE) {
            ods_shares.push(raw_share.to_vec());
        }

        let app_version = header
            .app_version()
            .map_err(ShrExError::response_decode_failed)?;

        let eds = ExtendedDataSquare::from_ods(ods_shares, app_version)
            .map_err(ShrExError::response_decode_failed)?;

        let dah = DataAvailabilityHeader::from_eds(&eds);

        if dah.hash() != header.dah.hash() {
            return Err(ShrExError::response_decode_failed(
                "EDS verification failed",
            ));
        }

        Ok(eds)
    }
}
