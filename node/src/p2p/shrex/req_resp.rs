// TODO: remove this
#![allow(unused)]

use std::io;
use std::marker::PhantomData;
use std::task::{Context, Waker};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use celestia_proto::shwap::{Row as RawRow, Sample as RawSample};
use celestia_types::eds::RawExtendedDataSquare;
use celestia_types::row::{ROW_ID_SIZE, Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use integer_encoding::VarInt;
use libp2p::StreamProtocol;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use prost::Message;
use prost::UnknownEnumValue;

// TODO: fix this in celestia-node repo
use celestia_proto::{Response as ProtoResponse, Status as ProtoStatus};

use crate::utils::{parse_protocol_id, protocol_id, read_up_to};

const REQ_MAX_SIZE: usize = 1;
const REQ_TIMEOUT: Duration = Duration::from_secs(1);
const RESP_MAX_SIZE: usize = 1;
const RESP_TIMEOUT: Duration = Duration::from_secs(10);
const STATUS_MAX_SIZE: usize = 16;

#[derive(Debug, Clone, Default)]
pub(crate) struct ShrexBytesCodec;

#[derive(Debug)]
pub(crate) enum Response {
    Invalid(i32),
    Ok(Vec<u8>),
    NotFound,
    InternalError,
}

#[async_trait]
impl Codec for ShrexBytesCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Response;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Vec<u8>>
    where
        T: AsyncRead + Unpin + Send,
    {
        // We try to read 1 bytes more in order to check if server send us more
        // than the max size.
        let (data, timed_out) = read_up_to(io, REQ_MAX_SIZE + 1, REQ_TIMEOUT).await?;

        if timed_out {
            return Err(io::Error::other("shrex: read_request timed out"));
        }

        if data.len() > REQ_MAX_SIZE {
            return Err(io::Error::other(
                "shrex: read_request received more data than max size",
            ));
        }

        Ok(data)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // TODO: add timeout
        let status = read_status(io).await?;

        match ProtoStatus::try_from(status) {
            Ok(ProtoStatus::Ok) => {
                // We try to read 1 bytes more in order to check if server send us more
                // than the max size.
                let (data, timed_out) = read_up_to(io, REQ_MAX_SIZE + 1, REQ_TIMEOUT).await?;

                if timed_out {
                    return Err(io::Error::other("shrex: read_request timed out"));
                }

                if data.len() > REQ_MAX_SIZE {
                    return Err(io::Error::other(
                        "shrex: read_request received more data than max size",
                    ));
                }

                Ok(Response::Ok(data))
            }
            Ok(ProtoStatus::NotFound) => Ok(Response::NotFound),
            Ok(ProtoStatus::Internal) => Ok(Response::InternalError),
            Ok(ProtoStatus::Invalid) => Ok(Response::Invalid(ProtoStatus::Invalid as i32)),
            Err(UnknownEnumValue(val)) => Ok(Response::Invalid(val)),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Vec<u8>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&req).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        resp: Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let status = match &resp {
            Response::Invalid(_) => ProtoStatus::Invalid,
            Response::Ok(_) => ProtoStatus::Ok,
            Response::NotFound => ProtoStatus::NotFound,
            Response::InternalError => ProtoStatus::Internal,
        };

        write_status(io, status).await?;

        if let Response::Ok(data) = resp {
            io.write_all(&data).await?;
        }

        Ok(())
    }
}

async fn read_varint<T>(io: &mut T) -> io::Result<usize>
where
    T: AsyncRead + Unpin + Send,
{
    let mut buf = [0u8; 10];
    let mut len = 0;

    for _ in 0..buf.len() {
        io.read_exact(&mut buf[len..len + 1]).await?;

        if let Some((val, _)) = usize::decode_var(&buf[..len]) {
            return Ok(val);
        }
    }

    Err(io::Error::other("shrex: failed to read valid varint"))
}

async fn read_status<T>(io: &mut T) -> io::Result<i32>
where
    T: AsyncRead + Unpin + Send,
{
    let len = read_varint(io).await?;

    if len > STATUS_MAX_SIZE {
        let s = format!("shrex: status message bigger than STATUS_MAX_SIZE");
        return Err(io::Error::other(s));
    }

    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf[..]).await?;

    let resp = ProtoResponse::decode(&buf[..]).map_err(|e| {
        let s = format!("shrex: failed to decode `Response`: {e}");
        io::Error::other(s)
    })?;

    Ok(resp.status)
}

async fn write_status<T>(io: &mut T, status: ProtoStatus) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let resp = ProtoResponse {
        status: status as i32,
    };

    let data = resp.encode_to_vec();
    let varint = data.len().encode_var_vec();

    io.write_all(&varint).await?;
    io.write_all(&data).await?;

    Ok(())
}
