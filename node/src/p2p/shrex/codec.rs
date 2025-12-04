// TODO: remove this
#![allow(unused)]

use std::io;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use celestia_proto::shwap::{Row as RawRow, Share as RawShare};
use celestia_types::row::{ROW_ID_SIZE, Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use futures::{AsyncRead, AsyncWrite};
use libp2p::StreamProtocol;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use std::marker::PhantomData;

use crate::utils::{parse_protocol_id, protocol_id, read_up_to};

trait ByteCodec: Send {
    const MAX_SIZE: usize;
    const TIMEOUT: Duration;

    fn encode(&self) -> Vec<u8>;
    fn decode(data: &[u8]) -> io::Result<Self>
    where
        Self: Sized;
}

impl ByteCodec for RowId {
    const MAX_SIZE: usize = ROW_ID_SIZE;
    const TIMEOUT: Duration = Duration::from_secs(1);

    fn encode(&self) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        self.encode(&mut bytes);
        bytes.into()
    }

    fn decode(data: &[u8]) -> io::Result<RowId> {
        RowId::decode(data).map_err(io::Error::other)
    }
}

impl ByteCodec for RawRow {
    const MAX_SIZE: usize = 0;
    const TIMEOUT: Duration = Duration::from_secs(1);

    fn encode(&self) -> Vec<u8> {
        todo!();
    }

    fn decode(data: &[u8]) -> io::Result<Self>
    where
        Self: Sized,
    {
        todo!();
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReqRespCodec<TReq, TResp> {
    _phantom_req: PhantomData<TReq>,
    _phantom_resp: PhantomData<TResp>,
}

impl<TReq, TResp> Default for ReqRespCodec<TReq, TResp> {
    fn default() -> Self {
        ReqRespCodec {
            _phantom_req: PhantomData,
            _phantom_resp: PhantomData,
        }
    }
}

#[async_trait]
impl<TReq, TResp> Codec for ReqRespCodec<TReq, TResp>
where
    TReq: ByteCodec + Send,
    TResp: ByteCodec + Send,
{
    type Protocol = StreamProtocol;
    type Request = TReq;
    type Response = TResp;

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let data = read_up_to(io, TReq::MAX_SIZE, TReq::TIMEOUT).await?;

        todo!();
        //unreachable!("
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!()
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _resps: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RowCodec;

#[async_trait]
impl Codec for RowCodec {
    type Protocol = StreamProtocol;
    type Request = RowId;
    type Response = RawRow;

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!();
        //unreachable!("
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!()
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _resps: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SampleCodec;

#[async_trait]
impl Codec for SampleCodec {
    type Protocol = StreamProtocol;
    type Request = SampleId;
    type Response = Sample;

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!();
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!()
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _resps: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }
}
