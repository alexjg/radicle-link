// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{io, marker::PhantomData};

use bytes::{
    buf::{BufExt, BufMutExt},
    BytesMut,
};
use futures_codec::{Decoder, Encoder};
use minicbor::{Decode, Encode};

#[derive(Clone, Copy, Default)]
pub struct LengthDelimitedCborCodec<Enc, Dec> {
    enc: PhantomData<Enc>,
    dec: PhantomData<Dec>,
}

impl<Enc, Dec> LengthDelimitedCborCodec<Enc, Dec> {
    pub fn new() -> Self {
        Self {
            enc: PhantomData,
            dec: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CborCodecError {
    #[error(transparent)]
    MinicborIo(#[from] minicbor_io::Error),

    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<Enc, Dec> Encoder for &LengthDelimitedCborCodec<Enc, Dec>
where
    Enc: Encode,
{
    type Item = Enc;
    type Error = CborCodecError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut writer = minicbor_io::Writer::new(dst.writer());
        writer.write(item)?;
        Ok(())
    }
}

impl<Enc, Dec> Decoder for &LengthDelimitedCborCodec<Enc, Dec>
where
    for<'b> Dec: Decode<'b>,
{
    type Item = Dec;
    type Error = CborCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut reader = minicbor_io::Reader::new(src.reader());
        reader
            .read::<'_, Self::Item>()
            .map_err(CborCodecError::from)
    }
}
