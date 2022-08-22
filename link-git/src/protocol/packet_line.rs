use std::{borrow::Cow, io::Write, ops::RangeFrom};

use nom::{
    IResult,
    error::ParseError, InputIter, AsChar
};

#[derive(Debug)]
pub enum PacketLine<'a> {
    Data(Cow<'a, [u8]>),
    Flush,
}

impl<'a> PacketLine<'a> {

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Data(d) => {
                write!(&mut out, "{:04}", d.len()).unwrap();
            },
            Self::Flush => {
                write!(&mut out, "0000").unwrap();
            }
        }
        out
    }

    pub fn parse<E: ParseError<&'a [u8]>>(input: &'a [u8]) -> IResult<&'a [u8], PacketLine, E> 
    {
        let (i, len) = len_prefix(input)?;
        if len == 0 {
            Ok((i, PacketLine::Flush))
        } else {
            let (i, payload) = nom::bytes::streaming::take(len)(i)?;
            Ok((i, PacketLine::Data(payload.into())))
        }
    }

    pub fn into_owned(self) -> PacketLine<'static> {
        match self {
            Self::Data(p) => PacketLine::Data(Cow::Owned(p.into_owned())),
            Self::Flush => PacketLine::Flush,
        }
    }

}

fn len_prefix<'a, I, E: ParseError<I>>(input: I) -> IResult<I, u16, E> 
where
    I: nom::Slice<RangeFrom<usize>> + InputIter + Clone + PartialEq,
    I::Item: AsChar + Copy,
    &'static str: nom::FindToken<I::Item>,
{
    let hex_digit = nom::character::streaming::one_of("abcdefABCDEF0123456789");
    let (i, prefix) = nom::multi::count(hex_digit, 4)(input)?;

    let res = prefix
        .iter()
        .rev()
        .enumerate()
        .map(|(k, &v)| {
            let digit = v as char;
            (digit.to_digit(16).unwrap_or(0) as u16) << (k * 4)
        })
    .sum();

    Ok((i, res))
}

