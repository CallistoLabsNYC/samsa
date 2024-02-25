//! Deserialize data from the bytecode protocol.
use bytes::Bytes;
use nom::{
    bytes::complete::take,
    combinator::map,
    error::{ErrorKind, ParseError},
    multi::many_m_n,
    number::complete::{be_i16, be_i32, be_u16, be_u32},
    Err::*,
    IResult,
    Needed::Unknown,
};
use nombytes::NomBytes;
use num_traits::FromPrimitive;

use crate::error::KafkaCode;

pub fn parse_kafka_code(s: NomBytes) -> IResult<NomBytes, KafkaCode> {
    map(be_i16, |n| {
        FromPrimitive::from_i16(n).unwrap_or(KafkaCode::Unknown)
    })(s)
}

pub fn take_varint<E>(i: NomBytes) -> nom::IResult<NomBytes, usize, E>
where
    E: ParseError<NomBytes>,
{
    let mut res: usize = 0;
    let mut count: usize = 0;
    let mut remainder = i;
    loop {
        let byte = match take::<usize, NomBytes, ()>(1)(remainder) {
            Ok((rest, bytes)) => {
                remainder = rest;
                let bytes = bytes.to_bytes();
                bytes.first().cloned().unwrap()
            }
            Err(_) => return Err(Incomplete(Unknown)),
        };
        res += ((byte as usize) & 127)
            .checked_shl((count * 7).try_into().unwrap_or(u32::MAX))
            .ok_or_else(|| Error(E::from_error_kind(remainder.clone(), ErrorKind::MapOpt)))?;
        count += 1;
        if (byte >> 7) == 0 {
            return Ok((remainder, res));
        }
    }
}

pub fn parse_string(s: NomBytes) -> IResult<NomBytes, Bytes> {
    let (s, length) = be_u16(s)?;
    let (s, string) = take(length)(s)?;
    Ok((s, string.into_bytes()))
}

pub fn parse_bytes(s: NomBytes) -> IResult<NomBytes, Bytes> {
    let (s, length) = be_u32(s)?;
    let (s, string) = take(length)(s)?;
    Ok((s, string.into_bytes()))
}

pub fn parse_array<O, E, F>(f: F) -> impl FnMut(NomBytes) -> IResult<NomBytes, Vec<O>, E>
where
    F: nom::Parser<NomBytes, O, E> + Copy,
    E: nom::error::ParseError<NomBytes>,
{
    move |input: NomBytes| {
        let i = input.clone();
        let (i, length) = be_i32(i)?;
        if length == -1 {
            return Ok((i, vec![]));
        }
        many_m_n(length as usize, length as usize, f)(i)
    }
}

pub fn parse_nullable_string(s: NomBytes) -> IResult<NomBytes, Option<Bytes>> {
    let (s, length) = be_i16(s)?;
    if length == -1 {
        return Ok((s, None));
    }

    let (s, string) = take(length as u16)(s)?;
    Ok((s, Some(string.into_bytes())))
}

pub fn parse_nullable_bytes(s: NomBytes) -> IResult<NomBytes, Option<Bytes>> {
    let (s, length) = be_i32(s)?;
    if length == -1 {
        return Ok((s, None));
    }

    let (s, bytes) = take(length as u32)(s)?;
    Ok((s, Some(bytes.into_bytes())))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_varint_simple() {
        assert_eq!(
            take_varint::<()>(NomBytes::from(b"\x0b\x01\x02\x03" as &[u8])),
            Ok((NomBytes::from(b"\x01\x02\x03" as &[u8]), 11))
        );
    }

    #[test]
    fn parse_varint_twobyte() {
        assert_eq!(
            take_varint::<()>(NomBytes::from(b"\x84\x02\x04\x05\x06" as &[u8])),
            Ok((NomBytes::from(b"\x04\x05\x06" as &[u8]), 260))
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn parse_varlong() {
        assert_eq!(
            take_varint::<()>(NomBytes::from(
                b"\xff\xff\xff\xff\xff\xff\xff\xff\x7f\x04\x05\x06" as &[u8]
            )),
            Ok((
                NomBytes::from(b"\x04\x05\x06" as &[u8]),
                9223372036854775807
            ))
        );
    }

    #[test]
    fn test_parse_string() {
        let buf = NomBytes::from(b"\x00\x04\x72\x75\x73\x74" as &[u8]);

        assert_eq!(
            parse_string(buf).unwrap().1,
            NomBytes::from(b"\x72\x75\x73\x74" as &[u8]).to_bytes()
        );
    }

    #[test]
    fn test_parse_array() {
        let buf = NomBytes::from(
            [
                0, 0, 0, 2, // array size
                0, 4, 114, 117, 115, 116, // string
                0, 4, 114, 117, 115, 116, // string
                0, 0, 0, // leftover input
            ]
            .as_slice(),
        );

        assert_eq!(
            parse_array(parse_string)(buf).unwrap().1,
            vec![String::from("rust"), String::from("rust")]
        );
    }
}
