use std::io;

use bytes::{Bytes, BytesMut};
use memchr::memchr;
use tokio_util::codec::{Decoder, Encoder};

const CFLR: &[u8; 2] = b"\r\n";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    String(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<Value>),
    NullArray,
    Boolean(bool),
}

enum ValueBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Integer(i64),
    BulkString(BufSplit),
    NullBulkString,
    Array(Vec<ValueBufSplit>),
    NullArray,
    Boolean(bool),
}

struct BufSplit(usize, usize);

impl BufSplit {
    #[inline(always)]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    #[inline(always)]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

impl ValueBufSplit {
    fn values(self, buf: &Bytes) -> Value {
        match self {
            ValueBufSplit::String(s) => Value::String(s.as_bytes(buf)),
            ValueBufSplit::Error(s) => Value::Error(s.as_bytes(buf)),
            ValueBufSplit::Integer(int) => Value::Integer(int),
            ValueBufSplit::BulkString(s) => Value::BulkString(s.as_bytes(buf)),
            ValueBufSplit::NullBulkString => Value::NullBulkString,
            ValueBufSplit::Array(arr) => {
                Value::Array(arr.into_iter().map(|b| b.values(buf)).collect())
            }
            ValueBufSplit::NullArray => Value::NullArray,
            ValueBufSplit::Boolean(boolean) => Value::Boolean(boolean),
        }
    }
}

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownLeadingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
}

impl From<io::Error> for RESPError {
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl Value {
    fn encode(&self, dst: &mut BytesMut) {
        match self {
            Value::String(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(s);
                dst.extend_from_slice(CFLR);
            }
            Value::Error(msg) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(msg);
                dst.extend_from_slice(CFLR);
            }
            Value::Integer(int) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(int.to_string().as_bytes());
                dst.extend_from_slice(CFLR);
            }
            Value::BulkString(s) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(s.len().to_string().as_bytes());
                dst.extend_from_slice(CFLR);
                dst.extend_from_slice(s);
                dst.extend_from_slice(CFLR);
            }
            Value::NullBulkString => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Value::Array(arr) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(arr.len().to_string().as_bytes());
                dst.extend_from_slice(CFLR);
                arr.iter().for_each(|e| e.encode(dst));
            }
            Value::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
            Value::Boolean(boolean) => {
                dst.extend_from_slice(if *boolean { b"#t\r\n" } else { b"#f\r\n" });
            }
        }
    }
}

type RedisResult = Result<Option<(usize, ValueBufSplit)>, RESPError>;

#[inline(always)]
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() < pos {
        return None;
    }
    memchr(b'\r', &buf[pos..]).and_then(|end| {
        if end + 1 < buf.len() {
            Some((pos + end + 2, BufSplit(pos, pos + end)))
        } else {
            None
        }
    })
}

fn _int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn simp_string(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, ValueBufSplit::String(word))))
}

fn simp_error(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, ValueBufSplit::Error(word))))
}

fn int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(_int(buf, pos)?.map(|(pos, int)| (pos, ValueBufSplit::Integer(int))))
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    match _int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, ValueBufSplit::NullBulkString))),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                Ok(Some((
                    total_size + 2,
                    ValueBufSplit::BulkString(BufSplit(pos, total_size)),
                )))
            }
        }
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    match _int(buf, pos)? {
        None => Ok(None),
        Some((pos, -1)) => Ok(Some((pos, ValueBufSplit::NullArray))),
        Some((pos, len)) if len >= 0 => {
            let mut values = Vec::with_capacity(len as _);
            let mut curr_pos = pos;
            for _ in 0..len {
                match parse(buf, curr_pos)? {
                    Some((new_pos, value)) => {
                        curr_pos = new_pos;
                        values.push(value);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((curr_pos, ValueBufSplit::Array(values))))
        }
        Some((_pos, bad_len)) => Err(RESPError::BadArraySize(bad_len)),
    }
}

fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => simp_string(buf, pos + 1),
        b'-' => simp_error(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b':' => int(buf, pos + 1),
        b'*' => array(buf, pos + 1),
        _ => Err(RESPError::UnknownLeadingByte),
    }
}

#[derive(Default)]
pub struct RespParser;

impl Decoder for RespParser {
    type Item = Value;
    type Error = RESPError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        match parse(src, 0)? {
            Some((pos, val)) => {
                let data = src.split_to(pos);
                Ok(Some(val.values(&data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<Value> for RespParser {
    type Error = io::Error;
    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod resp_parser_tests {
    use super::RespParser;
    use super::Value;
    use bytes::{Bytes, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    fn generic_test(input: &'static str, output: Value) {
        let mut decoder = RespParser;
        let result_read = decoder.decode(&mut BytesMut::from(input));

        let mut encoder = RespParser;
        let mut buf = BytesMut::new();
        let result_write = encoder.encode(output.clone(), &mut buf);

        assert!(
            result_write.as_ref().is_ok(),
            "{:?}",
            result_write.unwrap_err()
        );

        assert_eq!(input.as_bytes(), buf.as_ref());

        assert!(
            result_read.as_ref().is_ok(),
            "{:?}",
            result_read.unwrap_err()
        );
        // let values = result_read.unwrap().unwrap();

        // let generic_arr_test_case = vec![output.clone(), output.clone()];
        // let doubled = input.to_owned() + &input.to_owned();

        // assert_eq!(output, values);
        // generic_test_arr(&doubled, generic_arr_test_case)
    }

    fn generic_test_arr(input: &str, output: Vec<Value>) {
        // TODO: Try to make this occur randomly
        let first: usize = input.len() / 2;
        let second = input.len() - first;
        let mut first = BytesMut::from(&input[0..=first]);
        let mut second = Some(BytesMut::from(&input[second..]));

        let mut decoder = RespParser;
        let mut res: Vec<Value> = Vec::new();
        loop {
            match decoder.decode(&mut first) {
                Ok(Some(value)) => {
                    res.push(value);
                    break;
                }
                Ok(None) => {
                    if second.is_none() {
                        panic!("Test expected more bytes than expected!");
                    }
                    first.extend(second.unwrap());
                    second = None;
                }
                Err(e) => panic!("Should not error, {:?}", e),
            }
        }
        if let Some(second) = second {
            first.extend(second);
        }
        match decoder.decode(&mut first) {
            Ok(Some(value)) => {
                res.push(value);
            }
            Err(e) => panic!("Should not error, {:?}", e),
            _ => {}
        }
        assert_eq!(output, res);
    }

    fn ezs() -> Bytes {
        Bytes::from_static(b"hello")
    }

    // XXX: Simple String has been removed.
    // #[test]
    // fn test_simple_string() {
    //     let t = RedisValue::BulkString(ezs());
    //     let s = "+hello\r\n";
    //     generic_test(s, t);

    //     let t0 = RedisValue::BulkString(ezs());
    //     let t1 = RedisValue::BulkString("abcdefghijklmnopqrstuvwxyz".as_bytes().to_vec());
    //     let s = "+hello\r\n+abcdefghijklmnopqrstuvwxyz\r\n";
    //     generic_test_arr(s, vec![t0, t1]);
    // }

    #[test]
    fn test_error() {
        let t = Value::Error(ezs());
        let s = "-hello\r\n";
        generic_test(s, t);

        let t0 = Value::Error(Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz"));
        let t1 = Value::Error(ezs());
        let s = "-abcdefghijklmnopqrstuvwxyz\r\n-hello\r\n";
        generic_test_arr(s, vec![t0, t1]);
    }

    #[test]
    fn test_bulk_string() {
        let t = Value::BulkString(ezs());
        let s = "$5\r\nhello\r\n";
        generic_test(s, t);

        let t = Value::BulkString(Bytes::from_static(b""));
        let s = "$0\r\n\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_int() {
        let t = Value::Integer(0);
        let s = ":0\r\n";
        generic_test(s, t);

        let t = Value::Integer(123);
        let s = ":123\r\n";
        generic_test(s, t);

        let t = Value::Integer(-123);
        let s = ":-123\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_array() {
        let t = Value::Array(vec![]);
        let s = "*0\r\n";
        generic_test(s, t);

        let inner = vec![
            Value::BulkString(Bytes::from_static(b"foo")),
            Value::BulkString(Bytes::from_static(b"bar")),
        ];
        let t = Value::Array(inner);
        let s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let inner = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let t = Value::Array(inner);
        let s = "*3\r\n:1\r\n:2\r\n:3\r\n";
        generic_test(s, t);

        let inner = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::BulkString(Bytes::from_static(b"foobar")),
        ];
        let t = Value::Array(inner);
        let s = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n";
        generic_test(s, t);

        let inner = vec![
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
            Value::Array(vec![
                Value::BulkString(Bytes::from_static(b"Foo")),
                Value::Error(Bytes::from_static(b"Bar")),
            ]),
        ];
        let t = Value::Array(inner);
        let s = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n$3\r\nFoo\r\n-Bar\r\n";
        generic_test(s, t);

        let inner = vec![
            Value::BulkString(Bytes::from_static(b"foo")),
            Value::NullBulkString,
            Value::BulkString(Bytes::from_static(b"bar")),
        ];
        let t = Value::Array(inner);
        let s = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let t = Value::NullArray;
        let s = "*-1\r\n";
        generic_test(s, t);
    }
}
