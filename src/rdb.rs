use nom::branch::alt;
use nom::bytes::complete::{tag, take};
use nom::combinator::peek;
use nom::multi::{many_till, many0};
use nom::{Err, HexDisplay, IResult, Parser};

#[derive(Debug)]
struct Rdb {
    header: Header,
    metadata: Vec<KeyValue>,
}

#[derive(Debug)]
struct Header {
    magic: String,
    version: String,
}

#[derive(Debug)]
struct KeyValue {
    key: String,
    value: String,
}

#[derive(Debug, PartialEq)]
enum LengthOrEncoding {
    Length(u32),
    Encoding(u8),
}

fn magic(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("REDIS")(i)
}

fn version(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take(4usize)(i)
}

fn header(i: &[u8]) -> IResult<&[u8], Header> {
    let (i, magic) = magic(i)?;
    let (i, version) = version(i)?;
    let magic = String::from_utf8_lossy(magic).into_owned();
    let version = String::from_utf8_lossy(version).into_owned();
    Ok((i, Header { magic, version }))
}

fn length_or_encoding(i: &[u8]) -> IResult<&[u8], LengthOrEncoding> {
    let (_, first_byte) = peek(take(1usize)).parse(i)?;
    let type_bits = first_byte[0] >> 6;

    match type_bits {
        0b00..=0b10 => {
            let (i, len) = length(i)?;
            Ok((i, LengthOrEncoding::Length(len)))
        }
        0b11 => {
            let (i, byte) = take(1usize)(i)?;
            Ok((i, LengthOrEncoding::Encoding(byte[0] & 0x3F)))
        }
        _ => unreachable!(),
    }
}

fn length(i: &[u8]) -> IResult<&[u8], u32> {
    let (i, first_byte) = take(1usize)(i)?;
    let len_type = first_byte[0] >> 6;
    match len_type {
        // length is the remaining 6 bits of this byte
        0b00 => {
            let len = (first_byte[0] & 0x3F) as u32;
            Ok((i, len))
        }
        // Length is the remaining 6 bits of this byte, combined with the next byte
        0b01 => {
            let (i, next_byte) = take(1usize)(i)?;
            let len = u16::from_be_bytes([first_byte[0] & 0x3F, next_byte[0]]) as u32;
            Ok((i, len))
        }
        // Ignore the first byte. The length is the next 4 bytes, in big-endian.
        0b10 => {
            let (i, len_bytes) = take(4usize)(i)?;
            let len = u32::from_be_bytes(len_bytes.try_into().unwrap());
            Ok((i, len))
        }
        _ => {
            dbg!("oh no ", len_type);
            unreachable!()
        }
    }
}

fn string_encoded(i: &[u8], encoding: u8) -> IResult<&[u8], String> {
    // TODO: just return bytes
    match encoding {
        0b00 => {
            // 8 bit integer
            let (i, next_byte) = take(1usize)(i)?;
            let val = u8::from_be_bytes(next_byte.try_into().unwrap());
            Ok((i, val.to_string()))
        }
        0b01 => {
            let (i, next_bytes) = take(2usize)(i)?;
            let val = u16::from_le_bytes(next_bytes.try_into().unwrap());
            Ok((i, val.to_string()))
        }
        0b10 => {
            let (i, next_bytes) = take(4usize)(i)?;
            let val = u32::from_le_bytes(next_bytes.try_into().unwrap());
            Ok((i, val.to_string()))
        }
        0b11 => {
            // Compressed with LZF algo
            panic!("Can't handle LZF strings")
        }
        _ => panic!("Unknown encoding"),
    }
}

fn encoded_value(i: &[u8]) -> IResult<&[u8], String> {
    match length_or_encoding(i)? {
        (i, LengthOrEncoding::Length(length)) => {
            let (i, string) = take(length)(i)?;
            let string = String::from_utf8_lossy(string).into_owned();
            Ok((i, string))
        }
        (i, LengthOrEncoding::Encoding(encoding)) => {
            let (i, string) = string_encoded(i, encoding)?;
            Ok((i, string))
        }
    }
}

fn metadata_start(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let delim: &[u8] = &[0xFA];
    tag(delim)(i)
}

fn metadata(i: &[u8]) -> IResult<&[u8], KeyValue> {
    let (i, _) = metadata_start(i)?;
    let (i, key) = encoded_value(i)?;
    let (i, value) = encoded_value(i)?;
    Ok((i, KeyValue { key, value }))
}

fn database_start(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let delim: &[u8] = &[0xFE];
    tag(delim)(i)
}

fn eof_marker(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let delim: &[u8] = &[0xFF];
    tag(delim)(i)
}

fn checksum(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take(8usize)(i)
}

struct DatabaseHeader {
    index: u32,
    size: u32,
    expire_size: u32,
}

fn database_header(i: &[u8]) -> IResult<&[u8], DatabaseHeader> {
    let (i, index) = length(i)?;
    let hash_table_delim: &[u8] = &[0xFB];
    let (i, _) = tag(hash_table_delim)(i)?;
    let (i, size) = length(i)?;
    let (i, expire_size) = length(i)?;
    Ok((
        i,
        DatabaseHeader {
            index,
            size,
            expire_size,
        },
    ))
}

fn parse_rdb(i: &[u8]) -> IResult<&[u8], Rdb> {
    let (i, header) = header(i)?;
    let (i, (metadata, _)) = many_till(metadata, alt((database_start, eof_marker))).parse(i)?;
    Ok((i, Rdb { header, metadata }))
}

#[cfg(test)]
mod tests {
    use super::*;
    const EMPTY_DB: &str = "524544495330303132fa0972656469732d76657205382e342e30fa0a7265\
    6469732d62697473c040fa056374696d65c27f656169fa08757365642d6d\
    656dc280f41000fa08616f662d62617365c000ff4635ae29d917db65";

    const FOOBAR_DB: &str = "524544495330303132fa0972656469732d76657205382e342e30fa0a7265\
    6469732d62697473c040fa056374696d65c265c56269fa08757365642d6d\
    656dc2a0bf1100fa08616f662d62617365c000fe00fb01000003666f6f03\
    626172ff857a541ff5ce9d9c";

    #[test]
    fn test_foobar_db() {
        let b = foobar_db_bytes();
        assert_eq!(b.len(), 102);
        let (_, rdb) = parse_rdb(&b).unwrap();
        assert_eq!(rdb.header.magic, "REDIS");
        assert_eq!(rdb.header.version, "0012");
        assert_eq!(rdb.metadata.len(), 5);
    }

    #[test]
    fn test_db_header() {
        let db_header = "00FB0302";
        let db_header_bytes = hex::decode(db_header).unwrap();
        let (_, header) = database_header(&db_header_bytes).unwrap();
        assert_eq!(header.index, 0);
        assert_eq!(header.size, 3);
        assert_eq!(header.expire_size, 2);
    }

    fn empty_db_bytes() -> Vec<u8> {
        hex::decode(EMPTY_DB).unwrap()
    }

    fn foobar_db_bytes() -> Vec<u8> {
        hex::decode(FOOBAR_DB).unwrap()
    }

    #[test]
    fn test_magic() {
        let db = empty_db_bytes();
        let (_, magic) = magic(&db).unwrap();
        assert_eq!(magic, &b"REDIS"[..]);
    }

    #[test]
    fn test_version() {
        let db = empty_db_bytes();
        let (i, _) = magic(&db).unwrap();
        let (_, version) = version(i).unwrap();
        assert_eq!(version, &b"0012"[..]);
    }

    #[test]
    fn test_header() {
        let db = empty_db_bytes();
        let result = header(&db).unwrap();
        let (_, header) = result;
        assert_eq!(header.magic, "REDIS");
        assert_eq!(header.version, "0012");
    }

    #[test]
    fn test_length() {
        let zero: &[u8] = &[0x0A];
        let (_, result) = length(zero).unwrap();
        assert_eq!(result, 10);

        let one: &[u8] = &[0x42, 0xBC];
        let (_, result) = length(one).unwrap();
        assert_eq!(result, 700);

        let two: &[u8] = &[0x80, 0x00, 0x00, 0x42, 0x68];
        let (_, result) = length(two).unwrap();
        assert_eq!(result, 17000);
    }

    #[test]
    fn test_encoded_values() {
        let hello_world = "0D48656C6C6F2C20576F726C6421";
        let hello_world_bytes = hex::decode(hello_world).unwrap();
        let (_, result) = encoded_value(&hello_world_bytes).unwrap();
        assert_eq!(result, "Hello, World!".to_string());

        let onetwothree = "C07B";
        let onetwothree_bytes = hex::decode(onetwothree).unwrap();
        let (_, result) = encoded_value(&onetwothree_bytes).unwrap();
        assert_eq!(result, "123".to_string());

        let one2five = "C13930";
        let one2five_bytes = hex::decode(one2five).unwrap();
        let (_, result) = encoded_value(&one2five_bytes).unwrap();
        assert_eq!(result, "12345".to_string());

        let one2seven = "C287D61200";
        let one2seven_bytes = hex::decode(one2seven).unwrap();
        let (_, result) = encoded_value(&one2seven_bytes).unwrap();
        assert_eq!(result, "1234567".to_string());
    }

    #[test]
    fn test_metadata() {
        // Key
        // Value
        let md_hex = "FA\
            0972656469732D766572\
            06362E302E3136";
        let md_bytes = hex::decode(md_hex).unwrap();
        let (_, metadata) = metadata(&md_bytes).unwrap();
        assert_eq!(metadata.key, "redis-ver");
        assert_eq!(metadata.value, "6.0.16");
    }

    #[test]
    fn test_empty_file() {
        let b = empty_db_bytes();
        assert_eq!(b.len(), 88);
        let result = parse_rdb(&b);
        let (_, rdb) = result.unwrap();
        assert_eq!(rdb.header.magic, "REDIS");
        assert_eq!(rdb.header.version, "0012");
        assert_eq!(rdb.metadata.len(), 5);
        let metadata = &rdb.metadata[0];
        assert_eq!(metadata.key, "redis-ver");
        assert_eq!(metadata.value, "8.4.0");

        let metadata = &rdb.metadata[1];
        assert_eq!(metadata.key, "redis-bits");
        assert_eq!(metadata.value, "64");

        let metadata = &rdb.metadata[2];
        assert_eq!(metadata.key, "ctime");
        assert_eq!(metadata.value, "1767990655");

        let metadata = &rdb.metadata[3];
        assert_eq!(metadata.key, "used-mem");
        assert_eq!(metadata.value, "1111168");

        let metadata = &rdb.metadata[4];
        assert_eq!(metadata.key, "aof-base");
        assert_eq!(metadata.value, "0");
    }
}
