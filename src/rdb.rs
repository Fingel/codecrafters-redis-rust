use nom::branch::alt;
use nom::bytes::complete::{tag, take};
use nom::combinator::{opt, peek};
use nom::multi::{many_till, many0};
use nom::{IResult, Parser};

#[derive(Debug)]
pub struct Rdb {
    pub header: Header,
    pub metadata: Vec<KeyValue>,
    pub entries: Vec<DatabaseEntry>,
}

#[derive(Debug)]
pub struct Header {
    pub magic: String,
    pub version: String,
}

#[derive(Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct DatabaseEntry {
    pub kv: KeyValue,
    pub expire: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct DatabaseHeader {
    index: u32,
    size: u32,
    expire_size: u32,
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

fn database_value(i: &[u8]) -> IResult<&[u8], KeyValue> {
    let delim: &[u8] = &[0x00];
    let (i, _) = tag(delim)(i)?;
    let (i, key) = encoded_value(i)?;
    let (i, value) = encoded_value(i)?;
    Ok((i, KeyValue { key, value }))
}

fn entry_expire_ms(i: &[u8]) -> IResult<&[u8], DatabaseEntry> {
    let delim: &[u8] = &[0xFC];
    let (i, _) = tag(delim)(i)?;
    let (i, expire) = take(8usize)(i)?;
    let timestamp = u64::from_le_bytes(expire.try_into().unwrap());
    let (i, database_value) = database_value(i)?;
    Ok((
        i,
        DatabaseEntry {
            kv: database_value,
            expire: Some(timestamp),
        },
    ))
}

fn entry_expire_sec(i: &[u8]) -> IResult<&[u8], DatabaseEntry> {
    let delim: &[u8] = &[0xFD];
    let (i, _) = tag(delim)(i)?;
    let (i, expire) = take(4usize)(i)?;
    let timestamp = u32::from_le_bytes(expire.try_into().unwrap());
    let (i, database_value) = database_value(i)?;
    Ok((
        i,
        DatabaseEntry {
            kv: database_value,
            expire: Some(timestamp as u64),
        },
    ))
}

fn entry_no_expire(i: &[u8]) -> IResult<&[u8], DatabaseEntry> {
    let (i, database_value) = database_value(i)?;
    Ok((
        i,
        DatabaseEntry {
            kv: database_value,
            expire: None,
        },
    ))
}

pub fn parse_rdb(i: &[u8]) -> IResult<&[u8], Rdb> {
    let (i, header) = header(i)?;
    let (i, metadata) = many0(metadata).parse(i)?;
    let (i, db_header_opt) = opt(database_start).parse(i)?;
    let (i, entries) = if db_header_opt.is_some() {
        let (i, _db_header) = database_header(i)?;
        let (i, (entries, _)) = many_till(
            alt((entry_expire_ms, entry_expire_sec, entry_no_expire)),
            eof_marker,
        )
        .parse(i)?;
        (i, entries)
    } else {
        let (i, _) = eof_marker(i)?;
        (i, Vec::new())
    };
    let (i, _) = checksum(i)?;
    Ok((
        i,
        Rdb {
            header,
            metadata,
            entries,
        },
    ))
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

    const BAZ_TTL_DB: &str = "524544495330303132fa0972656469732d76657205382e342e30fa0a7265\
    6469732d62697473c040fa056374696d65c29b326369fa08757365642d6d\
    656dc2c8201200fa08616f662d62617365c000fe00fb02010003666f6f03\
    626172fc89037fab9b010000000362617a046672617aff1ac69407fe5b14\
    fc";

    #[test]
    fn test_baz_ttl_db() {
        let b = db_bytes(BAZ_TTL_DB);
        assert_eq!(b.len(), 121);
        let result = parse_rdb(&b);
        let (_, rdb) = result.unwrap();
        assert_eq!(rdb.header.magic, "REDIS");
        assert_eq!(rdb.header.version, "0012");
        assert_eq!(rdb.metadata.len(), 5);
        assert_eq!(rdb.entries.len(), 2);
        assert_eq!(rdb.entries[0].kv.key, "foo");
        assert_eq!(rdb.entries[0].kv.value, "bar");
        assert_eq!(rdb.entries[0].expire, None);
        assert_eq!(rdb.entries[1].kv.key, "baz");
        assert_eq!(rdb.entries[1].kv.value, "fraz");
        assert_eq!(rdb.entries[1].expire, Some(1768108786569));
    }

    #[test]
    fn test_foobar_db() {
        let b = db_bytes(FOOBAR_DB);
        assert_eq!(b.len(), 102);
        let result = parse_rdb(&b);
        let (_, rdb) = result.unwrap();
        assert_eq!(rdb.header.magic, "REDIS");
        assert_eq!(rdb.header.version, "0012");
        assert_eq!(rdb.metadata.len(), 5);
        assert_eq!(rdb.entries.len(), 1);
        assert_eq!(rdb.entries[0].kv.key, "foo");
        assert_eq!(rdb.entries[0].kv.value, "bar");
        assert_eq!(rdb.entries[0].expire, None);
    }

    #[test]
    fn test_empty_file() {
        let b = db_bytes(EMPTY_DB);
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

    #[test]
    fn test_expire_entry_timestamp() {
        let entry = "FC1572E7078F0100000003666F6F03626172";
        let entry_bytes = hex::decode(entry).unwrap();
        let (_, entry) = entry_expire_ms(&entry_bytes).unwrap();
        assert_eq!(entry.expire, Some(1713824559637));
        assert_eq!(entry.kv.key, "foo");
        assert_eq!(entry.kv.value, "bar");
    }

    #[test]
    fn test_database_value() {
        let value = "0006666F6F6261720662617A717578";
        let value_bytes = hex::decode(value).unwrap();
        let (_, kv) = database_value(&value_bytes).unwrap();
        assert_eq!(kv.key, "foobar");
        assert_eq!(kv.value, "bazqux");
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

    fn db_bytes(db_name: &str) -> Vec<u8> {
        hex::decode(db_name).unwrap()
    }

    #[test]
    fn test_magic() {
        let db = db_bytes(EMPTY_DB);
        let (_, magic) = magic(&db).unwrap();
        assert_eq!(magic, &b"REDIS"[..]);
    }

    #[test]
    fn test_version() {
        let db = db_bytes(EMPTY_DB);
        let (i, _) = magic(&db).unwrap();
        let (_, version) = version(i).unwrap();
        assert_eq!(version, &b"0012"[..]);
    }

    #[test]
    fn test_header() {
        let db = db_bytes(EMPTY_DB);
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
}
