use nom::IResult;
use nom::bytes::complete::{tag, take};

struct Header {
    magic: String,
    version: String,
}

fn magic_parser(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("REDIS")(i)
}

fn version_parser(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take(4usize)(i)
}

fn header_parser(i: &[u8]) -> IResult<&[u8], Header> {
    let (i, magic) = magic_parser(i)?;
    let (i, version) = version_parser(i)?;
    let magic = String::from_utf8_lossy(magic).into_owned();
    let version = String::from_utf8_lossy(version).into_owned();
    Ok((i, Header { magic, version }))
}

#[cfg(test)]
mod tests {
    use super::*;
    const EMPTY_DB: &str = "524544495330303132fa0972656469732d76657205382e342e30fa0a7265\
    6469732d62697473c040fa056374696d65c27f656169fa08757365642d6d\
    656dc280f41000fa08616f662d62617365c000ff4635ae29d917db65";

    fn empty_db_bytes() -> Vec<u8> {
        hex::decode(EMPTY_DB).unwrap()
    }

    #[test]
    fn test_magic() {
        let db = empty_db_bytes();
        let result = magic_parser(&db);
        assert_eq!(result, Ok((&db[5..], &b"REDIS"[..])));
    }

    #[test]
    fn test_version() {
        let db = empty_db_bytes();
        let (i, _) = magic_parser(&db).unwrap();
        let (_, version) = version_parser(i).unwrap();
        assert_eq!(version, &b"0012"[..]);
    }

    #[test]
    fn test_header() {
        let db = empty_db_bytes();
        let result = header_parser(&db).unwrap();
        let (_, header) = result;
        assert_eq!(header.magic, "REDIS");
        assert_eq!(header.version, "0012");
    }

    #[test]
    fn test_empty_file() {
        let b = empty_db_bytes();
        assert_eq!(b.len(), 88);
    }
}
