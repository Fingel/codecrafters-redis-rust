/// https://github.com/codecrafters-io/redis-geocoding-algorithm
use crate::{
    Db,
    parser::{RError, RedisValueRef},
    zset::zadd,
};

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

fn validate_lat_lng(lng: f64, lat: f64) -> Result<(), String> {
    if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&lng) {
        Err(format!("Longitude value ({}) is invalid", lng))
    } else if !(MIN_LATITUDE..=MAX_LATITUDE).contains(&lat) {
        Err(format!("Latitude value ({}) is invalid", lat))
    } else {
        Ok(())
    }
}

fn spread_i32_to_i64(v: i32) -> i64 {
    let v: i64 = v as i64 & 0xFFFFFFFF;
    let v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    let v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
    let v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
    let v = (v | (v << 2)) & 0x3333333333333333;
    (v | (v << 1)) & 0x5555555555555555
}

fn compact_i64_to_i32(v: i64) -> i32 {
    let v = v & 0x5555555555555555;
    let v = (v | (v >> 1)) & 0x3333333333333333;
    let v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    let v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    let v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    let v = (v | (v >> 16)) & 0x00000000FFFFFFFF;
    v as i32
}

fn interleave(x: i32, y: i32) -> i64 {
    let x = spread_i32_to_i64(x);
    let y = spread_i32_to_i64(y);
    let y_shifted = y << 1;
    x | y_shifted
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: i32,
    grid_longitude_number: i32,
) -> (f64, f64) {
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / (2.0f64.powi(26)));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / (2.0f64.powi(26)));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / (2.0f64.powi(26)));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / (2.0f64.powi(26)));

    //Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    (latitude, longitude)
}

fn encode_lng_lat(lng: f64, lat: f64) -> f64 {
    // Step one: Normalize
    let normalized_lat = 2.0f64.powi(26) * (lat - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_lng = 2.0f64.powi(26) * (lng - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Step two: Truncate
    let normalized_lat = normalized_lat.trunc() as i32;
    let normalized_lng = normalized_lng.trunc() as i32;

    interleave(normalized_lat, normalized_lng) as f64
}

fn decode_geocode(geo_code: f64) -> (f64, f64) {
    let y = geo_code.trunc() as i64 >> 1;
    let x = geo_code.trunc() as i64;
    let grid_latitude_number = compact_i64_to_i32(x);
    let grid_longitude_number = compact_i64_to_i32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

pub fn geoadd(db: &Db, set: String, lng: f64, lat: f64, member: String) -> RedisValueRef {
    if let Err(err) = validate_lat_lng(lng, lat) {
        return RError(format!("ERR {}", err));
    }
    let score = encode_lng_lat(lng, lat);
    zadd(db, set, score, member)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_lng_lat_bangkok() {
        let encoded = encode_lng_lat(100.5252, 13.7220);
        assert_eq!(encoded, 3962257306574459.0);
    }

    #[test]
    fn test_encode_lng_lat_sydney() {
        let encoded = encode_lng_lat(151.2093, -33.8688);
        assert_eq!(encoded, 3252046221964352.0);
    }

    #[test]
    fn decode_geocode_paris() {
        let (lat, lng) = decode_geocode(3663832752681684.0);
        assert!((lat - 48.8534).abs() < 0.0001);
        assert!((lng - 2.3488).abs() < 0.0001);
    }
}
