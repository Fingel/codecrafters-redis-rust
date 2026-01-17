/// https://github.com/codecrafters-io/redis-geocoding-algorithm
use crate::{
    Db,
    parser::{RArray, RError, RNullArray, RString, RedisValueRef},
    zset::{zadd, zrangebyscore, zscore},
};

const EARTH_RADIUS: f64 = 6372797.560856;
const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

#[derive(Debug, Clone)]
struct Point {
    lat: f64,
    lng: f64,
}

fn validate_point(point: &Point) -> Result<(), String> {
    if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&point.lng) {
        Err(format!("Longitude value ({}) is invalid", point.lng))
    } else if !(MIN_LATITUDE..=MAX_LATITUDE).contains(&point.lat) {
        Err(format!("Latitude value ({}) is invalid", point.lat))
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
) -> Point {
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / (2.0f64.powi(26)));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / (2.0f64.powi(26)));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / (2.0f64.powi(26)));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / (2.0f64.powi(26)));

    //Calculate the center point of the grid cell
    let lat = (grid_latitude_min + grid_latitude_max) / 2.0;
    let lng = (grid_longitude_min + grid_longitude_max) / 2.0;

    Point { lat, lng }
}

fn encode_point(point: Point) -> f64 {
    // Step one: Normalize
    let normalized_lat = 2.0f64.powi(26) * (point.lat - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_lng = 2.0f64.powi(26) * (point.lng - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Step two: Truncate
    let normalized_lat = normalized_lat.trunc() as i32;
    let normalized_lng = normalized_lng.trunc() as i32;

    interleave(normalized_lat, normalized_lng) as f64
}

fn decode_geocode(geo_code: f64) -> Point {
    let y = geo_code.trunc() as i64 >> 1;
    let x = geo_code.trunc() as i64;
    let grid_latitude_number = compact_i64_to_i32(x);
    let grid_longitude_number = compact_i64_to_i32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

pub fn geoadd(db: &Db, set: String, lng: f64, lat: f64, member: String) -> RedisValueRef {
    let point = Point { lat, lng };
    if let Err(err) = validate_point(&point) {
        return RError(format!("ERR {}", err));
    }
    let score = encode_point(point);
    zadd(db, set, score, member)
}

fn haversine_distance(origin: Point, dest: Point) -> f64 {
    let lat1 = origin.lat.to_radians();
    let lat2 = dest.lat.to_radians();
    let d_lat = lat2 - lat1;
    let d_lon = (dest.lng - origin.lng).to_radians();
    let a = (d_lat / 2.0).sin().powi(2) + (d_lon / 2.0).sin().powi(2) * lat1.cos() * lat2.cos();
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS * c
}

pub fn geopos(db: &Db, set: String, members: Vec<String>) -> RedisValueRef {
    let results: Vec<RedisValueRef> = members
        .iter()
        .map(|member| {
            let score = zscore(db, set.clone(), member.clone());
            match score.expect_int() {
                Ok(score) => {
                    let point = decode_geocode(score as f64);
                    RArray(vec![
                        RString(point.lng.to_string()),
                        RString(point.lat.to_string()),
                    ])
                }
                _ => RNullArray(),
            }
        })
        .collect();

    RArray(results)
}

pub fn geodist(db: &Db, set: String, member1: String, member2: String) -> RedisValueRef {
    let score1 = zscore(db, set.clone(), member1.clone());
    let score2 = zscore(db, set.clone(), member2.clone());

    match (score1.expect_int(), score2.expect_int()) {
        (Ok(score1), Ok(score2)) => {
            let point1 = decode_geocode(score1 as f64);
            let point2 = decode_geocode(score2 as f64);
            let distance = haversine_distance(point1, point2);
            RString(format!("{:.4}", distance))
        }
        _ => RNullArray(),
    }
}

pub fn geosearch(
    db: &Db,
    key: String,
    lng: f64,
    lat: f64,
    radius: f64,
    _unit: String,
) -> RedisValueRef {
    let origin = Point { lat, lng };

    // https://stackoverflow.com/questions/238260/how-to-calculate-the-bounding-box-for-a-given-lat-lng-location
    // http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#LongitudeIncorrect
    let angular_radius = radius / EARTH_RADIUS; // radians
    let lat_rad = lat.to_radians();

    let lat_delta = angular_radius.to_degrees();
    let lng_delta = (angular_radius.sin() / lat_rad.cos()).asin().to_degrees();

    let min_point = Point {
        lng: (lng - lng_delta).max(MIN_LONGITUDE),
        lat: (lat - lat_delta).max(MIN_LATITUDE),
    };
    let max_point = Point {
        lng: (lng + lng_delta).min(MAX_LONGITUDE),
        lat: (lat + lat_delta).min(MAX_LATITUDE),
    };

    // The box
    let min_score = encode_point(min_point);
    let max_score = encode_point(max_point);

    // This gives us rough set of candidates that can be filtered down futher by distance calculation
    let candidates = zrangebyscore(db, key.clone(), min_score, max_score);
    // TODO the command returns RedisValueRef, so we have to convert back to native values,
    // the logic should be factored out

    let filtered_candidates: Vec<String> = match candidates {
        RedisValueRef::Array(members) => members
            .into_iter()
            .filter_map(|member| {
                if let RedisValueRef::String(member_bytes) = member {
                    let member_name = String::from_utf8_lossy(&member_bytes).to_string();
                    if let RedisValueRef::String(score_bytes) =
                        zscore(db, key.clone(), member_name.clone())
                        && let Ok(score) = String::from_utf8_lossy(&score_bytes).parse::<f64>()
                    {
                        let point = decode_geocode(score);
                        let distance = haversine_distance(origin.clone(), point);
                        if distance <= radius {
                            return Some(member_name);
                        }
                    }
                }
                None
            })
            .collect(),
        _ => Vec::new(),
    };

    RArray(filtered_candidates.into_iter().map(RString).collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::RedisDb;

    use super::*;

    fn setup() -> Arc<RedisDb> {
        Arc::new(RedisDb::new(None, "/tmp/redis-files", "dump.rdb"))
    }

    #[test]
    fn test_encode_point_bangkok() {
        let encoded = encode_point(Point {
            lng: 100.5252,
            lat: 13.7220,
        });
        assert_eq!(encoded, 3962257306574459.0);
    }

    #[test]
    fn test_encode_point_sydney() {
        let encoded = encode_point(Point {
            lng: 151.2093,
            lat: -33.8688,
        });
        assert_eq!(encoded, 3252046221964352.0);
    }

    #[test]
    fn decode_geocode_paris() {
        let point = decode_geocode(3663832752681684.0);
        assert!((point.lat - 48.8534).abs() < 0.0001);
        assert!((point.lng - 2.3488).abs() < 0.0001);
    }

    #[test]
    fn test_geosearch() {
        let db = setup();
        geoadd(
            &db,
            "places".to_string(),
            11.5030378,
            48.164271,
            "Munich".to_string(),
        );
        geoadd(
            &db,
            "places".to_string(),
            2.2944692,
            48.8584625,
            "Paris".to_string(),
        );
        geoadd(
            &db,
            "places".to_string(),
            -0.0884948,
            51.506479,
            "London".to_string(),
        );

        let result = geosearch(
            &db,
            "places".to_string(),
            2.0,
            48.0,
            100_000.0,
            "m".to_string(),
        );
        assert_eq!(result, RArray(vec![RString("Paris".to_string())]));

        let result = geosearch(
            &db,
            "places".to_string(),
            2.0,
            48.0,
            500_000.0,
            "m".to_string(),
        );
        assert_eq!(
            result,
            RArray(vec![
                RString("London".to_string()),
                RString("Paris".to_string()),
            ])
        );

        let result = geosearch(
            &db,
            "places".to_string(),
            11.0,
            50.0,
            300_000.0,
            "m".to_string(),
        );
        assert_eq!(result, RArray(vec![RString("Munich".to_string()),]));
    }
}
