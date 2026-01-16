use crate::{
    Db,
    parser::{RError, RInt, RedisValueRef},
};

fn validate_lat_lng(lng: f64, lat: f64) -> Result<(), String> {
    if !(-180.0..=180.0).contains(&lng) {
        Err(format!("Longitude value ({}) is invalid", lng))
    } else if !(-85.05112878..=85.05112878).contains(&lat) {
        Err(format!("Latitude value ({}) is invalid", lat))
    } else {
        Ok(())
    }
}

pub fn geoadd(_db: &Db, _set: String, lng: f64, lat: f64, _member: String) -> RedisValueRef {
    if let Err(err) = validate_lat_lng(lng, lat) {
        return RError(format!("ERR {}", err));
    }
    RInt(1)
}
