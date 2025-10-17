const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

const SCALE_FACTOR: f64 = (1 << 26) as f64;

fn normalize_latitude(latitude: f64) -> u32 {
    (SCALE_FACTOR * ((latitude - MIN_LATITUDE) / LATITUDE_RANGE)) as _
}

fn normalize_longitude(longitude: f64) -> u32 {
    (SCALE_FACTOR * ((longitude - MIN_LONGITUDE) / LONGITUDE_RANGE)) as _
}

pub fn encode(latitude: f64, longitude: f64) -> Option<f64> {
    if !(MIN_LATITUDE..=MAX_LATITUDE).contains(&latitude) {
        eprintln!("aaa");
        return None;
    }
    if !(MIN_LONGITUDE..=MAX_LONGITUDE).contains(&longitude) {
        eprintln!("bbb");
        return None;
    }
    Some(interleave_f64(latitude, longitude) as f64)
}

fn interleave_f64(lat: f64, lon: f64) -> u64 {
    let latitude = normalize_latitude(lat);
    let longitude = normalize_longitude(lon);
    let latitude = spread_u32_to_u64(latitude);
    let longitude = spread_u32_to_u64(longitude);

    latitude | (longitude << 1)
}

fn spread_u32_to_u64(v: u32) -> u64 {
    let mut v = v as u64 & 0xFFFFFFFF;

    v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v << 2)) & 0x3333333333333333;
    v = (v | (v << 1)) & 0x5555555555555555;

    v
}

pub fn decode(score: f64) -> (f64, f64) {
    convert_score_to_coordinates(score as u64)
}

fn convert_score_to_coordinates(score: u64) -> (f64, f64) {
    let latitude = compact_u64_to_u32(score);
    let longitude = compact_u64_to_u32(score >> 1);
    let lat_min = MIN_LATITUDE + LATITUDE_RANGE * (latitude as f64 / SCALE_FACTOR);
    let lat_max = MIN_LATITUDE + LATITUDE_RANGE * ((latitude + 1) as f64 / SCALE_FACTOR);
    let lon_min = MIN_LONGITUDE + LATITUDE_RANGE * (longitude as f64 / SCALE_FACTOR);
    let lon_max = MIN_LONGITUDE + LATITUDE_RANGE * ((longitude + 1) as f64 / SCALE_FACTOR);

    (lat_min.midpoint(lat_max), lon_min.midpoint(lon_max))
}

fn compact_u64_to_u32(v: u64) -> u32 {
    let mut v = v & 0x5555555555555555;

    v = (v | (v >> 1)) & 0x3333333333333333;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF;

    v as _
}

#[cfg(test)]
mod test {
    use crate::geo_module::convert_score_to_coordinates;
    use crate::geo_module::interleave_f64;

    #[test]
    fn encode() {
        assert_eq!(interleave_f64(13.7220, 100.5252), 3962257306574459);
        eprintln!("{:?}", convert_score_to_coordinates(3962257306574459));
    }
}
