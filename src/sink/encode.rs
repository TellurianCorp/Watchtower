use serde_json::json;

use crate::proto;

/// Convert a proto timestamp to unix nanoseconds.
pub fn timestamp_to_nanos(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds * 1_000_000_000 + ts.nanos as i64
}

/// Build the full JSON document for a log record (same shape used by the ES sink).
pub fn record_to_json(record: &proto::LogRecord) -> serde_json::Value {
    let mut doc = json!({ "severity": record.severity, "body": record.body });

    if let Some(ts) = &record.timestamp {
        doc["@timestamp"] = json!(rfc3339_from_nanos(timestamp_to_nanos(ts)));
    }
    if let Some(resource) = &record.resource {
        doc["resource"] = json!({
            "service_name": resource.service_name,
            "host_name": resource.host_name,
            "instance_id": resource.instance_id,
        });
    }
    if !record.trace_id.is_empty() {
        doc["trace_id"] = json!(hex::encode(&record.trace_id));
    }
    if !record.span_id.is_empty() {
        doc["span_id"] = json!(hex::encode(&record.span_id));
    }
    if !record.attributes.is_empty() {
        doc["attributes"] = attributes_to_json(&record.attributes);
    }
    if !record.schema_url.is_empty() {
        doc["schema_url"] = json!(record.schema_url);
    }
    doc
}

/// Serialize a list of KeyValue attributes into a JSON object.
pub fn attributes_to_json(attrs: &[proto::KeyValue]) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = attrs
        .iter()
        .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.clone(), any_value_to_json(v))))
        .collect();
    json!(map)
}

pub fn any_value_to_json(v: &proto::AnyValue) -> serde_json::Value {
    match &v.value {
        Some(proto::any_value::Value::StringValue(s)) => json!(s),
        Some(proto::any_value::Value::IntValue(n)) => json!(n),
        Some(proto::any_value::Value::DoubleValue(f)) => json!(f),
        Some(proto::any_value::Value::BoolValue(b)) => json!(b),
        Some(proto::any_value::Value::BytesValue(b)) => json!(hex::encode(b)),
        Some(proto::any_value::Value::ArrayValue(arr)) => {
            json!(arr.values.iter().map(any_value_to_json).collect::<Vec<_>>())
        }
        Some(proto::any_value::Value::MapValue(m)) => attributes_to_json(&m.entries),
        None => serde_json::Value::Null,
    }
}

/// Format unix nanoseconds as an RFC3339 UTC string.
pub fn rfc3339_from_nanos(ts_nanos: i64) -> String {
    let seconds = ts_nanos.div_euclid(1_000_000_000);
    let nanos = ts_nanos.rem_euclid(1_000_000_000) as u32;
    const SECS_PER_DAY: i64 = 86400;
    let days = seconds.div_euclid(SECS_PER_DAY);
    let day_secs = seconds.rem_euclid(SECS_PER_DAY) as u32;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let secs = day_secs % 60;
    let (year, month, day) = days_to_date(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{secs:02}.{nanos:09}Z")
}

fn days_to_date(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AnyValue, KeyValue, LogRecord, Resource, Severity, any_value};

    fn sample() -> LogRecord {
        LogRecord {
            timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 123_000_000 }),
            severity: Severity::Error as i32,
            body: "boom".into(),
            attributes: vec![KeyValue {
                key: "order_id".into(),
                value: Some(AnyValue { value: Some(any_value::Value::StringValue("ord_9".into())) }),
            }],
            resource: Some(Resource {
                service_name: "pay".into(), host_name: "h1".into(), instance_id: "i1".into(),
                attributes: vec![],
            }),
            trace_id: vec![], span_id: vec![], schema_url: String::new(),
        }
    }

    #[test]
    fn nanos_round_trip() {
        let ts = prost_types::Timestamp { seconds: 1_700_000_000, nanos: 123_000_000 };
        assert_eq!(timestamp_to_nanos(&ts), 1_700_000_000_123_000_000);
    }

    #[test]
    fn rfc3339_has_expected_prefix() {
        let s = rfc3339_from_nanos(1_700_000_000_123_000_000);
        assert!(s.starts_with("2023-11-14T"), "got {s}");
        assert!(s.ends_with("Z"));
    }

    #[test]
    fn record_json_shape() {
        let doc = record_to_json(&sample());
        assert_eq!(doc["severity"], 17);
        assert_eq!(doc["body"], "boom");
        assert_eq!(doc["resource"]["service_name"], "pay");
        assert_eq!(doc["attributes"]["order_id"], "ord_9");
    }
}
