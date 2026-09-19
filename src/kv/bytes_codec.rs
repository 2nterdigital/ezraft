//! Serde codec for `Bytes`: base64 text on human-readable formats, raw bytes otherwise
//!
//! `EzKv` values are opaque bytes ([`bytes::Bytes`]), but they travel over two kinds of formats
//! with different conventions: JSON - the Raft log and the wire - wants text, and postcard - the
//! snapshot format - wants raw bytes for size and speed. [`Serializer::is_human_readable`] is
//! what tells the two apart; this module is the one place that checks it, applied to every
//! `Bytes` field in the `kv` vocabulary via `#[serde(with = "crate::kv::bytes_codec")]`.
//!
//! Plain derives elsewhere in this vocabulary stay possible only because this module (not a
//! forbidden `#[serde(...)]` attribute like `tag` or `untagged`) is where the special-casing
//! lives.

use std::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serializer;
use serde::de::Visitor;

/// Serialize as base64 text on human-readable formats, raw bytes otherwise
pub(crate) fn serialize<S>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    if serializer.is_human_readable() {
        serializer.serialize_str(&base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            value,
        ))
    } else {
        serializer.serialize_bytes(value)
    }
}

/// Deserialize base64 text on human-readable formats, raw bytes otherwise
///
/// # Errors
///
/// Returns a deserialization error when a human-readable value is not valid base64.
pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
where D: Deserializer<'de> {
    if deserializer.is_human_readable() {
        let text = String::deserialize(deserializer)?;
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, text.as_bytes())
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    } else {
        deserializer.deserialize_bytes(BytesVisitor)
    }
}

/// Visitor that accepts either borrowed or owned raw bytes, matching what binary serde formats
/// (postcard included) hand to [`Deserializer::deserialize_bytes`]
struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a byte sequence")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Bytes, E>
    where E: serde::de::Error {
        Ok(Bytes::copy_from_slice(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Bytes, E>
    where E: serde::de::Error {
        Ok(Bytes::from(v))
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;

    /// A single-field wrapper is enough to exercise the codec: derives stay plain, only the
    /// field opts into the custom (de)serialization.
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Wrapper {
        #[serde(with = "super")]
        value: Bytes,
    }

    #[test]
    fn json_round_trip_encodes_as_base64_text() {
        let wrapper = Wrapper {
            value: Bytes::from_static(b"hello \xff\x00 world"),
        };

        let json = serde_json::to_string(&wrapper).unwrap();
        assert_eq!(r#"{"value":"aGVsbG8g/wAgd29ybGQ="}"#, json);

        let back: Wrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(wrapper, back);
    }

    #[test]
    fn postcard_round_trip_encodes_as_raw_bytes() {
        let wrapper = Wrapper {
            value: Bytes::from_static(b"hello \xff\x00 world"),
        };

        let encoded = postcard::to_allocvec(&wrapper).unwrap();
        // Raw bytes, not base64 text: a length-prefixed copy of the payload, not the 24-byte
        // base64 string the JSON test produced for the same input.
        assert_eq!(
            wrapper.value.len(),
            encoded.len() - 1,
            "expected a 1-byte varint length prefix + raw payload"
        );

        let back: Wrapper = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(wrapper, back);
    }

    #[test]
    fn empty_bytes_round_trip_in_both_formats() {
        let wrapper = Wrapper { value: Bytes::new() };

        let json = serde_json::to_string(&wrapper).unwrap();
        assert_eq!(r#"{"value":""}"#, json);
        assert_eq!(wrapper, serde_json::from_str(&json).unwrap());

        let encoded = postcard::to_allocvec(&wrapper).unwrap();
        assert_eq!(wrapper, postcard::from_bytes(&encoded).unwrap());
    }
}
