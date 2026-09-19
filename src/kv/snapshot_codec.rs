//! The postcard snapshot codec: a magic and format version, checked before anything else
//!
//! [`encode_snapshot`] and [`decode_snapshot`] are the only place the whole replicated state -
//! every key, the KV's revision, and the foundation-internal state block ([`InternalState`]) - is
//! turned into bytes and back. `crate::storage::adapter::state_machine` is the only caller: it
//! builds a snapshot by cloning the state machine's root ([`KvState::view`]) and internal state
//! ([`KvState::internal_state`]) under its lock - O(1), both a persistent map's root and a clone
//! of the (today empty) internal block - then calls [`encode_snapshot`] afterwards, off that lock
//! and off the async worker thread (`spawn_blocking`): encoding a large state is CPU work, and it
//! must never hold up `apply` or `view()`. Installing a snapshot is the reverse:
//! [`decode_snapshot`] runs to completion, magic and version checked first, before a single byte of
//! it is persisted or swapped into the live state machine.
//!
//! # The v1 wire format
//!
//! ```text
//! byte 0..4   magic:   b"EZKV"
//! byte 4..6   version: u16, little-endian (currently 1)
//! byte 6..    body:    postcard-encoded, shape depends on `version`
//! ```
//!
//! The header is parsed by hand, never through postcard: postcard is a positional format with no
//! self-describing tags, so nothing about the body can be safely decoded until the version says
//! which shape to expect. For version 1 the body is [`SnapshotV1`]: `revision`, then every key as
//! a [`KeyValue`] (in the order the source [`View`] iterates them), then the (today empty)
//! [`InternalState`] block. Tickets ech0-media#30 and ech0-media#33 add fields to `InternalState`
//! itself - never to `SnapshotV1` directly - so the version stays `1` for as long as this
//! milestone runs (see `crate::kv`'s own module docs).
//!
//! # Errors
//!
//! [`decode_snapshot`] never panics on untrusted bytes and never partially applies a bad
//! snapshot: every failure - a body shorter than the header, a bad magic, an unrecognized
//! version, or a body that does not parse as `SnapshotV1` - is reported before anything is
//! returned, so a caller that gets `Err` has touched none of its own state and can leave whatever
//! it had in place.

use serde::Deserialize;
use serde::Serialize;

use crate::kv::state::InternalState;
use crate::kv::view::KeyValue;
use crate::kv::view::View;

/// Identifies this crate's snapshot format, distinct from any other bytes that might otherwise
/// end up in storage's snapshot slot
const MAGIC: [u8; 4] = *b"EZKV";

/// The only format version this build knows how to read or write
const CURRENT_VERSION: u16 = 1;

/// `MAGIC` followed by the little-endian `u16` version - the part of every snapshot that is
/// never postcard-encoded, so it can be checked before anything else is interpreted
const HEADER_LEN: usize = MAGIC.len() + 2;

/// The v1 snapshot body: every key, the KV's revision, and the foundation-internal state block
///
/// Plain derives only (no `tag`/`untagged`/`flatten`/`skip_serializing_if`/`default`): this is
/// exactly what postcard's positional format encodes, so any of those would either not compile
/// against postcard's `deserialize_any`-free `Deserializer` or silently change the wire shape.
#[derive(Serialize, Deserialize)]
struct SnapshotV1 {
    revision: u64,
    entries: Vec<KeyValue>,
    internal: InternalState,
}

/// What [`decode_snapshot`] hands back: the parts `KvState::restore` (crate-internal) needs to
/// rebuild an identical state machine
#[derive(Debug)]
pub(crate) struct DecodedSnapshot {
    pub(crate) revision: u64,
    pub(crate) entries: Vec<KeyValue>,
    pub(crate) internal: InternalState,
}

/// Encode `view` and `internal` as a v1 snapshot
///
/// `view.prefix("")` is the O(n) work in here - one [`KeyValue`] per key, plus the postcard
/// encoding of the whole body. The caller's job is to have already taken `view` and `internal`
/// cheaply (O(1): a persistent map's root and a clone of the - today empty - internal block)
/// while holding the state machine's lock, and to call this afterwards, off that lock and off an
/// async worker thread.
///
/// # Errors
///
/// Returns [`SnapshotEncodeError`] if postcard cannot serialize the body - not expected for any
/// of this crate's own types, but never assumed away with an `unwrap`.
pub(crate) fn encode_snapshot(view: &View, internal: &InternalState) -> Result<Vec<u8>, SnapshotEncodeError> {
    let body = SnapshotV1 {
        revision: view.revision(),
        entries: view.prefix("").collect(),
        internal: internal.clone(),
    };
    let body_bytes = postcard::to_allocvec(&body)?;

    let mut bytes = Vec::with_capacity(HEADER_LEN + body_bytes.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&CURRENT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&body_bytes);
    Ok(bytes)
}

/// Decode `bytes` as a snapshot, checking the magic and the format version before interpreting
/// anything else
///
/// # Errors
///
/// [`SnapshotDecodeError::Truncated`] if `bytes` is shorter than the fixed-size header;
/// [`SnapshotDecodeError::BadMagic`] if the first bytes are not this format's magic;
/// [`SnapshotDecodeError::UnknownVersion`] if the version is one this build does not know how to
/// read - named explicitly, alongside the version this build does support, so a caller can log
/// exactly why it is stopping rather than guessing at a layout it does not understand;
/// [`SnapshotDecodeError::Corrupt`] if the magic and version check out but the body does not
/// parse as that version's expected shape; [`SnapshotDecodeError::TrailingBytes`] if it parses
/// but is followed by bytes this format never writes. Every case is reported before any byte of
/// `bytes` is otherwise interpreted.
pub(crate) fn decode_snapshot(bytes: &[u8]) -> Result<DecodedSnapshot, SnapshotDecodeError> {
    if bytes.len() < HEADER_LEN {
        return Err(SnapshotDecodeError::Truncated {
            minimum: HEADER_LEN,
            actual: bytes.len(),
        });
    }

    let (header, body) = bytes.split_at(HEADER_LEN);
    let (magic, version_bytes) = header.split_at(MAGIC.len());
    if magic != MAGIC {
        return Err(SnapshotDecodeError::BadMagic);
    }

    let version = u16::from_le_bytes([version_bytes[0], version_bytes[1]]);
    if version != CURRENT_VERSION {
        return Err(SnapshotDecodeError::UnknownVersion {
            found: version,
            supported: CURRENT_VERSION,
        });
    }

    // `take_from_bytes`, not `from_bytes`: postcard stops reading once the value is complete and
    // would accept anything after it. Bytes this format did not write mean the snapshot is not
    // what it claims to be, whatever parsed before them.
    let (payload, rest): (SnapshotV1, &[u8]) = postcard::take_from_bytes(body)?;
    if !rest.is_empty() {
        return Err(SnapshotDecodeError::TrailingBytes { count: rest.len() });
    }

    Ok(DecodedSnapshot {
        revision: payload.revision,
        entries: payload.entries,
        internal: payload.internal,
    })
}

/// Why [`encode_snapshot`] could not turn a state machine into bytes
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotEncodeError {
    /// postcard could not serialize the snapshot body
    #[error("failed to encode the snapshot body: {0}")]
    Postcard(#[from] postcard::Error),
}

/// Why [`decode_snapshot`] could not make sense of a stored or received snapshot
///
/// Checked in the order the fields of a snapshot actually appear: the magic before the version,
/// the version before the body - so a truncated or corrupt transfer is reported for what it
/// actually is, never misdiagnosed as `BadMagic` or `UnknownVersion` by a check that ran on bytes
/// it should not have trusted yet.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotDecodeError {
    /// Shorter than the fixed-size header alone
    #[error("snapshot is {actual} bytes, shorter than the {minimum}-byte header")]
    Truncated { minimum: usize, actual: usize },
    /// The first bytes are not this format's magic
    #[error("snapshot does not start with the expected magic bytes")]
    BadMagic,
    /// A version this build does not know how to read
    #[error("snapshot format version {found} is not supported by this build (supported: {supported})")]
    UnknownVersion { found: u16, supported: u16 },
    /// The magic and version checked out, but the body does not parse as that version's shape
    #[error("snapshot body is corrupt or truncated: {0}")]
    Corrupt(#[from] postcard::Error),
    /// The body parsed, but bytes this format never writes follow it
    #[error("snapshot body is followed by {count} unexpected trailing byte(s)")]
    TrailingBytes { count: usize },
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::kv::state::KvState;
    use crate::kv::txn::Txn;

    /// A state with several keys, a revision past 1, and a re-created key (so `create_rev` and
    /// `mod_rev` genuinely differ) - a round trip that only worked for one key at revision 1
    /// would not prove much.
    fn non_trivial_state() -> KvState {
        let mut state = KvState::new();
        state.apply(&Txn::new().put("a", Bytes::from_static(b"1"))).unwrap();
        state.apply(&Txn::new().put("b", Bytes::from_static(b"2"))).unwrap();
        state.apply(&Txn::new().delete("a").put("a", Bytes::from_static(b"3"))).unwrap();
        state
    }

    #[test]
    fn round_trip_reproduces_every_key_its_revisions_and_the_kv_revision() {
        let state = non_trivial_state();
        let view = state.view();
        let internal = state.internal_state();

        let bytes = encode_snapshot(&view, &internal).unwrap();
        let decoded = decode_snapshot(&bytes).unwrap();

        assert_eq!(view.revision(), decoded.revision);
        let mut expected: Vec<KeyValue> = view.prefix("").collect();
        expected.sort_by(|a, b| a.key.cmp(&b.key));
        let mut actual = decoded.entries;
        actual.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(expected, actual);
        assert_eq!(internal, decoded.internal);
    }

    #[test]
    fn an_empty_state_round_trips_to_an_empty_state() {
        let state = KvState::new();
        let bytes = encode_snapshot(&state.view(), &state.internal_state()).unwrap();
        let decoded = decode_snapshot(&bytes).unwrap();

        assert_eq!(0, decoded.revision);
        assert!(decoded.entries.is_empty());
        assert_eq!(InternalState::default(), decoded.internal);
    }

    #[test]
    fn a_bad_magic_is_rejected() {
        let state = KvState::new();
        let mut bytes = encode_snapshot(&state.view(), &state.internal_state()).unwrap();
        bytes[0] = b'X';

        let err = decode_snapshot(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotDecodeError::BadMagic), "{err}");
    }

    #[test]
    fn input_shorter_than_the_header_is_truncated() {
        let err = decode_snapshot(b"EZ").unwrap_err();
        match err {
            SnapshotDecodeError::Truncated { minimum, actual } => {
                assert_eq!(HEADER_LEN, minimum);
                assert_eq!(2, actual);
            }
            other => panic!("expected Truncated, got {other}"),
        }
    }

    #[test]
    fn an_empty_input_is_truncated_not_a_panic() {
        let err = decode_snapshot(&[]).unwrap_err();
        assert!(matches!(err, SnapshotDecodeError::Truncated { .. }), "{err}");
    }

    #[test]
    fn a_body_cut_short_after_a_valid_header_is_corrupt_not_a_panic() {
        let state = non_trivial_state();
        let mut bytes = encode_snapshot(&state.view(), &state.internal_state()).unwrap();
        // The header survives intact; only the postcard body is chopped mid-way through.
        bytes.truncate(HEADER_LEN + 2);

        let err = decode_snapshot(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotDecodeError::Corrupt(_)), "{err}");
    }

    /// postcard stops reading once the value is complete, so without an explicit check a
    /// snapshot followed by anything at all would decode as if it were intact.
    #[test]
    fn bytes_after_a_complete_body_are_refused() {
        let state = non_trivial_state();
        let mut bytes = encode_snapshot(&state.view(), &state.internal_state()).unwrap();
        bytes.extend_from_slice(b"junk");

        let err = decode_snapshot(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotDecodeError::TrailingBytes { count: 4 }), "{err}");
    }

    #[test]
    fn an_unknown_version_is_rejected_and_names_the_version_found_and_supported() {
        let state = KvState::new();
        let mut bytes = encode_snapshot(&state.view(), &state.internal_state()).unwrap();
        bytes[4..6].copy_from_slice(&9u16.to_le_bytes());

        let err = decode_snapshot(&bytes).unwrap_err();
        match err {
            SnapshotDecodeError::UnknownVersion { found, supported } => {
                assert_eq!(9, found);
                assert_eq!(CURRENT_VERSION, supported);
            }
            other => panic!("expected UnknownVersion, got {other}"),
        }
    }
}
