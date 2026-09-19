//! `Changes`: the group of changes one successful transaction produced
//!
//! `KvState::apply` (crate-internal) returns a `Changes` alongside every
//! successful transaction. It is the unit `watch` (ech0-media#32) will deliver per revision:
//! everything one transaction changed, sharing that transaction's revision, in the order its ops
//! ran.

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

use crate::kv::bytes_codec;
use crate::kv::view::KeyValue;

/// One effective change a transaction's ops made
///
/// There is no item for an op that changed nothing: deleting an absent key, or a `DeletePrefix`
/// that matched no key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    /// A key was created or overwritten
    Put {
        /// The key written
        key: String,
        /// Its new value
        #[serde(with = "bytes_codec")]
        value: Bytes,
        /// The revision at which the key was created (unchanged if this was an overwrite)
        create_rev: u64,
        /// The revision of this change (always the transaction's new revision)
        mod_rev: u64,
    },
    /// A key was removed
    Delete {
        /// The key removed
        key: String,
        /// The key's value and revisions immediately before this transaction removed it
        prev: KeyValue,
    },
}

/// Everything one successful transaction changed
///
/// `items` lists effective changes in op order; a `DeletePrefix` op expands to one
/// [`Change::Delete`] per key it removed, in key order, contiguous within the op's position in that
/// order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changes {
    /// The revision every item in `items` shares: the transaction's new revision
    pub revision: u64,
    /// The effective changes, in op order
    pub items: Vec<Change>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changes_serde_round_trips_in_json_and_postcard() {
        let changes = Changes {
            revision: 5,
            items: vec![
                Change::Put {
                    key: "a".to_string(),
                    value: Bytes::from_static(b"\x00\xffvalue"),
                    create_rev: 5,
                    mod_rev: 5,
                },
                Change::Delete {
                    key: "b".to_string(),
                    prev: KeyValue {
                        key: "b".to_string(),
                        value: Bytes::from_static(b"old"),
                        create_rev: 2,
                        mod_rev: 4,
                    },
                },
            ],
        };

        let json = serde_json::to_string(&changes).unwrap();
        assert_eq!(changes, serde_json::from_str(&json).unwrap());

        let packed = postcard::to_allocvec(&changes).unwrap();
        assert_eq!(changes, postcard::from_bytes(&packed).unwrap());
    }
}
