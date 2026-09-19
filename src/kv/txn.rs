//! `Txn`: the conditional, all-or-nothing write the KV accepts
//!
//! A [`Txn`] evaluates its [`Condition`]s against the state before the transaction and, only if
//! every one holds, applies its [`Op`]s in order. Evaluating conditions and applying ops is
//! `KvState::apply`'s job (crate-internal); this module owns the request
//! shape, the result of a failed evaluation ([`Conflict`]), and the size limits ([`KvLimits`]) a
//! caller checks before ever proposing the transaction.

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

use crate::kv::bytes_codec;

/// A condition a transaction requires to hold before its ops run
///
/// Every variant names the key it inspects; [`key`](Condition::key) reads it back regardless of
/// which condition it is, which is what [`Conflict`] uses to report the key a failed condition
/// was about.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Condition {
    /// The key's `mod_rev` equals exactly this revision
    ModRevEq {
        /// The key this condition inspects
        key: String,
        /// The `mod_rev` the key must currently have
        mod_rev: u64,
    },
    /// The key does not exist (equivalent to `create_rev == 0`)
    NotExists {
        /// The key this condition inspects
        key: String,
    },
    /// The key exists (any `create_rev` other than `0`)
    Exists {
        /// The key this condition inspects
        key: String,
    },
}

impl Condition {
    /// The key this condition inspects, regardless of which variant it is
    pub fn key(&self) -> &str {
        match self {
            Condition::ModRevEq { key, .. } | Condition::NotExists { key } | Condition::Exists { key } => key,
        }
    }
}

/// A single change a transaction's ops make to the key space
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// Write `value` under `key`, creating it or overwriting whatever was there
    Put {
        /// The key to write
        key: String,
        /// The opaque bytes to store; never printed in logs (see [`Txn`]'s `Display`)
        #[serde(with = "bytes_codec")]
        value: Bytes,
    },
    /// Remove `key`, if present; removing an absent key is not an error and makes no change
    Delete {
        /// The key to remove
        key: String,
    },
    /// Remove every key that starts with `prefix`, if any; `prefix` must not be empty
    DeletePrefix {
        /// The prefix whose keys are all removed
        prefix: String,
    },
}

/// A conditional, all-or-nothing write against the KV
///
/// `conditions` are checked in order against the state before this transaction; the first one
/// that fails aborts the whole transaction with a [`Conflict`] and nothing is applied. If every
/// condition holds, `ops` apply in order, each one seeing the effect of the ones before it.
///
/// Build one with [`Txn::new`] and the builder methods, or construct the struct directly - both
/// fields are public because a caller assembling a transaction from data (not literals) needs to
/// push conditions and ops itself.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Txn {
    /// Conditions checked, in order, against the state before this transaction
    pub conditions: Vec<Condition>,
    /// Operations applied, in order, once every condition holds
    pub ops: Vec<Op>,
}

impl Txn {
    /// An empty transaction: no conditions, no ops
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a condition, checked in the order added
    #[must_use]
    pub fn when(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    /// Append a `Put` op
    #[must_use]
    pub fn put(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.ops.push(Op::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Append a `Delete` op
    #[must_use]
    pub fn delete(mut self, key: impl Into<String>) -> Self {
        self.ops.push(Op::Delete { key: key.into() });
        self
    }

    /// Append a `DeletePrefix` op
    #[must_use]
    pub fn delete_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.ops.push(Op::DeletePrefix { prefix: prefix.into() });
        self
    }

    /// Check this transaction against `limits`, without touching any state
    ///
    /// This is a separate, explicit step from `KvState::apply` (crate-internal), which never
    /// validates limits itself: nodes may run with different configured limits (`EzConfig` is
    /// per-node), so checking inside `apply` would let that difference make replicas diverge.
    /// `EzKv::write` calls this itself before ever proposing a transaction.
    ///
    /// Checks, in order: the op count, then each condition's and op's key (and each `Put`'s
    /// value) against the per-item limits, accumulating the transaction's total size; finally the
    /// total size against `max_txn_bytes`. Returns the first violation found.
    ///
    /// # Errors
    ///
    /// Returns a [`LimitError`] naming the limit that was exceeded, the offending size, and the
    /// configured maximum.
    pub fn validate(&self, limits: &KvLimits) -> Result<(), LimitError> {
        if self.ops.len() > limits.max_txn_ops {
            return Err(LimitError::TooManyOps {
                actual: self.ops.len(),
                max: limits.max_txn_ops,
            });
        }
        // Counted separately, against the same limit. Left unbounded, conditions are the one part
        // of a transaction whose encoded size the byte limit below says nothing about: each one
        // is a few dozen bytes of structure around a key that may be a single byte.
        if self.conditions.len() > limits.max_txn_ops {
            return Err(LimitError::TooManyConditions {
                actual: self.conditions.len(),
                max: limits.max_txn_ops,
            });
        }

        let mut total_bytes: usize = 0;

        for condition in &self.conditions {
            let key = condition.key();
            check_key(key, limits)?;
            total_bytes += key.len();
        }

        for op in &self.ops {
            match op {
                Op::Put { key, value } => {
                    check_key(key, limits)?;
                    if value.len() > limits.max_value_bytes {
                        return Err(LimitError::ValueTooLarge {
                            key: key.clone(),
                            actual: value.len(),
                            max: limits.max_value_bytes,
                        });
                    }
                    total_bytes += key.len() + value.len();
                }
                Op::Delete { key } => {
                    check_key(key, limits)?;
                    total_bytes += key.len();
                }
                Op::DeletePrefix { prefix } => {
                    if prefix.is_empty() {
                        return Err(LimitError::EmptyPrefix);
                    }
                    if prefix.len() > limits.max_key_bytes {
                        return Err(LimitError::KeyTooLarge {
                            key: prefix.clone(),
                            actual: prefix.len(),
                            max: limits.max_key_bytes,
                        });
                    }
                    total_bytes += prefix.len();
                }
            }
        }

        if total_bytes > limits.max_txn_bytes {
            return Err(LimitError::TooLarge {
                actual: total_bytes,
                max: limits.max_txn_bytes,
            });
        }

        Ok(())
    }
}

/// Check a `Condition`'s or `Delete`/`Put`'s key against the empty and per-key size limits
fn check_key(key: &str, limits: &KvLimits) -> Result<(), LimitError> {
    if key.is_empty() {
        return Err(LimitError::EmptyKey);
    }
    if key.len() > limits.max_key_bytes {
        return Err(LimitError::KeyTooLarge {
            key: key.to_string(),
            actual: key.len(),
            max: limits.max_key_bytes,
        });
    }
    Ok(())
}

impl std::fmt::Display for Txn {
    /// A short summary: op and condition counts only, never a key or a value
    ///
    /// openraft logs every request it processes; a `Txn`'s ops carry arbitrary caller-controlled
    /// bytes ([`Op::Put`]'s value), so the one thing this impl must never do is show them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Txn({} condition(s), {} op(s))",
            self.conditions.len(),
            self.ops.len()
        )
    }
}

/// Size and count limits a [`Txn`] is checked against before it is proposed
///
/// Defaults are ech0-media#27's, configurable through `EzConfig::kv_limits`. ech0-media#36
/// measured what depends on them (the request body limit, snapshot size and timing) and left
/// them as they are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvLimits {
    /// Maximum size of a single key, in bytes. Default: 1 KiB.
    pub max_key_bytes: usize,
    /// Maximum size of a single value, in bytes. Default: 64 KiB.
    pub max_value_bytes: usize,
    /// Maximum number of ops in one transaction, and - counted separately - of conditions.
    /// Default: 128.
    pub max_txn_ops: usize,
    /// Maximum total size of a transaction, in bytes: the sum of every key's (and, for `Put`,
    /// value's) bytes across both `conditions` and `ops`. Default: 1 MiB.
    pub max_txn_bytes: usize,
}

impl Default for KvLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_value_bytes: 64 * 1024,
            max_txn_ops: 128,
            max_txn_bytes: 1024 * 1024,
        }
    }
}

/// Why [`Txn::validate`] rejected a transaction
///
/// Names the limit that was exceeded, the offending size, and the configured maximum, so a caller
/// can report exactly what was too big without re-deriving it from the transaction itself.
///
/// Plain serde derives (ech0-media#30): the calling node validates a transaction against its own
/// limits before ever sending anything, but a forwarded write is validated *again* on whichever
/// node actually proposes it - which may be configured with different limits - so this type must
/// be able to travel back over that RPC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum LimitError {
    /// A condition's or op's key was empty
    #[error("key must not be empty")]
    EmptyKey,
    /// A `DeletePrefix`'s prefix was empty
    #[error("prefix must not be empty")]
    EmptyPrefix,
    /// A key (or `DeletePrefix` prefix) exceeded `max_key_bytes`
    #[error("key {key:?} is {actual} bytes, exceeding the {max}-byte limit")]
    KeyTooLarge {
        /// The offending key or prefix
        key: String,
        /// Its size in bytes
        actual: usize,
        /// The configured `max_key_bytes`
        max: usize,
    },
    /// A `Put`'s value exceeded `max_value_bytes`
    #[error("value for key {key:?} is {actual} bytes, exceeding the {max}-byte limit")]
    ValueTooLarge {
        /// The key whose value was too large
        key: String,
        /// Its size in bytes
        actual: usize,
        /// The configured `max_value_bytes`
        max: usize,
    },
    /// The transaction had more ops than `max_txn_ops`
    #[error("transaction has {actual} op(s), exceeding the {max}-op limit")]
    TooManyOps {
        /// The number of ops in the transaction
        actual: usize,
        /// The configured `max_txn_ops`
        max: usize,
    },
    /// The transaction had more conditions than `max_txn_ops`
    #[error("transaction has {actual} condition(s), exceeding the {max}-condition limit")]
    TooManyConditions {
        /// The number of conditions in the transaction
        actual: usize,
        /// The configured `max_txn_ops`, which bounds conditions and ops alike, each on its own
        max: usize,
    },
    /// The transaction's total size exceeded `max_txn_bytes`
    #[error("transaction is {actual} bytes, exceeding the {max}-byte limit")]
    TooLarge {
        /// The transaction's total size in bytes (sum of key and value bytes over conditions and
        /// ops)
        actual: usize,
        /// The configured `max_txn_bytes`
        max: usize,
    },
    /// A node's registry entry - its `external_addr` plus its `meta` - exceeded `max_value_bytes`
    ///
    /// Not a transaction's limit, but the same kind of refusal for the same reason: a registry
    /// entry is replicated through the log like any value, and is held to a value's size.
    #[error("node registry entry is {actual} bytes, exceeding the {max}-byte limit")]
    NodeEntryTooLarge {
        /// The entry's size in bytes (`external_addr` plus `meta`)
        actual: usize,
        /// The configured `max_value_bytes`
        max: usize,
    },
}

/// The failing condition of a transaction whose conditions were not all satisfied
///
/// Carries everything a caller needs to retry: which condition failed (`index`, `condition`), and
/// the key's actual current `mod_rev` (`0` when the key does not exist).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("condition {index} ({condition:?}) failed: key {key:?} has mod_rev {current_mod_rev}")]
pub struct Conflict {
    /// The index, in `conditions`, of the first condition that did not hold
    pub index: usize,
    /// The condition that failed
    pub condition: Condition,
    /// The key the failing condition was about (same as `condition.key()`, extracted for
    /// convenience)
    pub key: String,
    /// The key's actual `mod_rev` at the time the condition was checked; `0` if the key does not
    /// exist
    pub current_mod_rev: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_methods_assemble_conditions_and_ops_in_order() {
        let txn = Txn::new()
            .when(Condition::Exists {
                key: "room/1".to_string(),
            })
            .when(Condition::ModRevEq {
                key: "room/2".to_string(),
                mod_rev: 5,
            })
            .put("room/1/name", Bytes::from_static(b"lobby"))
            .delete("room/2")
            .delete_prefix("room/3/");

        assert_eq!(
            vec![
                Condition::Exists {
                    key: "room/1".to_string()
                },
                Condition::ModRevEq {
                    key: "room/2".to_string(),
                    mod_rev: 5
                },
            ],
            txn.conditions
        );
        assert_eq!(
            vec![
                Op::Put {
                    key: "room/1/name".to_string(),
                    value: Bytes::from_static(b"lobby"),
                },
                Op::Delete {
                    key: "room/2".to_string()
                },
                Op::DeletePrefix {
                    prefix: "room/3/".to_string()
                },
            ],
            txn.ops
        );
    }

    #[test]
    fn display_never_shows_keys_or_values() {
        let txn = Txn::new().put("secret-key", Bytes::from_static(b"secret-value"));
        let shown = txn.to_string();
        assert_eq!("Txn(0 condition(s), 1 op(s))", shown);
        assert!(!shown.contains("secret"));
    }

    #[test]
    fn validate_accepts_a_transaction_within_every_limit() {
        let txn = Txn::new().when(Condition::NotExists { key: "a".to_string() }).put("a", Bytes::from_static(b"v"));
        assert_eq!(Ok(()), txn.validate(&KvLimits::default()));
    }

    #[test]
    fn validate_rejects_an_empty_key() {
        let txn = Txn::new().put("", Bytes::from_static(b"v"));
        assert_eq!(Err(LimitError::EmptyKey), txn.validate(&KvLimits::default()));
    }

    #[test]
    fn validate_rejects_an_empty_condition_key() {
        let txn = Txn::new().when(Condition::Exists { key: String::new() });
        assert_eq!(Err(LimitError::EmptyKey), txn.validate(&KvLimits::default()));
    }

    #[test]
    fn validate_rejects_an_empty_prefix() {
        let txn = Txn::new().delete_prefix("");
        assert_eq!(Err(LimitError::EmptyPrefix), txn.validate(&KvLimits::default()));
    }

    #[test]
    fn validate_accepts_a_key_exactly_at_the_limit() {
        let limits = KvLimits {
            max_key_bytes: 4,
            ..KvLimits::default()
        };
        let txn = Txn::new().put("abcd", Bytes::from_static(b"v"));
        assert_eq!(Ok(()), txn.validate(&limits));
    }

    #[test]
    fn validate_rejects_a_key_one_byte_over_the_limit() {
        let limits = KvLimits {
            max_key_bytes: 4,
            ..KvLimits::default()
        };
        let txn = Txn::new().put("abcde", Bytes::from_static(b"v"));
        assert_eq!(
            Err(LimitError::KeyTooLarge {
                key: "abcde".to_string(),
                actual: 5,
                max: 4,
            }),
            txn.validate(&limits)
        );
    }

    #[test]
    fn validate_accepts_a_value_exactly_at_the_limit() {
        let limits = KvLimits {
            max_value_bytes: 3,
            ..KvLimits::default()
        };
        let txn = Txn::new().put("k", Bytes::from_static(b"abc"));
        assert_eq!(Ok(()), txn.validate(&limits));
    }

    #[test]
    fn validate_rejects_a_value_one_byte_over_the_limit() {
        let limits = KvLimits {
            max_value_bytes: 3,
            ..KvLimits::default()
        };
        let txn = Txn::new().put("k", Bytes::from_static(b"abcd"));
        assert_eq!(
            Err(LimitError::ValueTooLarge {
                key: "k".to_string(),
                actual: 4,
                max: 3,
            }),
            txn.validate(&limits)
        );
    }

    #[test]
    fn validate_accepts_exactly_the_op_limit() {
        let limits = KvLimits {
            max_txn_ops: 2,
            ..KvLimits::default()
        };
        let txn = Txn::new().put("a", Bytes::new()).put("b", Bytes::new());
        assert_eq!(Ok(()), txn.validate(&limits));
    }

    #[test]
    fn validate_rejects_one_op_over_the_limit() {
        let limits = KvLimits {
            max_txn_ops: 2,
            ..KvLimits::default()
        };
        let txn = Txn::new().put("a", Bytes::new()).put("b", Bytes::new()).put("c", Bytes::new());
        assert_eq!(Err(LimitError::TooManyOps { actual: 3, max: 2 }), txn.validate(&limits));
    }

    #[test]
    fn validate_bounds_conditions_by_the_same_limit_as_ops_counted_on_their_own() {
        let limits = KvLimits {
            max_txn_ops: 2,
            ..KvLimits::default()
        };
        let not_exists = |key: &str| Condition::NotExists { key: key.to_string() };

        // Two conditions and two ops: each count is at its own limit, neither over it.
        let at_the_limit = Txn::new().when(not_exists("a")).when(not_exists("b")).put("a", Bytes::new()).delete("b");
        assert_eq!(Ok(()), at_the_limit.validate(&limits));

        let one_over = Txn::new().when(not_exists("a")).when(not_exists("b")).when(not_exists("c"));
        assert_eq!(
            Err(LimitError::TooManyConditions { actual: 3, max: 2 }),
            one_over.validate(&limits)
        );
    }

    #[test]
    fn validate_accepts_a_transaction_exactly_at_the_total_size_limit() {
        let limits = KvLimits {
            max_txn_bytes: 3,
            ..KvLimits::default()
        };
        // key "a" (1 byte) + value "bc" (2 bytes) = 3 bytes total.
        let txn = Txn::new().put("a", Bytes::from_static(b"bc"));
        assert_eq!(Ok(()), txn.validate(&limits));
    }

    #[test]
    fn validate_rejects_a_transaction_one_byte_over_the_total_size_limit() {
        let limits = KvLimits {
            max_txn_bytes: 3,
            ..KvLimits::default()
        };
        // key "a" (1 byte) + value "bcd" (3 bytes) = 4 bytes total.
        let txn = Txn::new().put("a", Bytes::from_static(b"bcd"));
        assert_eq!(Err(LimitError::TooLarge { actual: 4, max: 3 }), txn.validate(&limits));
    }

    #[test]
    fn validate_counts_condition_keys_toward_the_total_size() {
        let limits = KvLimits {
            max_txn_bytes: 2,
            ..KvLimits::default()
        };
        // condition key "ab" (2 bytes) alone is already at the limit; adding any op tips it over.
        let txn = Txn::new().when(Condition::Exists { key: "ab".to_string() }).delete("c");
        assert_eq!(Err(LimitError::TooLarge { actual: 3, max: 2 }), txn.validate(&limits));
    }

    #[test]
    fn validate_treats_an_oversized_prefix_like_an_oversized_key() {
        let limits = KvLimits {
            max_key_bytes: 3,
            ..KvLimits::default()
        };
        let txn = Txn::new().delete_prefix("abcd");
        assert_eq!(
            Err(LimitError::KeyTooLarge {
                key: "abcd".to_string(),
                actual: 4,
                max: 3,
            }),
            txn.validate(&limits)
        );
    }

    #[test]
    fn condition_key_reads_back_regardless_of_variant() {
        assert_eq!(
            "a",
            Condition::ModRevEq {
                key: "a".to_string(),
                mod_rev: 1
            }
            .key()
        );
        assert_eq!("b", Condition::NotExists { key: "b".to_string() }.key());
        assert_eq!("c", Condition::Exists { key: "c".to_string() }.key());
    }

    #[test]
    fn txn_serde_round_trips_in_json_and_postcard() {
        let txn = Txn::new()
            .when(Condition::ModRevEq {
                key: "a".to_string(),
                mod_rev: 3,
            })
            .put("b", Bytes::from_static(b"\x00\x01payload"))
            .delete("c")
            .delete_prefix("d/");

        let json = serde_json::to_string(&txn).unwrap();
        assert_eq!(txn, serde_json::from_str(&json).unwrap());

        let packed = postcard::to_allocvec(&txn).unwrap();
        assert_eq!(txn, postcard::from_bytes(&packed).unwrap());
    }

    #[test]
    fn limit_error_serde_round_trips_in_json_and_postcard() {
        let err = LimitError::KeyTooLarge {
            key: "abcde".to_string(),
            actual: 5,
            max: 4,
        };

        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(err, serde_json::from_str(&json).unwrap());

        let packed = postcard::to_allocvec(&err).unwrap();
        assert_eq!(err, postcard::from_bytes(&packed).unwrap());
    }

    #[test]
    fn conflict_serde_round_trips_in_json_and_postcard() {
        let conflict = Conflict {
            index: 1,
            condition: Condition::Exists { key: "a".to_string() },
            key: "a".to_string(),
            current_mod_rev: 0,
        };

        let json = serde_json::to_string(&conflict).unwrap();
        assert_eq!(conflict, serde_json::from_str(&json).unwrap());

        let packed = postcard::to_allocvec(&conflict).unwrap();
        assert_eq!(conflict, postcard::from_bytes(&packed).unwrap());
    }
}
