//! Beam Fn user-state semantics.
//!
//! This module owns the Beam-specific logic layered on top of the opaque
//! key/value primitives in [`crate::store::element_store::FlareElementStore`].
//! The harness decodes the Fn API `StateKey` proto into a transport-agnostic
//! [`UserStateAddress`], and [`UserStateStore`] dispatches the operation to the
//! implementation for that state kind. Each variant interprets the shared
//! [`build_state_key`] composite according to its Beam semantics; e.g.
//! [`bag::BagState`] concatenates appended chunks into an ordered bag.
//!
//! The gRPC transport and `BeamFnState` protocol dispatch live in the `harness`
//! module (`engine/harness/state.rs`); this module is transport-agnostic.

pub mod backend;
pub mod bag;

pub use backend::StateBackend;
pub use bag::BagState;

use anyhow::Result;

/// Deterministic composite key for a user-state entry.
///
/// Every component is length-prefixed (4-byte big-endian) so distinct
/// `(transform_id, user_state_id, window, key)` tuples always map to distinct,
/// collision-free keys, and raw binary window/key bytes cannot be ambiguous.
pub fn build_state_key(
    transform_id: &str,
    user_state_id: &str,
    window: &[u8],
    key: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        4 * 4 + transform_id.len() + user_state_id.len() + window.len() + key.len(),
    );
    for part in [
        transform_id.as_bytes(),
        user_state_id.as_bytes(),
        window,
        key,
    ] {
        out.extend_from_slice(&(part.len() as u32).to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// A Beam Fn user-state kind FlareDB can serve.
///
/// The portable Fn API also defines `MultimapUserState`, `OrderedListUserState`,
/// and side-input keys; those are not implemented yet and are rejected by the
/// harness before reaching this layer.
///
/// Note that the SDK harness backs `ValueState`, `CombiningState`, and
/// `SetState` with `BagUserState` on the wire (for example
/// `BagUserState.java` asserts `stateKey.hasBagUserState()`), so `Bag` is the
/// only kind needed to serve those DoFn state types today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserStateKind {
    /// `StateKey.BagUserState`.
    Bag,
}

impl UserStateKind {
    /// The `beam_fn_api::StateKey` variant name, for diagnostics and errors.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Bag => "bag_user_state",
        }
    }
}

/// Transport-agnostic address of a Beam user-state cell.
///
/// Decoding the Fn API `StateKey` proto into this type keeps the proto out of
/// the state layer, mirroring how the harness owns transport framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserStateAddress {
    /// Which Beam user-state kind this cell belongs to.
    pub kind: UserStateKind,
    /// Id of the `PTransform` that owns the `StateSpec`.
    pub transform_id: String,
    /// The `ParDoPayload.state_specs` local name identifying this state.
    pub user_state_id: String,
    /// The window encoded in a nested context, exactly as received.
    pub window: Vec<u8>,
    /// The user key encoded in a nested context, exactly as received.
    pub key: Vec<u8>,
}

impl UserStateAddress {
    /// Build an address from its decoded components.
    pub fn new(
        kind: UserStateKind,
        transform_id: String,
        user_state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            transform_id,
            user_state_id,
            window,
            key,
        }
    }

    /// The opaque composite Paimon key addressing this cell.
    pub fn composite_key(&self) -> Vec<u8> {
        build_state_key(
            &self.transform_id,
            &self.user_state_id,
            &self.window,
            &self.key,
        )
    }
}

/// Dispatches a Beam user-state operation to the implementation for its kind.
///
/// This is the boundary between the Fn State request handler and the
/// Paimon-backed store: the harness decodes a `StateKey` into a
/// [`UserStateAddress`], and this type interprets it according to Beam state
/// semantics.
pub struct UserStateStore {
    backend: StateBackend,
    address: UserStateAddress,
}

impl UserStateStore {
    /// Bind `address` to `backend`.
    ///
    /// The backend is cheap to clone (it wraps shared handles), so a store may
    /// be constructed per request.
    pub fn new(backend: StateBackend, address: UserStateAddress) -> Self {
        Self { backend, address }
    }

    /// The address this store is bound to.
    pub fn address(&self) -> &UserStateAddress {
        &self.address
    }

    /// Read the cell's value, or empty when unset.
    ///
    /// For bag state this is the concatenation of all appended chunks, matching
    /// the Fn State `get` contract.
    pub async fn get(&self) -> Result<Vec<u8>> {
        let key = self.address.composite_key();
        match self.address.kind {
            UserStateKind::Bag => BagState::new(self.backend.clone()).get(&key).await,
        }
    }

    /// Append `data` to the cell (bag semantics).
    pub async fn append(&self, data: &[u8]) -> Result<()> {
        let key = self.address.composite_key();
        match self.address.kind {
            UserStateKind::Bag => BagState::new(self.backend.clone()).append(&key, data).await,
        }
    }

    /// Clear the cell.
    pub async fn clear(&self) -> Result<()> {
        let key = self.address.composite_key();
        match self.address.kind {
            UserStateKind::Bag => BagState::new(self.backend.clone()).clear(&key).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_state_key_is_deterministic_and_collision_free() {
        let a = build_state_key("t", "s", b"w", b"k");
        let b = build_state_key("t", "s", b"w", b"k");
        assert_eq!(a, b);

        // Different components must produce different keys.
        assert_ne!(a, build_state_key("t2", "s", b"w", b"k"));
        assert_ne!(a, build_state_key("t", "s2", b"w", b"k"));
        assert_ne!(a, build_state_key("t", "s", b"w2", b"k"));
        assert_ne!(a, build_state_key("t", "s", b"w", b"k2"));

        // Length-prefixing prevents ambiguous concatenations.
        assert_ne!(
            build_state_key("ab", "c", b"", b""),
            build_state_key("a", "bc", b"", b"")
        );
    }

    #[test]
    fn user_state_address_composite_key_matches_build_state_key() {
        let address = UserStateAddress::new(
            UserStateKind::Bag,
            "transform".to_string(),
            "state".to_string(),
            b"window".to_vec(),
            b"key".to_vec(),
        );

        assert_eq!(
            address.composite_key(),
            build_state_key("transform", "state", b"window", b"key")
        );
        assert_eq!(address.kind.wire_name(), "bag_user_state");
    }
}
