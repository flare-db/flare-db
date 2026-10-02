//! Beam Fn user-state semantics.
//!
//! This module owns the Beam-specific logic layered on top of the opaque
//! key/value primitives in [`crate::store::element_store::FlareElementStore`].
//! Each state variant interprets the shared [`build_state_key`] composite
//! according to its Beam semantics; e.g. [`bag::BagUserState`] concatenates
//! appended chunks into an ordered bag.
//!
//! The gRPC transport and `BeamFnState` protocol dispatch live in the `harness`
//! module (`engine/harness/state.rs`); this module is transport-agnostic.

pub mod backend;
pub mod bag;

pub use backend::StateBackend;
pub use bag::BagState;

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
}
