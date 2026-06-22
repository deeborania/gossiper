//! Randomness abstractions for deterministic protocol behavior.

/// Source of pseudo-random values used by the protocol.
///
/// The protocol core accepts randomness through this trait so tests and
/// simulations can be deterministic.
pub trait RandomSource {
    /// Returns the next pseudo-random `u64`.
    fn next_u64(&mut self) -> u64;

    /// Returns a pseudo-random index in `0..len`.
    ///
    /// Returns `None` when `len` is zero.
    fn index(&mut self, len: usize) -> Option<usize> {
        if len == 0 {
            return None;
        }

        Some((self.next_u64() as usize) % len)
    }
}

/// A tiny deterministic random source for tests and simulations.
///
/// This is not cryptographically secure. It is only intended to make protocol
/// behavior reproducible.
#[derive(Clone, Debug)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    /// Creates a deterministic RNG from a seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl RandomSource for DeterministicRng {
    fn next_u64(&mut self) -> u64 {
        // Constants from Numerical Recipes. Good enough for deterministic tests,
        // not suitable for cryptography or serious statistical simulation.
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);

        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::{DeterministicRng, RandomSource};

    #[test]
    fn deterministic_rng_repeats_for_same_seed() {
        let mut a = DeterministicRng::new(7);
        let mut b = DeterministicRng::new(7);

        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn index_returns_none_for_empty_range() {
        let mut rng = DeterministicRng::new(1);

        assert_eq!(rng.index(0), None);
    }

    #[test]
    fn index_returns_value_inside_range() {
        let mut rng = DeterministicRng::new(1);

        for _ in 0..100 {
            let index = rng.index(5).expect("non-empty range should produce index");
            assert!(index < 5);
        }
    }
}
