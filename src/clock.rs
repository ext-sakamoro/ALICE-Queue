//! Vector Clock for Causal Ordering
//!
//! **Optimizations**:
//! - Fixed-size inline array (no heap allocation)
//! - Compact serialization
//! - O(N) merge where N = number of nodes
//!
//! > "Time is just an increasing integer."

use core::cmp::Ordering;

/// Maximum number of nodes in the vector clock
pub const MAX_NODES: usize = 16;

/// Vector Clock for causal ordering
///
/// Each node maintains a counter. The vector represents "what this node knows".
/// If VC[i] = 5, it means this node has seen 5 events from node i.
#[derive(Clone, Copy, Debug)]
pub struct VectorClock {
    /// Clock values for each node
    clocks: [u64; MAX_NODES],
    /// This node's ID
    node_id: u8,
    /// Number of active nodes
    num_nodes: u8,
}

impl VectorClock {
    /// Create a new vector clock for a node
    pub fn new(node_id: u8, num_nodes: u8) -> Self {
        assert!((node_id as usize) < MAX_NODES, "Node ID too large");
        assert!((num_nodes as usize) <= MAX_NODES, "Too many nodes");

        Self {
            clocks: [0; MAX_NODES],
            node_id,
            num_nodes,
        }
    }

    /// Increment local clock (on send or local event)
    #[inline]
    pub fn tick(&mut self) -> u64 {
        self.clocks[self.node_id as usize] += 1;
        self.clocks[self.node_id as usize]
    }

    /// Get current local time
    #[inline]
    pub fn local_time(&self) -> u64 {
        self.clocks[self.node_id as usize]
    }

    /// Get time for a specific node
    #[inline]
    pub fn time_for(&self, node_id: u8) -> u64 {
        self.clocks[node_id as usize]
    }

    /// Merge with another clock (on receive)
    ///
    /// VC_merged[i] = max(VC_self[i], VC_other[i])
    #[inline]
    pub fn merge(&mut self, other: &VectorClock) {
        for i in 0..self.num_nodes as usize {
            self.clocks[i] = self.clocks[i].max(other.clocks[i]);
        }
        // Increment local clock after merge
        self.tick();
    }

    /// Compare two vector clocks for causality
    ///
    /// Returns:
    /// - `Less`: self happened-before other
    /// - `Greater`: other happened-before self
    /// - `Equal`: same event
    /// - `None`: concurrent (incomparable)
    pub fn compare(&self, other: &VectorClock) -> Option<Ordering> {
        let mut less = false;
        let mut greater = false;

        for i in 0..self.num_nodes.max(other.num_nodes) as usize {
            let a = self.clocks[i];
            let b = other.clocks[i];

            if a < b {
                less = true;
            } else if a > b {
                greater = true;
            }

            // Early exit if concurrent
            if less && greater {
                return None;
            }
        }

        match (less, greater) {
            (false, false) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (true, true) => None, // Concurrent
        }
    }

    /// Check if self happened-before other
    #[inline]
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), Some(Ordering::Less))
    }

    /// Check if events are concurrent
    #[inline]
    pub fn is_concurrent_with(&self, other: &VectorClock) -> bool {
        self.compare(other).is_none()
    }

    /// Serialize to bytes (compact format)
    ///
    /// Format: [num_nodes: u8][node_id: u8][clocks: u64 * num_nodes]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.num_nodes as usize * 8);
        bytes.push(self.num_nodes);
        bytes.push(self.node_id);
        for i in 0..self.num_nodes as usize {
            bytes.extend_from_slice(&self.clocks[i].to_le_bytes());
        }
        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }

        let num_nodes = bytes[0];
        let node_id = bytes[1];

        if num_nodes as usize > MAX_NODES {
            return None;
        }

        let expected_len = 2 + num_nodes as usize * 8;
        if bytes.len() < expected_len {
            return None;
        }

        let mut clocks = [0u64; MAX_NODES];
        for i in 0..num_nodes as usize {
            let offset = 2 + i * 8;
            clocks[i] = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
        }

        Some(Self {
            clocks,
            node_id,
            num_nodes,
        })
    }

    /// Size in bytes when serialized
    #[inline]
    pub fn serialized_size(&self) -> usize {
        2 + self.num_nodes as usize * 8
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new(0, 1)
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.compare(other), Some(Ordering::Equal))
    }
}

/// Hybrid Logical Clock (HLC)
///
/// Combines physical time with logical counters for bounded skew.
/// Useful when you want timestamps that are:
/// 1. Monotonically increasing
/// 2. Close to real time
/// 3. Causally consistent
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HybridClock {
    /// Physical time (milliseconds since epoch)
    physical: u64,
    /// Logical counter (for events at same physical time)
    logical: u32,
    /// Node ID (for tie-breaking)
    node_id: u16,
}

impl HybridClock {
    /// Create new HLC with current time
    pub fn now(node_id: u16) -> Self {
        Self {
            physical: Self::current_millis(),
            logical: 0,
            node_id,
        }
    }

    /// Tick for local event
    pub fn tick(&mut self) -> Self {
        let now = Self::current_millis();

        if now > self.physical {
            self.physical = now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }

        *self
    }

    /// Update on receive (merge with sender's clock)
    pub fn receive(&mut self, sender: &HybridClock) -> Self {
        let now = Self::current_millis();

        if now > self.physical && now > sender.physical {
            self.physical = now;
            self.logical = 0;
        } else if self.physical > sender.physical {
            self.logical += 1;
        } else if sender.physical > self.physical {
            self.physical = sender.physical;
            self.logical = sender.logical + 1;
        } else {
            // Same physical time
            self.logical = self.logical.max(sender.logical) + 1;
        }

        *self
    }

    /// Serialize to bytes (16 bytes total)
    pub fn to_bytes(&self) -> [u8; 14] {
        let mut bytes = [0u8; 14];
        bytes[0..8].copy_from_slice(&self.physical.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.logical.to_le_bytes());
        bytes[12..14].copy_from_slice(&self.node_id.to_le_bytes());
        bytes
    }

    /// Deserialize from bytes.
    ///
    /// The argument is a fixed-size `[u8; 14]` reference, so every sub-slice
    /// has a statically-known length; we index directly to avoid any runtime
    /// failure path that `try_into().unwrap()` would introduce.
    pub fn from_bytes(bytes: &[u8; 14]) -> Self {
        Self {
            physical: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            logical: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            node_id: u16::from_le_bytes([bytes[12], bytes[13]]),
        }
    }

    #[cfg(feature = "std")]
    fn current_millis() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        // `duration_since` returns Err only if the system clock is set before
        // the Unix epoch — a misconfigured host. Fall back to 0 rather than
        // panicking, which lets the logical counter continue to provide
        // monotonicity.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    #[cfg(not(feature = "std"))]
    fn current_millis() -> u64 {
        // In no_std, caller must provide time
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_basic() {
        let mut vc1 = VectorClock::new(0, 3);
        let mut vc2 = VectorClock::new(1, 3);

        // Local events
        vc1.tick();
        vc1.tick();
        assert_eq!(vc1.local_time(), 2);

        vc2.tick();
        assert_eq!(vc2.local_time(), 1);

        // vc1 and vc2 are concurrent (no message exchange)
        assert!(vc1.is_concurrent_with(&vc2));
    }

    #[test]
    fn test_vector_clock_causality() {
        let mut vc1 = VectorClock::new(0, 2);
        let mut vc2 = VectorClock::new(1, 2);

        // vc1 sends to vc2
        vc1.tick(); // vc1 = [1, 0]
        let msg_clock = vc1;

        vc2.merge(&msg_clock); // vc2 = [1, 1] (merged then ticked)

        // vc1 happened-before vc2
        assert!(vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
    }

    #[test]
    fn test_vector_clock_serialization() {
        let mut vc = VectorClock::new(2, 4);
        vc.tick();
        vc.tick();
        vc.tick();

        let bytes = vc.to_bytes();
        let vc2 = VectorClock::from_bytes(&bytes).unwrap();

        assert_eq!(vc.local_time(), vc2.local_time());
        assert_eq!(vc.compare(&vc2), Some(Ordering::Equal));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new(0, 2);
        let mut vc2 = VectorClock::new(1, 2);

        // Both tick independently
        vc1.tick(); // [1, 0]
        vc2.tick(); // [0, 1]

        // They are concurrent
        assert!(vc1.is_concurrent_with(&vc2));
        assert!(vc2.is_concurrent_with(&vc1));
        assert_eq!(vc1.compare(&vc2), None);
    }

    #[test]
    fn test_hlc_basic() {
        let mut hlc1 = HybridClock::now(1);
        let mut hlc2 = HybridClock::now(2);

        // Tick locally
        let t1 = hlc1.tick();
        let t2 = hlc1.tick();
        assert!(t2 > t1);

        // Receive message
        let t3 = hlc2.receive(&hlc1);
        assert!(t3 > hlc1);
    }

    #[test]
    fn test_hlc_serialization() {
        let hlc = HybridClock::now(42);
        let bytes = hlc.to_bytes();
        let hlc2 = HybridClock::from_bytes(&bytes);
        assert_eq!(hlc, hlc2);
    }
}
