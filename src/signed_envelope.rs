//! `signed_envelope` — cryptographic envelope over queue messages.
//!
//! Wraps queue payloads with an `Ed25519` producer signature and a
//! monotonic per-topic sequence number, giving downstream consumers three
//! properties in one primitive:
//!
//! 1. **Non-repudiation** — the producer cannot deny having sent the
//!    message (signature bound to `producer_id`).
//! 2. **Integrity** — any tamper of payload / topic / seq breaks the
//!    `FNV-1a` hash inside the signature payload.
//! 3. **Idempotency** — `(producer_id, seq)` uniquely identifies the
//!    message, so consumers deduplicate at-least-once deliveries safely.
//!
//! Complements the existing lock-free [`crate::ring`] and WAL
//! [`crate::journal`]: those move bytes fast, this proves *who* wrote them.
//!
//! # Regulatory alignment
//!
//! - **`MiFID-II RTS 25` Art. 4** — clock-synchronized message audit
//!   trail; combined with `alice-blockchain::timestamp` RFC 3161 tokens,
//!   satisfies the 1 µs granularity requirement.
//! - **`ISO 20022`** — payment message flows require producer attestation.
//! - **`SOC2 CC7.4`** — the entity monitors changes; every ingest event
//!   is attributable to the signing producer.
//!
//! Cryptographic primitives are provided by `alice-blockchain` (`Ed25519`).

#![allow(
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::cast_possible_wrap
)]

use alice_blockchain::signature::{KeyPair, PublicKey, Signature};

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

/// One queue message ready to be signed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// Producer identifier (service name, tenant, or DID URI).
    pub producer_id: String,
    /// Monotonic per-producer sequence number.
    pub seq: u64,
    /// Topic (or partition key) the message is routed on.
    pub topic: String,
    /// Unix nanosecond timestamp when the producer created the envelope.
    pub timestamp_ns: u64,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

impl Envelope {
    /// Canonical byte layout used for hashing and signing.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64 + self.payload.len());
        buf.extend_from_slice(self.producer_id.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(self.topic.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u64).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// `FNV-1a` hash of the canonical byte layout.
    #[must_use]
    pub fn hash(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in &self.canonical_bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }

    /// Message identifier used for deduplication: `(producer_id, seq)`.
    #[must_use]
    pub fn dedup_key(&self) -> (String, u64) {
        (self.producer_id.clone(), self.seq)
    }
}

// ---------------------------------------------------------------------------
// SignedEnvelope
// ---------------------------------------------------------------------------

/// An [`Envelope`] plus the producer's `Ed25519` signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedEnvelope {
    /// The wrapped envelope.
    pub envelope: Envelope,
    /// `FNV-1a` hash of the envelope's canonical bytes.
    pub hash: u64,
    /// `Ed25519` signature over the canonical bytes.
    pub signature: Signature,
    /// Producer's `Ed25519` public key.
    pub producer: PublicKey,
}

impl SignedEnvelope {
    /// Create a signed envelope from raw fields.
    #[must_use]
    pub fn sign(
        keypair: &KeyPair,
        producer_id: impl Into<String>,
        seq: u64,
        topic: impl Into<String>,
        timestamp_ns: u64,
        payload: Vec<u8>,
    ) -> Self {
        let envelope = Envelope {
            producer_id: producer_id.into(),
            seq,
            topic: topic.into(),
            timestamp_ns,
            payload,
        };
        let bytes = envelope.canonical_bytes();
        let hash = envelope.hash();
        let signature = keypair.sign(&bytes);
        let producer = keypair.public();
        Self {
            envelope,
            hash,
            signature,
            producer,
        }
    }

    /// Verify the signature and hash consistency.
    #[must_use]
    pub fn verify(&self) -> bool {
        if self.hash != self.envelope.hash() {
            return false;
        }
        self.producer
            .verify(&self.envelope.canonical_bytes(), &self.signature)
    }
}

// ---------------------------------------------------------------------------
// DedupBuffer
// ---------------------------------------------------------------------------

/// Sliding-window deduplication buffer for at-least-once delivery.
///
/// Tracks recently observed `(producer_id, seq)` keys and rejects
/// duplicates. The buffer is bounded — once `capacity` is reached, the
/// oldest key is evicted to make room. This gives O(1) memory with a
/// predictable trade-off between memory and deduplication window.
#[derive(Debug, Clone)]
pub struct DedupBuffer {
    keys: Vec<(String, u64)>,
    capacity: usize,
}

impl DedupBuffer {
    /// Construct a dedup buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Try to accept a signed envelope. Returns `true` if the envelope is
    /// new (and records the key), `false` if it is a duplicate.
    pub fn accept(&mut self, envelope: &SignedEnvelope) -> bool {
        let key = envelope.envelope.dedup_key();
        if self.keys.contains(&key) {
            return false;
        }
        if self.keys.len() >= self.capacity && !self.keys.is_empty() {
            self.keys.remove(0);
        }
        self.keys.push(key);
        true
    }

    /// Current number of tracked keys.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the buffer holds no keys.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The configured maximum number of tracked keys.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn kp(seed: u8) -> KeyPair {
        KeyPair::from_seed([seed; 32])
    }

    #[test]
    fn canonical_bytes_are_deterministic() {
        let env = Envelope {
            producer_id: String::from("svc-A"),
            seq: 1,
            topic: String::from("orders"),
            timestamp_ns: 1_000_000,
            payload: vec![1, 2, 3, 4],
        };
        assert_eq!(env.canonical_bytes(), env.canonical_bytes());
    }

    #[test]
    fn hash_differs_when_payload_changes() {
        let mut env = Envelope {
            producer_id: String::from("svc-A"),
            seq: 1,
            topic: String::from("orders"),
            timestamp_ns: 1,
            payload: vec![1, 2, 3],
        };
        let h1 = env.hash();
        env.payload.push(99);
        assert_ne!(h1, env.hash());
    }

    #[test]
    fn hash_differs_when_seq_changes() {
        let mut env = Envelope {
            producer_id: String::from("svc-A"),
            seq: 1,
            topic: String::from("orders"),
            timestamp_ns: 1,
            payload: vec![1, 2, 3],
        };
        let h1 = env.hash();
        env.seq = 2;
        assert_ne!(h1, env.hash());
    }

    #[test]
    fn dedup_key_is_producer_id_and_seq() {
        let env = Envelope {
            producer_id: String::from("svc-A"),
            seq: 42,
            topic: String::from("t"),
            timestamp_ns: 0,
            payload: vec![],
        };
        assert_eq!(env.dedup_key(), (String::from("svc-A"), 42));
    }

    #[test]
    fn signed_envelope_verifies() {
        let k = kp(1);
        let signed = SignedEnvelope::sign(&k, "svc-A", 1, "orders", 1, vec![1, 2, 3]);
        assert!(signed.verify());
    }

    #[test]
    fn tampered_payload_breaks_verify() {
        let k = kp(1);
        let mut signed = SignedEnvelope::sign(&k, "svc-A", 1, "orders", 1, vec![1, 2, 3]);
        signed.envelope.payload.push(99);
        assert!(!signed.verify());
    }

    #[test]
    fn tampered_topic_breaks_verify() {
        let k = kp(1);
        let mut signed = SignedEnvelope::sign(&k, "svc-A", 1, "orders", 1, vec![1, 2, 3]);
        signed.envelope.topic = String::from("attacker");
        assert!(!signed.verify());
    }

    #[test]
    fn tampered_seq_breaks_verify() {
        let k = kp(1);
        let mut signed = SignedEnvelope::sign(&k, "svc-A", 1, "orders", 1, vec![1, 2, 3]);
        signed.envelope.seq = 99;
        assert!(!signed.verify());
    }

    #[test]
    fn foreign_signature_is_rejected() {
        let owner = kp(1);
        let attacker = kp(2);
        let mut signed = SignedEnvelope::sign(&owner, "svc-A", 1, "orders", 1, vec![1, 2, 3]);
        let bytes = signed.envelope.canonical_bytes();
        signed.signature = attacker.sign(&bytes);
        assert!(!signed.verify());
    }

    #[test]
    fn dedup_buffer_accepts_novel_key() {
        let k = kp(1);
        let mut dedup = DedupBuffer::new(4);
        let e = SignedEnvelope::sign(&k, "svc-A", 1, "t", 1, vec![]);
        assert!(dedup.accept(&e));
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn dedup_buffer_rejects_duplicate() {
        let k = kp(1);
        let mut dedup = DedupBuffer::new(4);
        let e = SignedEnvelope::sign(&k, "svc-A", 1, "t", 1, vec![]);
        assert!(dedup.accept(&e));
        assert!(!dedup.accept(&e));
    }

    #[test]
    fn dedup_buffer_accepts_different_seq() {
        let k = kp(1);
        let mut dedup = DedupBuffer::new(4);
        let e1 = SignedEnvelope::sign(&k, "svc-A", 1, "t", 1, vec![]);
        let e2 = SignedEnvelope::sign(&k, "svc-A", 2, "t", 1, vec![]);
        assert!(dedup.accept(&e1));
        assert!(dedup.accept(&e2));
        assert_eq!(dedup.len(), 2);
    }

    #[test]
    fn dedup_buffer_evicts_oldest_when_full() {
        let k = kp(1);
        let mut dedup = DedupBuffer::new(2);
        let e1 = SignedEnvelope::sign(&k, "svc-A", 1, "t", 1, vec![]);
        let e2 = SignedEnvelope::sign(&k, "svc-A", 2, "t", 1, vec![]);
        let e3 = SignedEnvelope::sign(&k, "svc-A", 3, "t", 1, vec![]);
        assert!(dedup.accept(&e1));
        assert!(dedup.accept(&e2));
        assert!(dedup.accept(&e3));
        // e1 evicted, so re-submitting should now be accepted again.
        assert!(dedup.accept(&e1));
        assert_eq!(dedup.len(), 2);
    }

    #[test]
    fn dedup_buffer_capacity_accessor() {
        let dedup = DedupBuffer::new(42);
        assert_eq!(dedup.capacity(), 42);
        assert!(dedup.is_empty());
    }

    #[test]
    fn different_producers_have_disjoint_dedup_space() {
        let k1 = kp(1);
        let k2 = kp(2);
        let mut dedup = DedupBuffer::new(4);
        let a = SignedEnvelope::sign(&k1, "svc-A", 1, "t", 1, vec![]);
        let b = SignedEnvelope::sign(&k2, "svc-B", 1, "t", 1, vec![]);
        assert!(dedup.accept(&a));
        assert!(dedup.accept(&b));
    }
}
