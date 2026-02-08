//! ALICE-Queue × ALICE-Crypto Bridge
//!
//! Encrypted message queue: seal payloads with XChaCha20-Poly1305 before
//! enqueue, unseal after dequeue. BLAKE3 integrity verification.

use alice_crypto::{Key, seal, open, hash, Hash};
use crate::message::{Message, SenderKey};

/// Encrypted message wrapper.
pub struct EncryptedQueue {
    key: Key,
}

impl EncryptedQueue {
    /// Create an encrypted queue with the given key.
    pub fn new(key: Key) -> Self {
        Self { key }
    }

    /// Create with a randomly generated key.
    pub fn generate() -> Result<Self, &'static str> {
        let key = Key::generate().map_err(|_| "key generation failed")?;
        Ok(Self { key })
    }

    /// Seal a message's payload with XChaCha20-Poly1305.
    ///
    /// Returns a new Message with encrypted payload.
    pub fn seal_message(&self, msg: &Message) -> Result<Message, alice_crypto::CipherError> {
        let ciphertext = seal(&self.key, &msg.payload)?;
        Ok(Message::new(msg.header.sender, msg.header.seq, ciphertext))
    }

    /// Unseal a message's encrypted payload.
    ///
    /// Returns a new Message with decrypted payload.
    pub fn open_message(&self, msg: &Message) -> Result<Message, alice_crypto::CipherError> {
        let plaintext = open(&self.key, &msg.payload)?;
        Ok(Message::new(msg.header.sender, msg.header.seq, plaintext))
    }

    /// Compute BLAKE3 integrity hash of a message payload.
    pub fn payload_hash(msg: &Message) -> Hash {
        hash(&msg.payload)
    }

    /// Verify payload integrity against a known hash.
    pub fn verify_integrity(msg: &Message, expected: &Hash) -> bool {
        let actual = hash(&msg.payload);
        actual.as_bytes() == expected.as_bytes()
    }

    /// Get a reference to the encryption key.
    pub fn key(&self) -> &Key { &self.key }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sender() -> SenderKey {
        let mut key = [0u8; 32];
        key[0] = 42;
        key
    }

    #[test]
    fn test_seal_open_roundtrip() {
        let eq = EncryptedQueue::generate().unwrap();
        let msg = Message::new(test_sender(), 1, b"secret payload".to_vec());

        let sealed = eq.seal_message(&msg).unwrap();
        // Sealed payload should differ from original
        assert_ne!(sealed.payload, msg.payload);

        let opened = eq.open_message(&sealed).unwrap();
        assert_eq!(opened.payload, b"secret payload");
        assert_eq!(opened.header.seq, 1);
    }

    #[test]
    fn test_integrity_verification() {
        let msg = Message::new(test_sender(), 1, b"data".to_vec());
        let h = EncryptedQueue::payload_hash(&msg);
        assert!(EncryptedQueue::verify_integrity(&msg, &h));

        // Tampered message should fail
        let tampered = Message::new(test_sender(), 1, b"tampered".to_vec());
        assert!(!EncryptedQueue::verify_integrity(&tampered, &h));
    }
}
