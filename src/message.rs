//! Message Format & Deterministic ID
//!
//! **Deep Fried Optimizations**:
//! - BLAKE3 for fast, deterministic message ID
//! - Zero-copy serialization where possible
//! - Compact wire format
//!
//! > "Message ID = BLAKE3(sender_pubkey + seq + payload)"

use crate::clock::VectorClock;

/// Message ID (BLAKE3 hash, 32 bytes)
pub type MessageId = [u8; 32];

/// Sender public key (or identifier)
pub type SenderKey = [u8; 32];

/// Message header (fixed size for predictable parsing)
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MessageHeader {
    /// Message ID (BLAKE3 hash)
    pub id: MessageId,
    /// Sender public key / identifier
    pub sender: SenderKey,
    /// Sequence number (monotonic per sender)
    pub seq: u64,
    /// Payload length
    pub payload_len: u32,
    /// Flags (reserved)
    pub flags: u16,
    /// Vector clock size (0 if not included)
    pub vclock_size: u8,
    /// Reserved
    pub _reserved: u8,
}

impl MessageHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Message flags
pub mod flags {
    pub const NONE: u16 = 0;
    pub const HAS_VCLOCK: u16 = 1 << 0;
    pub const COMPRESSED: u16 = 1 << 1;
    pub const ENCRYPTED: u16 = 1 << 2;
    pub const REQUIRES_ACK: u16 = 1 << 3;
}

/// Complete message with header, optional vector clock, and payload
#[derive(Clone, Debug)]
pub struct Message {
    /// Header
    pub header: MessageHeader,
    /// Vector clock (optional)
    pub vclock: Option<VectorClock>,
    /// Payload data
    pub payload: Vec<u8>,
}

impl Message {
    /// Create a new message
    pub fn new(sender: SenderKey, seq: u64, payload: Vec<u8>) -> Self {
        let id = Self::compute_id(&sender, seq, &payload);

        Self {
            header: MessageHeader {
                id,
                sender,
                seq,
                payload_len: payload.len() as u32,
                flags: flags::NONE,
                vclock_size: 0,
                _reserved: 0,
            },
            vclock: None,
            payload,
        }
    }

    /// Create message with vector clock
    pub fn with_vclock(sender: SenderKey, seq: u64, payload: Vec<u8>, vclock: VectorClock) -> Self {
        let id = Self::compute_id(&sender, seq, &payload);

        Self {
            header: MessageHeader {
                id,
                sender,
                seq,
                payload_len: payload.len() as u32,
                flags: flags::HAS_VCLOCK,
                vclock_size: vclock.serialized_size() as u8,
                _reserved: 0,
            },
            vclock: Some(vclock),
            payload,
        }
    }

    /// Compute deterministic message ID
    ///
    /// ID = BLAKE3(sender || seq || payload)
    #[inline]
    pub fn compute_id(sender: &SenderKey, seq: u64, payload: &[u8]) -> MessageId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(sender);
        hasher.update(&seq.to_le_bytes());
        hasher.update(payload);
        *hasher.finalize().as_bytes()
    }

    /// Verify message ID is correct
    #[inline]
    pub fn verify_id(&self) -> bool {
        let expected = Self::compute_id(&self.header.sender, self.header.seq, &self.payload);
        self.header.id == expected
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let vclock_bytes = self.vclock.as_ref().map(|vc| vc.to_bytes());
        let vclock_len = vclock_bytes.as_ref().map(|b| b.len()).unwrap_or(0);

        let total_len = MessageHeader::SIZE + vclock_len + self.payload.len();
        let mut bytes = Vec::with_capacity(total_len);

        // Write header
        bytes.extend_from_slice(&self.header.id);
        bytes.extend_from_slice(&self.header.sender);
        bytes.extend_from_slice(&self.header.seq.to_le_bytes());
        bytes.extend_from_slice(&self.header.payload_len.to_le_bytes());
        bytes.extend_from_slice(&self.header.flags.to_le_bytes());
        bytes.push(self.header.vclock_size);
        bytes.push(self.header._reserved);

        // Write vector clock if present
        if let Some(ref vc_bytes) = vclock_bytes {
            bytes.extend_from_slice(vc_bytes);
        }

        // Write payload
        bytes.extend_from_slice(&self.payload);

        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < MessageHeader::SIZE {
            return None;
        }

        // Parse header
        let id: MessageId = bytes[0..32].try_into().ok()?;
        let sender: SenderKey = bytes[32..64].try_into().ok()?;
        let seq = u64::from_le_bytes(bytes[64..72].try_into().ok()?);
        let payload_len = u32::from_le_bytes(bytes[72..76].try_into().ok()?);
        let flags = u16::from_le_bytes(bytes[76..78].try_into().ok()?);
        let vclock_size = bytes[78];
        let _reserved = bytes[79];

        let header = MessageHeader {
            id,
            sender,
            seq,
            payload_len,
            flags,
            vclock_size,
            _reserved,
        };

        let mut offset = MessageHeader::SIZE;

        // Parse vector clock if present
        let vclock = if vclock_size > 0 {
            let vc_end = offset + vclock_size as usize;
            if bytes.len() < vc_end {
                return None;
            }
            let vc = VectorClock::from_bytes(&bytes[offset..vc_end])?;
            offset = vc_end;
            Some(vc)
        } else {
            None
        };

        // Parse payload
        let payload_end = offset + payload_len as usize;
        if bytes.len() < payload_end {
            return None;
        }
        let payload = bytes[offset..payload_end].to_vec();

        Some(Self {
            header,
            vclock,
            payload,
        })
    }

    /// Total serialized size
    pub fn serialized_size(&self) -> usize {
        MessageHeader::SIZE + self.header.vclock_size as usize + self.payload.len()
    }
}

/// Message builder for ergonomic construction
pub struct MessageBuilder {
    sender: SenderKey,
    seq: u64,
    payload: Vec<u8>,
    vclock: Option<VectorClock>,
    flags: u16,
}

impl MessageBuilder {
    pub fn new(sender: SenderKey, seq: u64) -> Self {
        Self {
            sender,
            seq,
            payload: Vec::new(),
            vclock: None,
            flags: flags::NONE,
        }
    }

    pub fn payload(mut self, data: Vec<u8>) -> Self {
        self.payload = data;
        self
    }

    pub fn vclock(mut self, vc: VectorClock) -> Self {
        self.vclock = Some(vc);
        self.flags |= flags::HAS_VCLOCK;
        self
    }

    pub fn requires_ack(mut self) -> Self {
        self.flags |= flags::REQUIRES_ACK;
        self
    }

    pub fn build(self) -> Message {
        let id = Message::compute_id(&self.sender, self.seq, &self.payload);

        Message {
            header: MessageHeader {
                id,
                sender: self.sender,
                seq: self.seq,
                payload_len: self.payload.len() as u32,
                flags: self.flags,
                vclock_size: self.vclock.as_ref().map(|vc| vc.serialized_size() as u8).unwrap_or(0),
                _reserved: 0,
            },
            vclock: self.vclock,
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sender() -> SenderKey {
        let mut key = [0u8; 32];
        key[0] = 1;
        key[31] = 42;
        key
    }

    #[test]
    fn test_message_basic() {
        let sender = test_sender();
        let msg = Message::new(sender, 1, b"hello world".to_vec());

        assert!(msg.verify_id());
        assert_eq!(msg.header.seq, 1);
        assert_eq!(msg.payload, b"hello world");
    }

    #[test]
    fn test_message_deterministic_id() {
        let sender = test_sender();

        // Same inputs should produce same ID
        let msg1 = Message::new(sender, 1, b"test".to_vec());
        let msg2 = Message::new(sender, 1, b"test".to_vec());
        assert_eq!(msg1.header.id, msg2.header.id);

        // Different payload should produce different ID
        let msg3 = Message::new(sender, 1, b"different".to_vec());
        assert_ne!(msg1.header.id, msg3.header.id);

        // Different seq should produce different ID
        let msg4 = Message::new(sender, 2, b"test".to_vec());
        assert_ne!(msg1.header.id, msg4.header.id);
    }

    #[test]
    fn test_message_serialization() {
        let sender = test_sender();
        let msg = Message::new(sender, 42, b"payload data".to_vec());

        let bytes = msg.to_bytes();
        let msg2 = Message::from_bytes(&bytes).unwrap();

        assert_eq!(msg.header.id, msg2.header.id);
        assert_eq!(msg.header.seq, msg2.header.seq);
        assert_eq!(msg.payload, msg2.payload);
        assert!(msg2.verify_id());
    }

    #[test]
    fn test_message_with_vclock() {
        let sender = test_sender();
        let vclock = VectorClock::new(0, 3);

        let msg = Message::with_vclock(sender, 1, b"data".to_vec(), vclock);
        assert!(msg.vclock.is_some());
        assert!(msg.header.flags & flags::HAS_VCLOCK != 0);

        let bytes = msg.to_bytes();
        let msg2 = Message::from_bytes(&bytes).unwrap();
        assert!(msg2.vclock.is_some());
    }

    #[test]
    fn test_message_builder() {
        let sender = test_sender();
        let vclock = VectorClock::new(1, 4);

        let msg = MessageBuilder::new(sender, 5)
            .payload(b"built message".to_vec())
            .vclock(vclock)
            .requires_ack()
            .build();

        assert_eq!(msg.header.seq, 5);
        assert!(msg.header.flags & flags::HAS_VCLOCK != 0);
        assert!(msg.header.flags & flags::REQUIRES_ACK != 0);
        assert!(msg.vclock.is_some());
    }

    #[test]
    fn test_header_size() {
        // Ensure header is packed correctly
        assert_eq!(MessageHeader::SIZE, 80);
    }
}
