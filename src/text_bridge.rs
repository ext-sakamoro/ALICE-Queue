//! ALICE-Queue × ALICE-Text bridge
//!
//! Compressed text payloads for log ingestion pipeline.
//!
//! Author: Moroya Sakamoto

use alice_text::ALICEText;

/// Encoding type marker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TextEncoding {
    AliceText = 0,
    Raw = 1,
}

/// Compressed text payload for queue transmission
#[derive(Debug, Clone)]
pub struct TextPayload {
    pub compressed: Vec<u8>,
    pub original_len: u32,
    pub encoding: TextEncoding,
}

/// Encode text into a compact queue payload
pub fn encode_text_payload(text: &str) -> TextPayload {
    let compressor = ALICEText::new();
    match compressor.compress(text) {
        Ok(compressed) => TextPayload {
            original_len: text.len() as u32,
            compressed,
            encoding: TextEncoding::AliceText,
        },
        Err(_) => TextPayload {
            original_len: text.len() as u32,
            compressed: text.as_bytes().to_vec(),
            encoding: TextEncoding::Raw,
        },
    }
}

/// Decode a queue payload back to text
pub fn decode_text_payload(payload: &TextPayload) -> Result<String, String> {
    match payload.encoding {
        TextEncoding::AliceText => {
            let decompressor = ALICEText::new();
            decompressor
                .decompress(&payload.compressed)
                .map_err(|e| format!("Decompress error: {}", e))
        }
        TextEncoding::Raw => {
            String::from_utf8(payload.compressed.clone())
                .map_err(|e| format!("UTF-8 error: {}", e))
        }
    }
}

/// Serialize TextPayload to bytes for queue insertion
pub fn serialize_payload(payload: &TextPayload) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.compressed.len());
    out.push(payload.encoding as u8);
    out.extend_from_slice(&payload.original_len.to_le_bytes());
    out.extend_from_slice(&payload.compressed);
    out
}

/// Deserialize bytes from queue into TextPayload
pub fn deserialize_payload(data: &[u8]) -> Result<TextPayload, String> {
    if data.len() < 5 {
        return Err("Payload too short".into());
    }
    let encoding = match data[0] {
        0 => TextEncoding::AliceText,
        1 => TextEncoding::Raw,
        v => return Err(format!("Unknown encoding: {}", v)),
    };
    let original_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
    let compressed = data[5..].to_vec();
    Ok(TextPayload { compressed, original_len, encoding })
}

/// Batched text log pipeline
pub struct TextLogPipeline {
    pub buffer: Vec<TextPayload>,
    pub total_bytes_saved: u64,
}

impl TextLogPipeline {
    pub fn new() -> Self {
        Self { buffer: Vec::new(), total_bytes_saved: 0 }
    }

    /// Add a log line to the pipeline
    pub fn push(&mut self, text: &str) {
        let payload = encode_text_payload(text);
        let saved = text.len().saturating_sub(payload.compressed.len());
        self.total_bytes_saved += saved as u64;
        self.buffer.push(payload);
    }

    /// Drain all buffered payloads
    pub fn drain(&mut self) -> Vec<TextPayload> {
        std::mem::take(&mut self.buffer)
    }

    pub fn pending_count(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let text = "2026-02-11 ERROR: Connection refused to 10.0.0.1:8080";
        let payload = encode_text_payload(text);
        assert!(payload.original_len > 0);
        let decoded = decode_text_payload(&payload).unwrap();
        assert_eq!(decoded, text);
    }

    #[test]
    fn test_serialize_deserialize() {
        let payload = encode_text_payload("test log entry");
        let bytes = serialize_payload(&payload);
        let restored = deserialize_payload(&bytes).unwrap();
        assert_eq!(restored.original_len, payload.original_len);
        assert_eq!(restored.encoding, payload.encoding);
    }

    #[test]
    fn test_pipeline() {
        let mut pipeline = TextLogPipeline::new();
        pipeline.push("log line 1");
        pipeline.push("log line 2");
        assert_eq!(pipeline.pending_count(), 2);
        let batch = pipeline.drain();
        assert_eq!(batch.len(), 2);
        assert_eq!(pipeline.pending_count(), 0);
    }
}
