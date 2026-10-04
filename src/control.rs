//! Control messages on the Unix socket. Encode and decode only; each side
//! does its own I/O and passes descriptors with `SCM_RIGHTS`.
//!
//! ```text
//! frame:   len u32 (whole frame) | kind u16 | version u16 | id u32 | body
//! ```
//!
//! Every body has an exact size, and reserved bits must be zero.

use crate::error::ControlError;

/// Bytes before the body.
pub const FRAME_HEADER: usize = 12;

/// Largest frame.
pub const MAX_FRAME: usize = 4096;

/// Control protocol version this build speaks.
pub const PROTOCOL_VERSION: u16 = 1;

/// Longest text in an Error message.
pub const MAX_ERROR_TEXT: usize = 256;

/// The one AttachOk flag in version 1: every write is durable when it
/// completes.
pub const ATTACH_DURABLE: u16 = 1;

/// An Attach request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attach {
    /// Volume UUID bytes.
    pub volume: [u8; 16],
    /// 0 to ask for a new generation; otherwise the one this process has.
    pub generation: u64,
    /// Queues asked for.
    pub queues: u16,
    /// Ring depth asked for.
    pub depth: u32,
    /// Buffer pages per queue asked for.
    pub buf_pages: u32,
    /// Largest request in 4 KiB blocks.
    pub max_blocks: u32,
}

/// The engine's answer to Attach. The descriptors travel with its first
/// byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachOk {
    /// The attachment's generation.
    pub generation: u64,
    /// Queues granted.
    pub queues: u16,
    /// Ring depth granted.
    pub depth: u32,
    /// Buffer pages per queue granted.
    pub buf_pages: u32,
    /// Largest request in 4 KiB blocks.
    pub max_blocks: u32,
    /// Volume size in 4 KiB blocks.
    pub volume_blocks: u64,
    /// Bytes of the region.
    pub region_len: u64,
    /// Region layout version.
    pub layout_version: u16,
    /// Flags; only [`ATTACH_DURABLE`] in version 1.
    pub flags: u16,
}

/// One control message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// rust-bhyve opens the conversation.
    Hello {
        /// Lowest protocol version it speaks.
        min_version: u16,
        /// Highest protocol version it speaks.
        max_version: u16,
        /// Feature bits.
        features: u64,
    },
    /// The engine's choice.
    HelloAck {
        /// The chosen version.
        version: u16,
        /// Feature bits both sides use.
        features: u64,
    },
    /// Attach a volume.
    Attach(Attach),
    /// Attach succeeded.
    AttachOk(AttachOk),
    /// Stop taking new requests.
    Pause,
    /// Every request taken before Pause has its completion in the ring.
    Paused,
    /// Take requests again.
    Resume,
    /// Resume done.
    Resumed,
    /// Detach the volume.
    Detach,
    /// No request is in the engine; the engine unmaps.
    Detached,
    /// Heartbeat.
    Ping,
    /// Heartbeat answer.
    Pong,
    /// The sender found a protocol error and will close.
    Error {
        /// Error code.
        code: u16,
        /// Short text, UTF-8, at most [`MAX_ERROR_TEXT`] bytes.
        text: String,
    },
}

impl Message {
    fn kind(&self) -> u16 {
        match self {
            Self::Hello { .. } => 1,
            Self::HelloAck { .. } => 2,
            Self::Attach(_) => 3,
            Self::AttachOk(_) => 4,
            Self::Pause => 5,
            Self::Paused => 6,
            Self::Resume => 7,
            Self::Resumed => 8,
            Self::Detach => 9,
            Self::Detached => 10,
            Self::Ping => 11,
            Self::Pong => 12,
            Self::Error { .. } => 13,
        }
    }

    /// The frame for this message, with request id `id`. An Error text
    /// longer than [`MAX_ERROR_TEXT`] is cut at a character boundary.
    pub fn encode(&self, id: u32) -> Vec<u8> {
        let mut body = Vec::new();
        match self {
            Self::Hello {
                min_version,
                max_version,
                features,
            } => {
                body.extend_from_slice(&min_version.to_le_bytes());
                body.extend_from_slice(&max_version.to_le_bytes());
                body.extend_from_slice(&features.to_le_bytes());
            }
            Self::HelloAck { version, features } => {
                body.extend_from_slice(&version.to_le_bytes());
                body.extend_from_slice(&features.to_le_bytes());
            }
            Self::Attach(a) => {
                body.extend_from_slice(&a.volume);
                body.extend_from_slice(&a.generation.to_le_bytes());
                body.extend_from_slice(&a.queues.to_le_bytes());
                body.extend_from_slice(&a.depth.to_le_bytes());
                body.extend_from_slice(&a.buf_pages.to_le_bytes());
                body.extend_from_slice(&a.max_blocks.to_le_bytes());
            }
            Self::AttachOk(a) => {
                body.extend_from_slice(&a.generation.to_le_bytes());
                body.extend_from_slice(&a.queues.to_le_bytes());
                body.extend_from_slice(&a.depth.to_le_bytes());
                body.extend_from_slice(&a.buf_pages.to_le_bytes());
                body.extend_from_slice(&a.max_blocks.to_le_bytes());
                body.extend_from_slice(&a.volume_blocks.to_le_bytes());
                body.extend_from_slice(&a.region_len.to_le_bytes());
                body.extend_from_slice(&a.layout_version.to_le_bytes());
                body.extend_from_slice(&a.flags.to_le_bytes());
            }
            Self::Error { code, text } => {
                let mut end = text.len().min(MAX_ERROR_TEXT);
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                body.extend_from_slice(&code.to_le_bytes());
                body.extend_from_slice(&(end as u16).to_le_bytes());
                body.extend_from_slice(&text.as_bytes()[..end]);
            }
            Self::Pause
            | Self::Paused
            | Self::Resume
            | Self::Resumed
            | Self::Detach
            | Self::Detached
            | Self::Ping
            | Self::Pong => {}
        }
        let len = (FRAME_HEADER + body.len()) as u32;
        let mut out = Vec::with_capacity(len as usize);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&self.kind().to_le_bytes());
        out.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// Decodes the frame at the start of `buf`. Returns `Ok(None)` if the
    /// frame is not complete yet, else the message, its id and the bytes it
    /// used.
    pub fn decode(buf: &[u8]) -> Result<Option<(Message, u32, usize)>, ControlError> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if (len as usize) < FRAME_HEADER || len as usize > MAX_FRAME {
            return Err(ControlError::Length(len));
        }
        let len = len as usize;
        if buf.len() < len {
            return Ok(None);
        }
        let kind = u16::from_le_bytes([buf[4], buf[5]]);
        let version = u16::from_le_bytes([buf[6], buf[7]]);
        if version != PROTOCOL_VERSION {
            return Err(ControlError::Version(version));
        }
        let id = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let mut r = Reader(&buf[FRAME_HEADER..len]);
        let msg = match kind {
            1 => Self::Hello {
                min_version: r.u16()?,
                max_version: r.u16()?,
                features: r.u64()?,
            },
            2 => Self::HelloAck {
                version: r.u16()?,
                features: r.u64()?,
            },
            3 => Self::Attach(Attach {
                volume: r.array()?,
                generation: r.u64()?,
                queues: r.u16()?,
                depth: r.u32()?,
                buf_pages: r.u32()?,
                max_blocks: r.u32()?,
            }),
            4 => {
                let a = AttachOk {
                    generation: r.u64()?,
                    queues: r.u16()?,
                    depth: r.u32()?,
                    buf_pages: r.u32()?,
                    max_blocks: r.u32()?,
                    volume_blocks: r.u64()?,
                    region_len: r.u64()?,
                    layout_version: r.u16()?,
                    flags: r.u16()?,
                };
                if a.flags & !ATTACH_DURABLE != 0 {
                    return Err(ControlError::Body);
                }
                Self::AttachOk(a)
            }
            5 => Self::Pause,
            6 => Self::Paused,
            7 => Self::Resume,
            8 => Self::Resumed,
            9 => Self::Detach,
            10 => Self::Detached,
            11 => Self::Ping,
            12 => Self::Pong,
            13 => {
                let code = r.u16()?;
                let n = usize::from(r.u16()?);
                if n > MAX_ERROR_TEXT {
                    return Err(ControlError::Body);
                }
                let text = std::str::from_utf8(r.bytes(n)?)
                    .map_err(|_| ControlError::Body)?
                    .to_string();
                Self::Error { code, text }
            }
            k => return Err(ControlError::Kind(k)),
        };
        if !r.0.is_empty() {
            return Err(ControlError::Body);
        }
        Ok(Some((msg, id, len)))
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn bytes(&mut self, n: usize) -> Result<&[u8], ControlError> {
        if self.0.len() < n {
            return Err(ControlError::Body);
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ControlError> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.bytes(N)?);
        Ok(a)
    }

    fn u16(&mut self) -> Result<u16, ControlError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, ControlError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ControlError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(m: Message) {
        let b = m.encode(42);
        assert_eq!(Message::decode(&b), Ok(Some((m, 42, b.len()))));
        assert_eq!(Message::decode(&b[..b.len() - 1]), Ok(None));
    }

    #[test]
    fn every_message_round_trips() {
        round_trip(Message::Hello {
            min_version: 1,
            max_version: 1,
            features: 3,
        });
        round_trip(Message::HelloAck {
            version: 1,
            features: 1,
        });
        round_trip(Message::Attach(Attach {
            volume: [7; 16],
            generation: 0,
            queues: 4,
            depth: 256,
            buf_pages: 1024,
            max_blocks: 512,
        }));
        round_trip(Message::AttachOk(AttachOk {
            generation: 9,
            queues: 4,
            depth: 256,
            buf_pages: 512,
            max_blocks: 512,
            volume_blocks: 1 << 30,
            region_len: 1 << 26,
            layout_version: 1,
            flags: ATTACH_DURABLE,
        }));
        for m in [
            Message::Pause,
            Message::Paused,
            Message::Resume,
            Message::Resumed,
            Message::Detach,
            Message::Detached,
            Message::Ping,
            Message::Pong,
        ] {
            round_trip(m);
        }
        round_trip(Message::Error {
            code: 4,
            text: "bad request".into(),
        });
    }

    #[test]
    fn rejects_bad_frames() {
        let ping = Message::Ping.encode(1);
        let mut b = ping.clone();
        b[0] = 4;
        assert_eq!(Message::decode(&b), Err(ControlError::Length(4)));
        let mut b = ping.clone();
        b[4] = 99;
        assert_eq!(Message::decode(&b), Err(ControlError::Kind(99)));
        let mut b = ping.clone();
        b[6] = 2;
        assert_eq!(Message::decode(&b), Err(ControlError::Version(2)));
        let mut b = ping;
        b.push(0);
        b[0] += 1;
        assert_eq!(Message::decode(&b), Err(ControlError::Body));
        let big = 5000u32.to_le_bytes();
        assert_eq!(Message::decode(&big), Err(ControlError::Length(5000)));
    }

    #[test]
    fn attach_ok_reserved_flags_are_refused() {
        let mut b = Message::AttachOk(AttachOk {
            generation: 1,
            queues: 1,
            depth: 2,
            buf_pages: 1,
            max_blocks: 1,
            volume_blocks: 1,
            region_len: 1,
            layout_version: 1,
            flags: ATTACH_DURABLE,
        })
        .encode(0);
        let n = b.len();
        b[n - 2] |= 2;
        assert_eq!(Message::decode(&b), Err(ControlError::Body));
    }

    #[test]
    fn error_text_is_cut_on_a_char_boundary() {
        let text = "é".repeat(200);
        let b = Message::Error { code: 1, text }.encode(0);
        let Ok(Some((Message::Error { text, .. }, _, _))) = Message::decode(&b) else {
            panic!("decode");
        };
        assert!(text.len() <= MAX_ERROR_TEXT);
    }
}
