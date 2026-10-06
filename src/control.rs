//! Control messages on the Unix socket. Encode and decode only; each side
//! does its own I/O and passes descriptors with `SCM_RIGHTS`.
//!
//! ```text
//! frame:   len u32 (whole frame) | kind u16 | version u16 | id u32 | body
//! ```
//!
//! Every body has an exact size, and reserved bits must be zero.
//!
//! Hello and HelloAck always go in frame version 1, so a peer of any
//! version reads the offer and can answer it. After HelloAck both sides
//! use the agreed version for every frame; an Error frame of any known
//! version is still read, so a refusal always arrives.

use crate::error::ControlError;

/// Bytes before the body.
pub const FRAME_HEADER: usize = 12;

/// Largest frame.
pub const MAX_FRAME: usize = 4096;

/// Newest control protocol version this build speaks.
pub const PROTOCOL_VERSION: u16 = 2;

/// Oldest control protocol version this build speaks.
pub const MIN_PROTOCOL_VERSION: u16 = 1;

/// The frame version of Hello and HelloAck.
pub const HELLO_VERSION: u16 = 1;

/// Hello feature bits. Each needs control protocol version 2 or later.
pub mod feature {
    /// 512-byte logical sectors: a write, deallocate or write zeroes may
    /// name a byte range inside its 4 KiB blocks.
    pub const SECTOR_512E: u64 = 1 << 0;
    /// Reserved: the region as System V shared memory (spec `37` §11).
    pub const SYSV_REGION: u64 = 1 << 1;
    /// Ops 4 (deallocate) and 5 (write zeroes).
    pub const DEALLOCATE: u64 = 1 << 2;
    /// The bits this build uses.
    pub const KNOWN: u64 = SECTOR_512E | DEALLOCATE;
}

/// The version and feature bits for a Hello offer: the highest version
/// both sides speak, and the offered bits this build knows. Feature bits
/// need version 2. `None` when no version is common.
pub fn choose(min_version: u16, max_version: u16, features: u64) -> Option<(u16, u64)> {
    let v = max_version.min(PROTOCOL_VERSION);
    if v < min_version.max(MIN_PROTOCOL_VERSION) {
        return None;
    }
    let f = if v >= 2 { features & feature::KNOWN } else { 0 };
    Some((v, f))
}

/// Longest text in an Error message.
pub const MAX_ERROR_TEXT: usize = 256;

/// AttachOk flag: every write is durable when it completes.
pub const ATTACH_DURABLE: u16 = 1;

/// AttachOk flag, version 2: the volume takes ops 4 and 5.
pub const ATTACH_DEALLOCATE: u16 = 2;

const ERROR_KIND: u16 = 13;

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
    /// [`ATTACH_DURABLE`]; from version 2 also [`ATTACH_DEALLOCATE`].
    pub flags: u16,
    /// The volume's logical sector, 512 or 4096, fixed at create. Version
    /// 1 carries no field and means 4096; the engine refuses a 512-byte
    /// volume to a version 1 peer.
    pub sector_bytes: u16,
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
            Self::Error { .. } => ERROR_KIND,
        }
    }

    /// The frame for this message in version 1, with request id `id`.
    pub fn encode(&self, id: u32) -> Vec<u8> {
        self.encode_as(id, HELLO_VERSION)
    }

    /// The frame for this message in `version`, with request id `id`. An
    /// Error text longer than [`MAX_ERROR_TEXT`] is cut at a character
    /// boundary. Version 1 has no field for AttachOk's sector size or its
    /// version 2 flags.
    pub fn encode_as(&self, id: u32, version: u16) -> Vec<u8> {
        let version = match self {
            Self::Hello { .. } | Self::HelloAck { .. } => HELLO_VERSION,
            _ => version,
        };
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
                if version >= 2 {
                    body.extend_from_slice(&a.flags.to_le_bytes());
                    body.extend_from_slice(&a.sector_bytes.to_le_bytes());
                    body.extend_from_slice(&0u16.to_le_bytes());
                } else {
                    body.extend_from_slice(&(a.flags & ATTACH_DURABLE).to_le_bytes());
                }
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
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// Decodes the version 1 frame at the start of `buf`: Hello, HelloAck,
    /// and every frame of a version 1 session.
    pub fn decode(buf: &[u8]) -> Result<Option<(Message, u32, usize)>, ControlError> {
        Self::decode_as(buf, HELLO_VERSION)
    }

    /// Decodes the frame at the start of `buf` in the agreed `version`.
    /// Returns `Ok(None)` if the frame is not complete yet, else the
    /// message, its id and the bytes it used.
    pub fn decode_as(
        buf: &[u8],
        version: u16,
    ) -> Result<Option<(Message, u32, usize)>, ControlError> {
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
        let found = u16::from_le_bytes([buf[6], buf[7]]);
        let known = (MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION).contains(&found);
        let hello = matches!(kind, 1 | 2);
        let agreed = if hello { HELLO_VERSION } else { version };
        if !known || (found != agreed && kind != ERROR_KIND) {
            return Err(ControlError::Version(found));
        }
        let version = found;
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
                let mut a = AttachOk {
                    generation: r.u64()?,
                    queues: r.u16()?,
                    depth: r.u32()?,
                    buf_pages: r.u32()?,
                    max_blocks: r.u32()?,
                    volume_blocks: r.u64()?,
                    region_len: r.u64()?,
                    layout_version: r.u16()?,
                    flags: r.u16()?,
                    sector_bytes: 4096,
                };
                let flags = if version >= 2 {
                    a.sector_bytes = r.u16()?;
                    if r.u16()? != 0 || !matches!(a.sector_bytes, 512 | 4096) {
                        return Err(ControlError::Body);
                    }
                    ATTACH_DURABLE | ATTACH_DEALLOCATE
                } else {
                    ATTACH_DURABLE
                };
                if a.flags & !flags != 0 {
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
        for v in [1, 2] {
            let b = m.encode_as(42, v);
            assert_eq!(
                Message::decode_as(&b, v),
                Ok(Some((m.clone(), 42, b.len())))
            );
            assert_eq!(Message::decode_as(&b[..b.len() - 1], v), Ok(None));
        }
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
            sector_bytes: 4096,
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
            sector_bytes: 4096,
        })
        .encode(0);
        let n = b.len();
        b[n - 2] |= ATTACH_DEALLOCATE as u8;
        assert_eq!(Message::decode(&b), Err(ControlError::Body));
    }

    fn attach_ok(sector_bytes: u16, flags: u16) -> AttachOk {
        AttachOk {
            generation: 1,
            queues: 1,
            depth: 2,
            buf_pages: 1,
            max_blocks: 1,
            volume_blocks: 1,
            region_len: 1,
            layout_version: 1,
            flags,
            sector_bytes,
        }
    }

    #[test]
    fn version_2_attach_ok_carries_the_sector_and_checks_it() {
        let ok = attach_ok(512, ATTACH_DURABLE | ATTACH_DEALLOCATE);
        let b = Message::AttachOk(ok.clone()).encode_as(3, 2);
        let Ok(Some((Message::AttachOk(got), 3, _))) = Message::decode_as(&b, 2) else {
            panic!("decode");
        };
        assert_eq!(got, ok);
        let n = b.len();
        for (at, v) in [(n - 4, 0x01u8), (n - 1, 1), (n - 3, 0x01)] {
            let mut bad = b.clone();
            bad[at] = v;
            assert_eq!(
                Message::decode_as(&bad, 2),
                Err(ControlError::Body),
                "byte {at}"
            );
        }
        let mut bad = b.clone();
        bad[n - 6] |= 4;
        assert_eq!(Message::decode_as(&bad, 2), Err(ControlError::Body));
        // Version 1 has no sector field: a version 1 peer reads 4096.
        let v1 = Message::AttachOk(attach_ok(4096, ATTACH_DURABLE)).encode_as(3, 1);
        assert_eq!(v1.len() + 4, b.len());
    }

    #[test]
    fn frames_must_be_in_the_agreed_version_but_errors_always_arrive() {
        let ping = Message::Ping.encode_as(1, 2);
        assert_eq!(Message::decode(&ping), Err(ControlError::Version(2)));
        assert!(matches!(Message::decode_as(&ping, 2), Ok(Some(_))));
        let ping1 = Message::Ping.encode(1);
        assert_eq!(Message::decode_as(&ping1, 2), Err(ControlError::Version(1)));
        let err = Message::Error {
            code: 3,
            text: "x".into(),
        };
        assert!(matches!(Message::decode_as(&err.encode(0), 2), Ok(Some(_))));
        assert!(matches!(Message::decode(&err.encode_as(0, 2)), Ok(Some(_))));
        let mut far = err.encode(0);
        far[6] = 3;
        assert_eq!(Message::decode(&far), Err(ControlError::Version(3)));
        let mut far = Message::Ping.encode_as(1, 2);
        far[6] = 3;
        assert_eq!(Message::decode_as(&far, 3), Err(ControlError::Version(3)));
        // Hello goes in version 1 whatever the session agrees.
        let hello = Message::Hello {
            min_version: 1,
            max_version: 2,
            features: 5,
        };
        let b = hello.encode_as(0, 2);
        assert_eq!(u16::from_le_bytes([b[6], b[7]]), HELLO_VERSION);
        assert!(matches!(Message::decode_as(&b, 2), Ok(Some(_))));
    }

    #[test]
    fn choose_takes_the_highest_common_version_and_known_bits() {
        let all = feature::SECTOR_512E | feature::DEALLOCATE | 1 << 40;
        assert_eq!(choose(1, 1, all), Some((1, 0)));
        assert_eq!(choose(1, 2, all), Some((2, feature::KNOWN)));
        assert_eq!(
            choose(1, 9, feature::DEALLOCATE),
            Some((2, feature::DEALLOCATE))
        );
        assert_eq!(choose(3, 9, 0), None);
        assert_eq!(choose(2, 1, 0), None);
        assert_eq!(choose(0, 0, 0), None);
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
