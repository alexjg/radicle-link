use nom::Parser;

use crate::protocol::packet_line::PacketLine;

enum StreamState {
    ReceivingReferences,
    ReceivingPack,
    Broken,
}

pub(super) struct IncomingState {
    buffer: Vec<u8>,
    state: StreamState,
}

impl IncomingState {
    pub(super) fn new() -> Self {
        Self {
            buffer: Vec::new(),
            state: StreamState::ReceivingReferences,
        }
    }

    pub(super) fn with_data(&mut self, incoming_data: &[u8]) -> Vec<u8> {
        self.buffer.extend_from_slice(incoming_data);
        let mut lines = Vec::new();
        let mut remaining = &self.buffer[..];
        loop {
            match self.state {
                StreamState::Broken | StreamState::ReceivingPack => { 
                    return incoming_data.to_vec() 
                },
                StreamState::ReceivingReferences => {
                    match Message::parse(&remaining) {
                        Ok((i, Message::Packet(p))) => {
                            tracing::info!(?p, "parse packet");
                            lines.extend(p.as_bytes());
                            remaining = i;
                        },
                        Ok((i, Message::PackStart)) => {
                            self.state = StreamState::ReceivingPack;
                            lines.extend(b"PACK");
                            lines.extend(i);
                            remaining = &[];
                            break;
                        },
                        Err(e) => {
                            if e.is_incomplete() {
                                break;
                            } else {
                                tracing::info!(err=?e, "parsing failed");
                                self.state = StreamState::Broken;
                                lines.extend(remaining);
                                remaining = &[];
                                break;
                            }
                        },
                    }
                },
            }
        }
        self.buffer = remaining.to_vec();
        lines
    }
}

enum Message<'a> {
    Packet(PacketLine<'a>),
    PackStart,
}

impl<'a> Message<'a> {
    fn parse(
        input: &'a [u8],
    ) -> nom::IResult<&'a [u8], Message<'a>, nom::error::VerboseError<&'a [u8]>> {
        let packstart = nom::bytes::streaming::tag(b"PACK").map(|_| Message::PackStart);
        let line =
            nom::combinator::map(PacketLine::parse::<nom::error::VerboseError<&[u8]>>, |l| {
                Message::Packet(l)
            });
        nom::branch::alt((packstart, line))(input)
    }
}
