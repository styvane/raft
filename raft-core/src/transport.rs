//! Transport implement the message passing system.

use anyhow::{self, Context};

use std::io::{self, BufWriter, ErrorKind, Read, Write};

/// HEADER_SIZE is the size of the message.
const HEADER_SIZE: usize = 10000;

/// This trait groups the basic Read and Write traits.
pub trait ReaderWriter: io::Read + io::Write {}

/// The `Transport` owns the data for sending/receiving bytes.
pub struct Transport<T> {
    messenger: T,
}

impl<T> Transport<T>
where
    T: io::Read + io::Write,
{
    pub fn new(messenger: T) -> Self {
        Transport { messenger }
    }

    /// Send a header-prefixed message over the network.
    pub fn send_message(&mut self, message: &[u8]) -> anyhow::Result<usize> {
        let header = format!("{:>size$}", message.len(), size = HEADER_SIZE);
        let header = header.as_bytes();
        let mut writer = BufWriter::with_capacity(HEADER_SIZE + message.len(), &mut self.messenger);
        writer.write(header)?;
        let size = writer.write(message)?;
        writer.flush().context("Failed to flush buffer")?;

        Ok(size)
    }

    /// Receive a header-prefixed message.
    pub fn recv_message(&mut self) -> anyhow::Result<String> {
        let mut size = [0; HEADER_SIZE];
        self.messenger
            .read_exact(&mut size)
            .context("Unable to read message header")?;

        let size = String::from_utf8(size.to_vec()).unwrap();
        let size: usize = size.trim().parse().unwrap();
        let mut buf = vec![0; size];

        self.messenger
            .read_exact(&mut buf)
            .context("Reading received message failed")?;
        String::from_utf8(buf).context("Unable to convert message to string")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockMesseger {
        buf: Option<String>,
    }

    impl io::Read for MockMesseger {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match self.buf.as_mut() {
                Some(data) => {
                    let bytes = data.as_bytes();
                    buf.copy_from_slice(&bytes[..buf.len()]);
                    data.drain(..buf.len());

                    Ok(buf.len())
                }
                None => Err(io::Error::new(ErrorKind::UnexpectedEof, "no data")),
            }
        }
    }

    impl MockMesseger {
        fn new() -> Self {
            MockMesseger { buf: None }
        }
    }

    impl io::Write for MockMesseger {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let len = buf.len();
            match String::from_utf8(buf.to_vec()) {
                Ok(s) => {
                    match self.buf.as_mut() {
                        Some(v) => {
                            v.push_str(&s);
                        }
                        None => {
                            self.buf.replace(s);
                        }
                    };
                    return Ok(len);
                }
                Err(_) => return Err(io::Error::new(ErrorKind::Other, "invalid utf-8 data")),
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_send_message() {
        let m = MockMesseger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_message(b"hello").is_ok());
    }

    #[test]
    fn test_recv_one_message() {
        let m = MockMesseger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_message(b"hello").is_ok());
        assert_eq!(trp.recv_message().unwrap(), "hello");
    }

    #[test]
    fn test_recv_multi_messages() {
        let m = MockMesseger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_message(b"hello").is_ok());
        assert!(trp.send_message(b"world").is_ok());

        let mut data = trp.recv_message().unwrap();
        assert_eq!(data, "hello");

        data = trp.recv_message().unwrap();
        assert_eq!(data, "world");
    }
}
