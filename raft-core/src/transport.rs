//! Transport implement the message passing system.

use std::io::{self, BufWriter, Write};

/// HEADER_SIZE is the size of the message.
const HEADER_SIZE: usize = 10000;

/// The `Transporter` trait allows for reading and writing header-prefixed bytes.
pub trait Transporter {
    /// Send a header-prefixed message over the network.
    fn send_message(&mut self, message: &[u8]) -> io::Result<usize>;

    /// Receive a header-prefixed message.
    fn recv_message(&mut self) -> io::Result<String>;
}

/// This trait groups the basic Read and Write traits.
pub trait ReaderWriter: io::Read + io::Write {}

/// The `Transport` owns the data for sending/receiving bytes.
pub struct Transport<T> {
    sock: T,
}

impl<T> Transport<T>
where
    T: io::Read + io::Write,
{
    pub fn new(sock: T) -> Self {
        Transport { sock }
    }
}

impl<T> Transporter for Transport<T>
where
    T: io::Read + io::Write,
{
    /// Send a size-prefixed message over the network.
    fn send_message(&mut self, message: &[u8]) -> io::Result<usize> {
        let header = format!("{:>size$}", message.len(), size = HEADER_SIZE);
        let header = header.as_bytes();
        let mut writer = BufWriter::with_capacity(HEADER_SIZE + message.len(), &mut self.sock);
        writer.write(header)?;
        let size = writer.write(message)?;
        if let Err(error) = writer.flush() {
            return Err(error);
        }

        Ok(size)
    }

    /// Receive a size-prefixed message.
    fn recv_message(&mut self) -> io::Result<String> {
        let mut size = [0; HEADER_SIZE];
        if let Err(error) = self.sock.read_exact(&mut size) {
            return Err(error);
        }

        let size = String::from_utf8(size.to_vec()).unwrap();
        let size: usize = size.trim().parse().unwrap();
        let mut buf = vec![0; size];
        match self.sock.read_exact(&mut buf) {
            Ok(_) => {
                return Ok(String::from_utf8(buf).unwrap());
            }
            Err(err) => Err(err),
        }
    }
}
