//! Transport implement the message passing system.

use anyhow::{self, Context as _};

use async_std::io::{self, BufWriter};
use async_std::prelude::*;

/// HEADER_SIZE is the size of the message.
const HEADER_SIZE: usize = 10000;

/// The `Transport` owns the data for sending/receiving bytes.
#[derive(Debug, Clone)]
pub struct Transport<T> {
    messenger: T,
}

impl<T> Transport<T>
where
    T: io::Read + io::Write + Unpin,
{
    pub fn new(messenger: T) -> Self {
        Transport { messenger }
    }

    /// Send a header-prefixed message over the network.
    pub async fn send_message(&mut self, message: &[u8]) -> anyhow::Result<usize> {
        let header = format!("{:>size$}", message.len(), size = HEADER_SIZE);
        let header = header.as_bytes();
        let mut writer = BufWriter::with_capacity(HEADER_SIZE + message.len(), &mut self.messenger);
        writer.write(header).await?;
        let size = writer.write(message).await?;
        writer.flush().await.context("Failed to flush buffer")?;

        Ok(size)
    }

    /// Receive a header-prefixed message.
    pub async fn recv_message(&mut self) -> anyhow::Result<String> {
        let mut size = [0; HEADER_SIZE];
        self.messenger
            .read_exact(&mut size)
            .await
            .context("Unable to read message header")?;

        let size = String::from_utf8(size.to_vec()).unwrap();
        let size: usize = size.trim().parse().unwrap();
        let mut buf = vec![0; size];

        self.messenger
            .read_exact(&mut buf)
            .await
            .context("Reading received message failed")?;
        String::from_utf8(buf).context("Unable to convert message to string")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task::{Context, Poll};
    use std::cell::RefCell;
    use std::pin::Pin;

    struct MockMessenger {
        buf: RefCell<Option<String>>,
    }

    impl io::Read for MockMessenger {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            match &mut *self.as_ref().buf.borrow_mut() {
                Some(data) => {
                    let bytes = data.as_bytes();
                    buf.copy_from_slice(&bytes[..buf.len()]);
                    data.drain(..buf.len());

                    Poll::Ready(Ok(buf.len()))
                }
                None => Poll::Ready(Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no data"))),
            }
        }
    }

    impl MockMessenger {
        fn new() -> Self {
            MockMessenger {
                buf: RefCell::new(None),
            }
        }
    }

    impl io::Write for MockMessenger {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let len = buf.len();
            match String::from_utf8(buf.to_vec()) {
                Ok(s) => {
                    let mut new_data = String::new();
                    let buf = self.as_ref().buf.borrow().clone();
                    match buf {
                        Some(v) => {
                            new_data.push_str(&v);
                            new_data.push_str(&s);
                        }
                        None => {
                            new_data = s;
                        }
                    };

                    self.buf.borrow_mut().replace(new_data);

                    return Poll::Ready(Ok(len));
                }
                Err(_) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "invalid utf-8 data",
                    )))
                }
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[async_std::test]
    async fn test_send_message() {
        let m = MockMessenger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_message(b"hello").await.is_ok());
    }

    #[async_std::test]
    async fn test_recv_one_message() {
        let m = MockMessenger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_message(b"hello").await.is_ok());
        assert_eq!(trp.recv_message().await.unwrap(), "hello");
    }

    #[async_std::test]
    async fn test_recv_multi_messages() {
        let m = MockMessenger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_message(b"hello").await.is_ok());
        assert!(trp.send_message(b"world").await.is_ok());

        let mut data = trp.recv_message().await.unwrap();
        assert_eq!(data, "hello");

        data = trp.recv_message().await.unwrap();
        assert_eq!(data, "world");
    }
}
