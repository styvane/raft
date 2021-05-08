//! Transport implement the message passing system.

use anyhow::{self, Context as _};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

/// HEADER_SIZE is the size of the message.
const HEADER_SIZE: usize = 10000;

/// The `Transport` owns the data for sending/receiving bytes.
#[derive(Debug, Clone)]
pub struct Transport<T> {
    messenger: T,
}

impl<T> Transport<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(messenger: T) -> Self {
        Transport { messenger }
    }

    /// Send a header-prefixed frame over the network.
    pub async fn send_frame(&mut self, buf: &[u8]) -> anyhow::Result<usize> {
        let header = format!("{:>size$}", buf.len(), size = HEADER_SIZE);
        let header = header.as_bytes();
        let mut writer = BufWriter::with_capacity(HEADER_SIZE + buf.len(), &mut self.messenger);
        writer.write(header).await?;
        let size = writer.write(buf).await?;
        writer.flush().await.context("Failed to flush buffer")?;

        Ok(size)
    }

    /// Receive a header-prefixed message.
    pub async fn recv_frame(&mut self) -> anyhow::Result<String> {
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
    use std::cell::RefCell;
    use std::cmp;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{self, ReadBuf};

    struct MockMessenger {
        buf: RefCell<Option<String>>,
    }

    impl AsyncRead for MockMessenger {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf,
        ) -> Poll<io::Result<()>> {
            match &mut *self.as_ref().buf.borrow_mut() {
                Some(data) => {
                    let bytes = data.as_bytes();
                    let amt = cmp::min(bytes.len(), buf.remaining());
                    buf.put_slice(&bytes[..amt]);
                    data.drain(..amt);

                    Poll::Ready(Ok(()))
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

    impl AsyncWrite for MockMessenger {
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

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_send_message() {
        let m = MockMessenger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_frame(b"hello").await.is_ok());
    }

    #[tokio::test]
    async fn test_recv_one_message() {
        let m = MockMessenger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_frame(b"hello").await.is_ok());
        assert_eq!(trp.recv_frame().await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_recv_multi_messages() {
        let m = MockMessenger::new();
        let mut trp = Transport::new(m);
        assert!(trp.send_frame(b"hello").await.is_ok());
        assert!(trp.send_frame(b"world").await.is_ok());

        let mut data = trp.recv_frame().await.unwrap();
        assert_eq!(data, "hello");

        data = trp.recv_frame().await.unwrap();
        assert_eq!(data, "world");
    }
}
