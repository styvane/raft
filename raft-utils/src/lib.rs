//! Header prefix frame processing.

use anyhow::{self, Context as _};

use async_std::io::{self, BufWriter};
use async_std::prelude::*;

/// HEADER_SIZE is the size of the message.
const HEADER_SIZE: usize = 10000;

/// Send a header-prefixed frame over the network.
pub async fn send_frame<T>(writer: &mut T, buf: &[u8]) -> anyhow::Result<usize>
where
    T: io::Write + Unpin,
{
    let header = format!("{:>size$}", buf.len(), size = HEADER_SIZE);
    let header = header.as_bytes();
    let mut buf_writer = BufWriter::with_capacity(HEADER_SIZE + buf.len(), writer);
    buf_writer.write(header).await?;
    let size = buf_writer.write(buf).await?;
    buf_writer.flush().await.context("Failed to flush buffer")?;

    Ok(size)
}

/// Receive a header-prefixed message.
pub async fn recv_frame<T>(reader: &mut T) -> anyhow::Result<String>
where
    T: io::Read + Unpin,
{
    let mut size = [0; HEADER_SIZE];
    reader
        .read_exact(&mut size)
        .await
        .context("Unable to read message header")?;

    let size = String::from_utf8(size.to_vec()).unwrap();
    let size: usize = size.trim().parse().unwrap();
    let mut buf = vec![0; size];

    reader
        .read_exact(&mut buf)
        .await
        .context("Reading received message failed")?;
    String::from_utf8(buf).context("Unable to convert message to string")
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

                    Poll::Ready(Ok(len))
                }
                Err(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "invalid utf-8 data",
                ))),
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
        let mut m = MockMessenger::new();
        assert!(send_frame(&mut m, b"hello").await.is_ok());
    }

    #[async_std::test]
    async fn test_recv_one_message() {
        let mut m = MockMessenger::new();
        assert!(send_frame(&mut m, b"hello").await.is_ok());
        assert_eq!(recv_frame(&mut m).await.unwrap(), "hello");
    }

    #[async_std::test]
    async fn test_recv_multi_messages() {
        let mut m = MockMessenger::new();
        assert!(send_frame(&mut m, b"hello").await.is_ok());
        assert!(send_frame(&mut m, b"world").await.is_ok());

        let mut data = recv_frame(&mut m).await.unwrap();
        assert_eq!(data, "hello");

        data = recv_frame(&mut m).await.unwrap();
        assert_eq!(data, "world");
    }
}
