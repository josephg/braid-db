use async_std::io::{Read as AsyncRead, BufRead as AsyncBufRead};
use async_std::task::ready;
use async_std::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::io;
// use futures_lite::future::ready;
use async_std::channel;
// use futures_lite::Stream;

pin_project_lite::pin_project! {
    /// The read half of an IO channel pair
    #[derive(Debug)]
    pub(crate) struct IOReadChannel {
        buf: Box<[u8]>,
        cursor: usize,
        #[pin]
        receiver: channel::Receiver<Vec<u8>>,
    }
}

pub(crate) fn channel() -> (channel::Sender<Vec<u8>>, IOReadChannel) {
    let (sender, receiver) = channel::bounded(1);
    let reader = IOReadChannel {
        receiver,
        buf: Box::default(),
        cursor: 0,
    };

    (sender, reader)
}

impl AsyncRead for IOReadChannel {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        // Request a new buffer if current one is exhausted.
        if this.buf.len() <= *this.cursor {
            match ready!(this.receiver.poll_next(cx)) {
                Some(buf) => {
                    // log::trace!("> Received a new buffer with len {}", buf.len());
                    *this.buf = buf.into_boxed_slice();
                    *this.cursor = 0;
                }
                None => {
                    // log::trace!("> Encoder done reading");
                    return Poll::Ready(Ok(0));
                }
            };
        }

        // Write the current buffer to completion.
        let local_buf = &this.buf[*this.cursor..];
        let max = buf.len().min(local_buf.len());
        buf[..max].clone_from_slice(&local_buf[..max]);
        *this.cursor += max;

        // Return bytes read.
        Poll::Ready(Ok(max))
    }
}

impl AsyncBufRead for IOReadChannel {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.project();
        // Request a new buffer if current one is exhausted.
        if this.buf.len() <= *this.cursor {
            match ready!(this.receiver.poll_next(cx)) {
                Some(buf) => {
                    // log::trace!("> Received a new buffer with len {}", buf.len());
                    *this.buf = buf.into_boxed_slice();
                    *this.cursor = 0;
                }
                None => {
                    // log::trace!("> Encoder done reading");
                    return Poll::Ready(Ok(&[]));
                }
            };
        }
        Poll::Ready(Ok(&this.buf[*this.cursor..]))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        *this.cursor += amt;
    }
}
