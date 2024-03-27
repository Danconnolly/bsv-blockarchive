use std::pin::Pin;
use std::task::{Context, Poll};
use async_trait::async_trait;
use bitcoinsv::bitcoin::{BlockHash, BlockHeader};
use tokio::io::AsyncRead;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use crate::Result;


/// The BlockArchive stores blocks, where a block is a BlockHeader and the transactions
/// that are required to validate the block.
///
/// The BlockArchive has very little knowledge of the structure of block, it only knows how to
/// store and retrieve blocks.
#[async_trait]
pub trait BlockArchive {
    /// Get a block from the archive.
    async fn get_block<R>(&self, block_hash: BlockHash) -> Result<R>
        where R: AsyncRead + Unpin + Send + 'async_trait;

    /// Check if a block exists in the archive.
    async fn block_exists(&self, block_hash: BlockHash) -> Result<bool>;

    /// Store a block in the archive.
    async fn store_block<S>(&self, block: S) -> Result<()>
        where S: AsyncRead + Unpin + Send + 'async_trait;

    /// Get the size of a block in the archive.
    async fn block_size(&self, block_hash: BlockHash) -> Result<usize>;

    /// Get the header of a block in the archive.
    async fn block_header(&self, block_hash: BlockHash) -> Result<BlockHeader>;

    /// Get a list of all the blocks in the archive.
    async fn block_list(&mut self) -> Result<Pin<Box<dyn BlockHashListStream<Item=BlockHash>>>>;
}

pub trait BlockHashListStream: Stream<Item = BlockHash> {}

pub struct BlockHashListStreamFromChannel {
    // The receiver to which the background task sends block hashes.
    receiver: Receiver<BlockHash>,
    // Handle to the background task that reads the block hashes.
    handle: JoinHandle<()>,
}

impl BlockHashListStreamFromChannel {
    pub fn new(receiver: Receiver<BlockHash>, handle: JoinHandle<()>) -> BlockHashListStreamFromChannel {
        BlockHashListStreamFromChannel { receiver, handle }
    }
}

impl Stream for BlockHashListStreamFromChannel {
    type Item = BlockHash;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl BlockHashListStream for BlockHashListStreamFromChannel {}

impl Drop for BlockHashListStreamFromChannel {
    // close the handle to the background task when the stream is dropped
    fn drop(&mut self) {
        if self.handle.is_finished() {
            return;
        }
        self.handle.abort();
    }
}
