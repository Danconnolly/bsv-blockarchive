use std::path::PathBuf;
use std::pin::Pin;
use async_trait::async_trait;
use bitcoinsv::bitcoin::{BlockHash, BlockHeader};
use tokio::io::AsyncRead;
use crate::{BlockArchive, Result};
use hex::{FromHex, ToHex};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReadDirStream;
use crate::block_archive::{BlockHashListStream, BlockHashListStreamFromChannel};

/// A simple file-based block archive.
///
/// Blocks are stored in a directory structure based on the block hash. The first level of directories
/// is based on the last two characters of the hash, the second level is based on the third and fourth
/// characters, and the block is stored in a file named after the hash.
///
/// Example: /31/c5/00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531.bin
///
/// This is simplistic to get started. It is not efficient for large numbers of small blocks.
///
/// Example code:
///     let root_dir = std::path::PathBuf::from("/mnt/blockstore/mainnet");
///     let mut archive= SimpleFileBasedBlockArchive::new(root_dir);
///     let mut results = archive.block_list().await.unwrap();
pub struct SimpleFileBasedBlockArchive
{
    /// The root of the file store
    pub root_path: PathBuf,
}

impl SimpleFileBasedBlockArchive
{
    /// Create a new block archive with the given root path.
    pub fn new(root_path: PathBuf) -> SimpleFileBasedBlockArchive {
        SimpleFileBasedBlockArchive {
            root_path,
        }
    }

    // Get the path for a block.
    fn get_path_from_hash(&self, hash: BlockHash) -> PathBuf {
        let mut path = self.root_path.clone();
        let s: String = hash.encode_hex();
        path.push(&s[62..]);
        path.push(&s[60..62]);
        path.push(s);
        path.set_extension("bin");
        return path
    }

    // Get a list of all blocks in the background, sending results to the channel.
    async fn block_list_bgrnd(root_path: PathBuf, transmit: tokio::sync::mpsc::Sender<BlockHash>) {
        let mut stack = Vec::new();
        stack.push(root_path);
        while let Some(path) = stack.pop() {
            let dir = tokio::fs::read_dir(path).await.unwrap();
            let mut stream = ReadDirStream::new(dir);
            // it would be fun to spawn a new task for each directory, but that would be a bit daft
            while let Some(entry) = stream.next().await {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let f_name = path.file_stem().unwrap().to_str().unwrap();
                    let h = BlockHash::from_hex(f_name).unwrap();
                    transmit.send(h).await.unwrap();
                }
            }
        }
    }
}

#[async_trait]
impl BlockArchive for SimpleFileBasedBlockArchive
{
    async fn get_block<R>(&self, block_hash: BlockHash) -> Result<R> {
        todo!()
    }

    async fn block_exists(&self, block_hash: BlockHash) -> Result<bool> {
        todo!()
    }

    async fn store_block<S>(&self, block: S) -> Result<()>
        where S: AsyncRead + Unpin + Send + 'async_trait
    {
        todo!()
    }

    async fn block_size(&self, block_hash: BlockHash) -> Result<usize> {
        todo!()
    }

    async fn block_header(&self, block_hash: BlockHash) -> Result<BlockHeader> {
        todo!()
    }

    async fn block_list(&mut self) -> Result<Pin<Box<dyn BlockHashListStream<Item=BlockHash>>>> {
        // make the channel large enough to buffer all hashes, including testnet
        let (tx, rx) = tokio::sync::mpsc::channel(2_000_000);
        let handle = tokio::spawn(Self::block_list_bgrnd(self.root_path.clone(), tx));
        Ok(Box::pin(BlockHashListStreamFromChannel::new(rx, handle)))
    }
}


#[cfg(test)]
mod tests {
    use hex::FromHex;
    use super::*;

    // Test the path generation from a block hash.
    #[test]
    fn check_path_from_hash() {
        let s = SimpleFileBasedBlockArchive::new(PathBuf::from("/"));
        let h = BlockHash::from_hex("00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531").unwrap();
        let path = s.get_path_from_hash(h);
        assert_eq!(path, PathBuf::from("/31/c5/00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531.bin"));
    }
}
