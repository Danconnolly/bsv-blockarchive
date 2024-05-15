use std::path::PathBuf;
use std::collections::{BTreeSet, VecDeque};
use std::io::Cursor;
use bitcoinsv::bitcoin::{BlockHash, FullBlockStream, ToHex};
use clap::{Parser, Subcommand};
use bsv_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive, Result, Error};
use tokio_stream::StreamExt;

/// A simple CLI for managing block archives.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The root of the block archive.
    #[clap(short = 'r', long, env)]
    root_dir: String,
    /// Emit more status messages.
    #[clap(short = 'v', long, default_value = "false")]
    verbose: bool,
    /// Command to perform
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Perform checks on the archive.
    Check {
        #[command(subcommand)]
        check_cmd: CheckCommands,
    },
    /// Get the header of a block
    Header {
        /// Return hex encoded.
        #[clap(short = 'x', long, default_value = "false")]
        hex: bool,
        /// Block hash.
        block_hash: BlockHash,
    },
    /// List all blocks in the archive.
    List,
}

#[derive(Subcommand, Debug)]
enum CheckCommands {
    /// Check that all blocks are linked in the archive (except the Genesis block).  WARNING: this may take a long time.
    Linked,
    /// Consistency check of a single block.
    ///
    /// The consistency check is not block validation. It checks that the block is consistent which
    /// involves reading every transaction, hashing the transaction, and checking that the merkle
    /// root of the transaction hashes matches the value in the header.
    Block {
        /// Block hash.
        block_hash: BlockHash,
    },
    /// Consistency check of all blocks. WARNING: this may take a long time.
    ///
    /// The consistency check is not block validation. It checks that the block is consistent which
    /// involves reading every transaction, hashing the transaction, and checking that the merkle
    /// root of the transaction hashes matches the value in the header.
    Blocks,
}

async fn list_blocks(root_dir: PathBuf) -> Result<()>{
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut results = archive.block_list().await.unwrap();
    while let Some(block_hash) = results.next().await {
        println!("{}", block_hash);
    }
    Ok(())
}

async fn check_links(root_dir: PathBuf) -> Result<()> {
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut block_it = archive.block_list().await.unwrap();
    // collect all hashes for checking parents
    let mut block_hashes = BTreeSet::new();
    // headers where we didnt find the parent on the first pass
    let mut not_found = Vec::new();
    // for each block
    while let Some(block_hash) = block_it.next().await {
        block_hashes.insert(block_hash);
        let h = archive.block_header(&block_hash).await.unwrap();
        if ! block_hashes.contains(&h.prev_hash) {
            not_found.push(h);
        }
    }
    // check the ones not found yet
    for h in not_found {
        if ! block_hashes.contains(&h.prev_hash) {
            println!("dont have parent of block {}", h.hash())
        }
    }
    Ok(())
}

// check a single block, returns true if all ok, false otherwise
async fn check_single_block(mut block: FullBlockStream) -> Result<bool>{
    // collect transaction hashes
    let mut hashes = VecDeque::new();
    while let Some(tx) = block.next().await {
        match tx {
            Ok(t) => {
                hashes.push_back(t.hash());
            }
            Err(e) => {
                return Err(Error::from(e));
            }
        }
    }
    // calculate merkle root
    while hashes.len() > 1 {
        let mut n = hashes.len();
        while n > 0 {
            n -= 1;
            let h1 = hashes.pop_front().unwrap();
            let h2 = if n == 0 {
                h1
            } else {
                n -= 1;
                hashes.pop_front().unwrap()
            };
            let h = Vec::with_capacity(64);
            let mut c = Cursor::new(h);
            std::io::Write::write(&mut c, &h1.hash).unwrap();
            std::io::Write::write(&mut c, &h2.hash).unwrap();
            let r = BlockHash::sha256d(c.get_ref());
            hashes.push_back(r);
        }
    }
    let m_root = hashes.pop_front().unwrap();
    return Ok(m_root == block.block_header.merkle_root);
}

// check the consistency of a single block
async fn check_block(root_dir: PathBuf, block_hash: BlockHash) -> Result<()> {
    let archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let reader = archive.get_block(&block_hash).await.unwrap();
    let block = FullBlockStream::new(reader).await.unwrap();
    println!("Block hash: {}", block.block_header.hash());
    println!("Number of transactions: {}", block.num_tx);
    let r = check_single_block(block).await.unwrap();
    if r {
        println!("OK: consistency check succeeded block {}", block_hash);
    } else {
        println!("ERROR: merkle root mismatch for block {}", block_hash);
    }
    Ok(())
}

// check all blocks
async fn check_all_blocks(root_dir: PathBuf, verbose: bool) -> Result<()> {
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut block_it = archive.block_list().await.unwrap();
    let mut num = 0;
    let mut errs = 0;
    while let Some(block_hash) = block_it.next().await {
        let reader = archive.get_block(&block_hash).await.unwrap();
        let block = FullBlockStream::new(reader).await.unwrap();
        num += 1;
        match check_single_block(block).await {
            Ok(r) => {
                if r {
                    if verbose {
                        println!("OK: block {}", block_hash);
                    }
                } else {
                    println!("ERROR: block {}", block_hash);
                    errs += 1;
                }
            }
            Err(_) => {
                println!("ERROR: error reading block {}", block_hash);
                errs += 1;
            }
        }
    }
    if verbose {
        println!("{} blocks checked, {} errors found", num, errs);
    }
    Ok(())
}

async fn header(root_dir: PathBuf, block_hash: BlockHash, hex: bool) -> Result<()> {
    let archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    match archive.block_header(&block_hash).await {
        Ok(h) => {
            if hex {
                let x: String = h.encode_hex();
                println!("{}", x);
            } else {
                println!("{:?}", h);
            }
            Ok(())
        },
        Err(e) => {
            match e {
                Error::BlockNotFound => {
                    println!("Block not found");
                    Ok(())
                },
                _ => {
                    Err(e)
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let root_dir = std::path::PathBuf::from(args.root_dir);
    match args.cmd {
        Commands::Check{check_cmd} => {
            match check_cmd {
                CheckCommands::Linked => {
                    check_links(root_dir).await.unwrap();
                }
                CheckCommands::Block{block_hash} => {
                    check_block(root_dir, block_hash).await.unwrap();
                }
                CheckCommands::Blocks => {
                    check_all_blocks(root_dir, args.verbose).await.unwrap();
                }
            }
        }
        Commands::Header{hex, block_hash} => {
            header(root_dir, block_hash, hex).await.unwrap();
        }
        Commands::List => {
            list_blocks(root_dir).await.unwrap();
        }
    };
}
