use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use alloy_consensus::BlockHeader as _;
use alloy_primitives::B256;
use anyhow::{Context, Result, bail};
use reth_ethereum::{
    chainspec::ChainSpecBuilder,
    node::{EthereumNode, api::NodeTypesWithDBAdapter},
    primitives::Block as _,
    provider::{
        BlockHashReader, BlockNumReader, BlockReader, HeaderProvider, ProviderFactory,
        ReceiptProvider, TransactionVariant, db::DatabaseEnv, providers::ReadOnlyConfig,
    },
};

#[path = "api.rs"]
mod api;
#[path = "code.rs"]
mod code;
#[path = "convert.rs"]
mod convert;
#[path = "logs.rs"]
mod logs;
#[path = "receipts.rs"]
mod receipts;
use convert::{
    hash_hex, i64_to_u64, parse_b256, provider_block_from_header,
    provider_receipts_and_logs_from_recovered, provider_transactions_from_recovered,
};

use super::{ProviderBlock, ProviderBlockBundle, ProviderHeadSnapshot, ProviderResolvedBlock};

type EthereumRethProviderFactory =
    ProviderFactory<NodeTypesWithDBAdapter<EthereumNode, DatabaseEnv>>;

#[derive(Clone)]
pub struct RethDbProvider {
    reader: Arc<RethDbReader>,
}

struct RethDbReader {
    chain: String,
    datadir: PathBuf,
    factory: OnceLock<Result<Arc<EthereumRethProviderFactory>, String>>,
}

impl RethDbReader {
    fn factory(&self) -> Result<Arc<EthereumRethProviderFactory>> {
        if self.chain != "ethereum-mainnet" {
            bail!(
                "Reth DB provider currently supports ethereum-mainnet only; configured chain {} at {} needs JSON-RPC or a chain-specific Reth reader",
                self.chain,
                self.datadir.display()
            );
        }

        match self.factory.get_or_init(|| {
            open_ethereum_factory(&self.datadir)
                .map(Arc::new)
                .map_err(|error| format!("{error:#}"))
        }) {
            Ok(factory) => Ok(Arc::clone(factory)),
            Err(error) => bail!(
                "failed to open Reth DB provider source for chain {} at {}: {error}",
                self.chain,
                self.datadir.display()
            ),
        }
    }

    fn fetch_chain_heads_sync(&self) -> Result<ProviderHeadSnapshot> {
        let factory = self.factory()?;
        let chain_info = factory.chain_info()?;
        let provider = factory.provider()?;
        let safe_number = nonzero_checkpoint_number(provider.last_safe_block_number()?);
        let finalized_number = nonzero_checkpoint_number(provider.last_finalized_block_number()?);
        drop(provider);

        Ok(ProviderHeadSnapshot {
            canonical: self.fetch_block_by_b256(&factory, chain_info.best_hash)?,
            safe: safe_number
                .map(|number| self.fetch_block_by_number(&factory, number, "safe"))
                .transpose()?,
            finalized: finalized_number
                .map(|number| self.fetch_block_by_number(&factory, number, "finalized"))
                .transpose()?,
        })
    }

    fn fetch_block_hashes_by_numbers_sync(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        let factory = self.factory()?;
        if let Some((from_block, to_block)) = contiguous_block_number_range(block_numbers)? {
            let headers = factory.sealed_headers_range(from_block..=to_block)?;
            if headers.len() != block_numbers.len() {
                bail!(
                    "Reth DB returned {} sealed headers for {} requested block numbers",
                    headers.len(),
                    block_numbers.len()
                );
            }

            return headers
                .iter()
                .zip(block_numbers.iter())
                .map(|(header, requested_number)| {
                    if header.number() != i64_to_u64(*requested_number, "provider block number")? {
                        bail!(
                            "Reth DB returned sealed header number {} for requested block {}",
                            header.number(),
                            requested_number
                        );
                    }
                    Ok(ProviderResolvedBlock {
                        block_number: *requested_number,
                        block_hash: hash_hex(header.hash()),
                    })
                })
                .collect();
        }

        let mut resolved = Vec::with_capacity(block_numbers.len());

        for block_number in block_numbers {
            let number = i64_to_u64(*block_number, "provider block number")?;
            let block_hash = factory.block_hash(number)?.with_context(|| {
                format!("Reth DB did not return block hash for number {number}")
            })?;
            resolved.push(ProviderResolvedBlock {
                block_number: *block_number,
                block_hash: hash_hex(block_hash),
            });
        }

        Ok(resolved)
    }

    fn fetch_block_by_hash_sync(&self, block_hash: &str) -> Result<ProviderBlock> {
        let factory = self.factory()?;
        let block_hash = parse_b256(block_hash, "block hash")?;
        self.fetch_block_by_b256(&factory, block_hash)
    }

    fn fetch_block_headers_by_hashes_sync(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        let factory = self.factory()?;
        if let Some((from_block, to_block)) = contiguous_resolved_block_range(resolved_blocks)? {
            let headers = factory.sealed_headers_range(from_block..=to_block)?;
            if headers.len() != resolved_blocks.len() {
                bail!(
                    "Reth DB returned {} sealed headers for {} resolved block headers",
                    headers.len(),
                    resolved_blocks.len()
                );
            }

            return resolved_blocks
                .iter()
                .zip(headers.iter())
                .map(|(resolved_block, header)| {
                    let header_hash = hash_hex(header.hash());
                    if header.number()
                        != i64_to_u64(resolved_block.block_number, "provider block number")?
                    {
                        bail!(
                            "Reth DB returned sealed header number {} for requested block {}",
                            header.number(),
                            resolved_block.block_number
                        );
                    }
                    if header_hash != resolved_block.block_hash {
                        bail!(
                            "Reth DB resolved block number {} to hash {}, but range header returned hash {}",
                            resolved_block.block_number,
                            resolved_block.block_hash,
                            header_hash
                        );
                    }
                    provider_block_from_header(header.hash(), header.header())
                })
                .collect();
        }

        let mut blocks = Vec::with_capacity(resolved_blocks.len());

        for resolved_block in resolved_blocks {
            let block = self.fetch_block_by_hash_sync(&resolved_block.block_hash)?;
            if block.block_number != resolved_block.block_number {
                bail!(
                    "Reth DB resolved block number {} to hash {}, but hash-scoped fetch returned block number {}",
                    resolved_block.block_number,
                    resolved_block.block_hash,
                    block.block_number
                );
            }
            blocks.push(block);
        }

        Ok(blocks)
    }

    fn fetch_block_bundles_by_hashes_sync(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        include_logs: bool,
    ) -> Result<Vec<ProviderBlockBundle>> {
        let mut bundles = Vec::with_capacity(resolved_blocks.len());

        for resolved_block in resolved_blocks {
            let bundle =
                self.fetch_block_bundle_by_hash_sync(&resolved_block.block_hash, include_logs)?;
            if bundle.block.block_number != resolved_block.block_number {
                bail!(
                    "Reth DB resolved block number {} to hash {}, but hash-scoped fetch returned block number {}",
                    resolved_block.block_number,
                    resolved_block.block_hash,
                    bundle.block.block_number
                );
            }
            bundles.push(bundle);
        }

        Ok(bundles)
    }

    fn fetch_block_bundle_by_hash_sync(
        &self,
        block_hash: &str,
        include_logs: bool,
    ) -> Result<ProviderBlockBundle> {
        let factory = self.factory()?;
        let requested_hash = parse_b256(block_hash, "block hash")?;
        let recovered = factory
            .sealed_block_with_senders(requested_hash.into(), TransactionVariant::WithHash)?
            .with_context(|| {
                format!(
                    "Reth DB did not return recovered block {}",
                    hash_hex(requested_hash)
                )
            })?;
        let receipts = factory
            .receipts_by_block(requested_hash.into())?
            .with_context(|| {
                format!(
                    "Reth DB did not return receipts for block {}",
                    hash_hex(requested_hash)
                )
            })?;
        let block = provider_block_from_header(requested_hash, recovered.header())?;
        let transactions = provider_transactions_from_recovered(&recovered, &block)?;
        let (receipts, logs) =
            provider_receipts_and_logs_from_recovered(&receipts, &recovered, &block, include_logs)?;

        Ok(ProviderBlockBundle {
            block,
            transactions,
            logs,
            receipts,
            raw_payloads: Vec::new(),
        })
    }

    fn fetch_block_by_number(
        &self,
        factory: &EthereumRethProviderFactory,
        number: u64,
        label: &str,
    ) -> Result<ProviderBlock> {
        let block_hash = factory
            .block_hash(number)?
            .with_context(|| format!("Reth DB did not return {label} block hash for {number}"))?;
        self.fetch_block_by_b256(factory, block_hash)
    }

    fn fetch_block_by_b256(
        &self,
        factory: &EthereumRethProviderFactory,
        block_hash: B256,
    ) -> Result<ProviderBlock> {
        let block = factory
            .block_by_hash(block_hash)?
            .with_context(|| format!("Reth DB did not return block {}", hash_hex(block_hash)))?;
        let provider_block = provider_block_from_header(block_hash, block.header())?;
        if provider_block.block_hash != hash_hex(block_hash) {
            bail!(
                "Reth DB returned block {} for requested hash {}",
                provider_block.block_hash,
                hash_hex(block_hash)
            );
        }
        Ok(provider_block)
    }

    fn revalidate_resolved_blocks(&self, resolved_blocks: &[ProviderResolvedBlock]) -> Result<()> {
        let block_numbers = resolved_blocks
            .iter()
            .map(|resolved| resolved.block_number)
            .collect::<Vec<_>>();
        let actual = self.fetch_block_hashes_by_numbers_sync(&block_numbers)?;

        if actual.len() != resolved_blocks.len() {
            bail!(
                "Reth DB revalidated {} blocks for {} requested blocks",
                actual.len(),
                resolved_blocks.len()
            );
        }

        for (expected, actual) in resolved_blocks.iter().zip(actual) {
            if actual.block_number != expected.block_number {
                bail!(
                    "Reth DB revalidated block number {}, but received block number {}",
                    expected.block_number,
                    actual.block_number
                );
            }
            if actual.block_hash != expected.block_hash {
                bail!(
                    "Reth DB block hash changed after range log lookup for block number {}: expected {}, got {}",
                    expected.block_number,
                    expected.block_hash,
                    actual.block_hash
                );
            }
        }

        Ok(())
    }
}

fn open_ethereum_factory(datadir: &Path) -> Result<EthereumRethProviderFactory> {
    validate_ethereum_datadir(datadir)?;
    let spec = ChainSpecBuilder::mainnet().build();
    let runtime = reth_ethereum::tasks::Runtime::test();
    EthereumNode::provider_factory_builder()
        .open_read_only(spec.into(), ReadOnlyConfig::from_datadir(datadir), runtime)
        .map_err(|error| {
            anyhow::anyhow!(
                "failed to open read-only Reth database at {}: {error}; confirm the process has a high nofile limit for RocksDB SST files",
                datadir.display()
            )
        })
        .with_context(|| {
            format!(
                "failed to initialize Reth provider factory for {}",
                datadir.display()
            )
        })
}

fn contiguous_resolved_block_range(
    resolved_blocks: &[ProviderResolvedBlock],
) -> Result<Option<(u64, u64)>> {
    if resolved_blocks.is_empty() {
        return Ok(None);
    }

    let block_numbers = resolved_blocks
        .iter()
        .map(|resolved| resolved.block_number)
        .collect::<Vec<_>>();
    contiguous_block_number_range(&block_numbers)
}

fn contiguous_block_number_range(block_numbers: &[i64]) -> Result<Option<(u64, u64)>> {
    let Some(first) = block_numbers.first() else {
        return Ok(None);
    };
    let first = i64_to_u64(*first, "provider block number")?;
    let mut previous = first;

    for block_number in &block_numbers[1..] {
        let block_number = i64_to_u64(*block_number, "provider block number")?;
        if block_number != previous + 1 {
            return Ok(None);
        }
        previous = block_number;
    }

    Ok(Some((first, previous)))
}

fn nonzero_checkpoint_number(number: Option<u64>) -> Option<u64> {
    number.filter(|number| *number > 0)
}

fn validate_ethereum_datadir(datadir: &Path) -> Result<()> {
    let db_dir = datadir.join("db");
    let static_files_dir = datadir.join("static_files");
    let rocksdb_dir = datadir.join("rocksdb");
    for (label, path) in [
        ("Reth database directory", &db_dir),
        ("Reth static files directory", &static_files_dir),
        ("Reth RocksDB directory", &rocksdb_dir),
    ] {
        if !path.is_dir() {
            bail!(
                "{label} {} does not exist or is not a directory",
                path.display()
            );
        }
    }

    let data_file = db_dir.join("mdbx.dat");
    if !data_file.is_file() {
        bail!(
            "Reth MDBX data file {} does not exist or is not a file",
            data_file.display()
        );
    }

    let lock_file = db_dir.join("mdbx.lck");
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_file)
        .with_context(|| {
            format!(
                "Reth MDBX lock file {} must be writable by the indexer process even when the database is opened read-only",
                lock_file.display()
            )
        })?;

    Ok(())
}
