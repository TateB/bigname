use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::process::{Child, Command};

type ProfileSnapshot = Vec<(PathBuf, Vec<u8>)>;
const E2E_MANIFEST_PROFILE_ENV: &str = "BIGNAME_E2E_MANIFEST_PROFILE_ROOT";
const RUNTIME_PROFILE_MIRROR_PREFIX: &str = ".bigname-e2e-runtime-profile-";

struct CachedProfileRunner {
    snapshot: ProfileSnapshot,
    binary: Weak<ProfileRunnerBinary>,
    managed_lease: Option<Arc<ProfileRunnerBinary>>,
}

struct ProfileRunnerBinary {
    path: PathBuf,
}

struct ProfileBuildLock {
    _file: std::fs::File,
}

impl ProfileBuildLock {
    fn acquire(path: &Path) -> Result<Self> {
        let file = open_profile_build_lock(path)?;
        file.lock()
            .with_context(|| format!("lock deployment-profile Cargo build at {path:?}"))?;
        Ok(Self { _file: file })
    }

    #[cfg(test)]
    fn try_acquire(path: &Path) -> Result<Option<Self>> {
        let file = open_profile_build_lock(path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error)
                .with_context(|| format!("try deployment-profile Cargo build lock at {path:?}")),
        }
    }
}

fn open_profile_build_lock(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open deployment-profile Cargo build lock at {path:?}"))
}

impl ProfileRunnerBinary {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl std::ops::Deref for ProfileRunnerBinary {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for ProfileRunnerBinary {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "failed to remove temporary deployment-profile phase-runner {:?}: {error}",
                self.path
            );
        }
    }
}
type NameProjectionRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Value,
    String,
    Option<String>,
    Value,
    Value,
    Value,
);
type RecordInventoryRow = (Value, Value, Option<Value>, Value, String, Option<String>);
type ChildProjectionRow = (
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<String>,
    Option<String>,
    Value,
    Value,
    Value,
);
type AddressNameProjectionRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    Option<String>,
    Value,
    Value,
    Value,
);
type PrimaryNameProjectionRow = (String, Option<String>, bool, Option<String>, Value);

static PROCESS_LOG_SEQ: AtomicU64 = AtomicU64::new(0);

const DEFAULT_READY_TIMEOUT_SECS: u64 = 600;
const DEFAULT_COMMAND_TIMEOUT_SECS: u64 = 600;
const PROFILE_BINARY_DIR_ENV: &str = "BIGNAME_E2E_PROFILE_BINARY_DIR";

static PHASE_RUNNER_BINARY: tokio::sync::OnceCell<std::result::Result<PathBuf, String>> =
    tokio::sync::OnceCell::const_new();
static PROFILE_PHASE_RUNNERS: OnceLock<tokio::sync::Mutex<Vec<CachedProfileRunner>>> =
    OnceLock::new();

/// Initialize the fresh phase schema through the shipped binary. E2e scenario
/// pools select only `bigname_phase` after this command succeeds.
pub async fn phase_runner_init_schema(repo_root: &Path, database_url: &str) -> Result<()> {
    let binary = canonical_phase_runner(repo_root).await?;
    let mut command = pipeline_command(repo_root, binary);
    command.args(["init-schema", "--database-url", database_url]);
    run_to_completion(command, "phase-runner init-schema").await?;
    Ok(())
}

async fn canonical_phase_runner(repo_root: &Path) -> Result<&'static PathBuf> {
    match PHASE_RUNNER_BINARY
        .get_or_init(|| async {
            build_phase_runner_binary(repo_root, None)
                .await
                .map_err(|error| format!("{error:#}"))
        })
        .await
    {
        Ok(binary) => Ok(binary),
        Err(error) => bail!("{error}"),
    }
}

/// Build a phase-runner whose manifest hash allowlist includes the exact
/// generated scenario [deployment profile](../../../../docs/glossary.md#deployment-profile).
/// Production binaries reject local-address deployment profiles by design;
/// the harness briefly mirrors the deployment profile under the workspace
/// manifest root while compiling, hard-links the resulting executable for
/// concurrent deployment-profile isolation, and removes both the mirror and
/// link through scoped lifetimes. The checked-in manifest tree is never edited.
async fn profile_phase_runner(
    repo_root: &Path,
    manifests_root: &Path,
) -> Result<Arc<ProfileRunnerBinary>> {
    let snapshot = profile_snapshot(manifests_root)?;
    let runners = PROFILE_PHASE_RUNNERS.get_or_init(|| tokio::sync::Mutex::new(Vec::new()));
    let mut runners = runners.lock().await;
    runners.retain(|runner| runner.managed_lease.is_some() || runner.binary.strong_count() > 0);
    if let Some(binary) = runners
        .iter()
        .find(|runner| runner.snapshot == snapshot)
        .and_then(|runner| runner.binary.upgrade())
    {
        return Ok(binary);
    }
    let binary = Arc::new(ProfileRunnerBinary::new(
        build_phase_runner_binary(repo_root, Some(manifests_root)).await?,
    ));
    let managed_lease = std::env::var_os(PROFILE_BINARY_DIR_ENV).map(|_| binary.clone());
    runners.push(CachedProfileRunner {
        snapshot,
        binary: Arc::downgrade(&binary),
        managed_lease,
    });
    Ok(binary)
}

async fn build_phase_runner_binary(
    repo_root: &Path,
    manifests_root: Option<&Path>,
) -> Result<PathBuf> {
    let build_lock_path = profile_build_lock_path(repo_root);
    let _build_lock =
        tokio::task::spawn_blocking(move || ProfileBuildLock::acquire(&build_lock_path))
            .await
            .context("join deployment-profile Cargo build-lock task")??;
    let runtime_profile = match manifests_root {
        Some(profile) => Some(RuntimeProfileMirror::create(repo_root, profile)?),
        None => None,
    };
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut command = Command::new(cargo);
    command.current_dir(repo_root).args([
        "build",
        "--locked",
        "--message-format=json-render-diagnostics",
        "--package",
        "phase-runner",
        "--bin",
        "phase-runner",
    ]);
    if let Some(runtime_profile) = runtime_profile.as_ref() {
        command.env(E2E_MANIFEST_PROFILE_ENV, &runtime_profile.path);
    }
    let stdout = run_to_completion(command, "phase-runner binary build").await?;
    let executable = parse_phase_runner_binary(repo_root, stdout.as_bytes())?;
    if manifests_root.is_none() {
        return Ok(executable);
    }
    let linked_path = hard_link_profile_binary(&executable)?;
    drop(runtime_profile);
    Ok(linked_path)
}

fn profile_build_lock_path(repo_root: &Path) -> PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                repo_root.join(path)
            }
        })
        .unwrap_or_else(|| repo_root.join("target"));
    target_dir.join(".bigname-e2e-phase-runner-build.lock")
}

fn hard_link_profile_binary(executable: &Path) -> Result<PathBuf> {
    let fallback_directory = executable
        .parent()
        .context("phase-runner executable has no parent directory")?;
    let configured_directory = std::env::var_os(PROFILE_BINARY_DIR_ENV).map(PathBuf::from);
    let directory = configured_directory
        .as_deref()
        .unwrap_or(fallback_directory);
    for _ in 0..1000 {
        let sequence = PROCESS_LOG_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(
            ".bigname-e2e-phase-runner-profile-{}-{sequence}",
            std::process::id()
        ));
        match std::fs::hard_link(executable, &path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "hard-link deployment-profile phase-runner from {executable:?} to {path:?}"
                    )
                });
            }
        }
    }
    bail!("could not allocate a unique deployment-profile phase-runner hard link")
}

fn parse_phase_runner_binary(repo_root: &Path, stdout: &[u8]) -> Result<PathBuf> {
    let expected_manifest = repo_root.join("apps/phase-runner/Cargo.toml");
    let mut executable = None;
    for (line_index, line) in stdout.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        let message: Value = serde_json::from_slice(line).with_context(|| {
            format!("parse Cargo JSON message on stdout line {}", line_index + 1)
        })?;
        if message.get("reason").and_then(Value::as_str) != Some("compiler-artifact")
            || message.pointer("/target/name").and_then(Value::as_str) != Some("phase-runner")
            || !message
                .pointer("/target/kind")
                .and_then(Value::as_array)
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
        {
            continue;
        }
        let Some(manifest) = message.get("manifest_path").and_then(Value::as_str) else {
            continue;
        };
        if normalize_cargo_path(repo_root, manifest) != expected_manifest {
            continue;
        }
        let path = message
            .get("executable")
            .and_then(Value::as_str)
            .context("Cargo omitted the phase-runner executable")?;
        if executable
            .replace(normalize_cargo_path(repo_root, path))
            .is_some()
        {
            bail!("Cargo reported duplicate phase-runner executable artifacts");
        }
    }
    executable.context("Cargo build did not report the phase-runner executable artifact")
}

fn profile_snapshot(root: &Path) -> Result<ProfileSnapshot> {
    fn visit(root: &Path, directory: &Path, files: &mut ProfileSnapshot) -> Result<()> {
        let mut entries = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files)?;
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("toml") {
                files.push((path.strip_prefix(root)?.to_owned(), std::fs::read(path)?));
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, root, &mut files)?;
    anyhow::ensure!(
        !files.is_empty(),
        "scenario deployment profile has no TOML files"
    );
    Ok(files)
}

struct RuntimeProfileMirror {
    path: PathBuf,
}

impl RuntimeProfileMirror {
    fn create(repo_root: &Path, source: &Path) -> Result<Self> {
        let manifest_root = repo_root.join("manifests");
        sweep_stale_runtime_profile_mirrors(&manifest_root)?;
        let path = manifest_root.join(format!(
            "{RUNTIME_PROFILE_MIRROR_PREFIX}{}",
            std::process::id()
        ));
        if path.exists() {
            std::fs::remove_dir_all(&path).with_context(|| {
                format!("remove stale runtime deployment-profile mirror {path:?}")
            })?;
        }
        copy_profile_tree(source, &path)?;
        Ok(Self { path })
    }
}

fn sweep_stale_runtime_profile_mirrors(manifest_root: &Path) -> Result<()> {
    for entry in std::fs::read_dir(manifest_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(pid) = name.strip_prefix(RUNTIME_PROFILE_MIRROR_PREFIX) else {
            continue;
        };
        let is_live = pid
            .parse::<u32>()
            .ok()
            .is_some_and(|pid| Path::new("/proc").join(pid.to_string()).is_dir());
        if is_live {
            continue;
        }
        let path = entry.path();
        if let Err(error) = std::fs::remove_dir_all(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error).with_context(|| {
                format!("remove stale runtime deployment-profile mirror {path:?}")
            });
        }
    }
    Ok(())
}

impl Drop for RuntimeProfileMirror {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "failed to remove runtime deployment-profile mirror {:?}: {error}",
                self.path
            );
        }
    }
}

fn copy_profile_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_profile_tree(&source_path, &destination_path)?;
        } else {
            std::fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

/// Execute the fixture-backed schema-v2 spine for one or more local chains.
/// Raw facts are supplied up front from Anvil, then the real phase-runner
/// binary performs interpretation and projection over the recorded extent.
pub async fn run_fixture_spine(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_urls: &[ChainRpcUrl<'_>],
    targets: &[(&str, u64)],
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    let repository = bigname_manifests::load_repository(manifests_root)?;
    bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;

    for (chain, target) in targets {
        let rpc_url = chain_rpc_urls
            .iter()
            .find_map(|(configured_chain, url)| (*configured_chain == *chain).then_some(*url))
            .with_context(|| format!("fixture chain {chain} has no RPC URL"))?;
        super::facts::seed_anvil_snapshot(pool, chain, rpc_url, *target).await?;
    }

    let binary = profile_phase_runner(repo_root, manifests_root).await?;
    for (chain, target) in targets {
        let rpc_url = chain_rpc_urls
            .iter()
            .find_map(|(configured_chain, url)| (*configured_chain == *chain).then_some(*url))
            .expect("checked above");
        run_phase_redo(
            repo_root,
            &binary,
            database_url,
            manifests_root,
            chain,
            "interpret",
            0,
            *target,
            Some(rpc_url),
        )
        .await?;
        run_phase_redo(
            repo_root,
            &binary,
            database_url,
            manifests_root,
            chain,
            "project",
            0,
            *target,
            Some(rpc_url),
        )
        .await?;
    }

    // Redo is synchronous, so one evaluation after command completion replaces
    // the former polling loop. The predicate still carries scenario-specific
    // semantic assertions that must not be discarded.
    if let Some(ready_sql) = extra_ready_sql {
        let ready: bool = sqlx::query_scalar(ready_sql)
            .fetch_one(pool)
            .await
            .with_context(|| format!("evaluate post-redo readiness SQL: {ready_sql}"))?;
        anyhow::ensure!(
            ready,
            "post-redo readiness predicate was false: {ready_sql}"
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Arguments map one-for-one to bounded redo CLI fields.
async fn run_phase_redo(
    repo_root: &Path,
    binary: &Path,
    database_url: &str,
    manifests_root: &Path,
    chain: &str,
    phase: &str,
    from_block: u64,
    target: u64,
    hydration_rpc_url: Option<&str>,
) -> Result<()> {
    let from_block = from_block.to_string();
    let target = target.to_string();
    let (source_kind, seed_basis, source_endpoint) = match (chain, hydration_rpc_url) {
        ("ethereum-sepolia", Some(rpc_url)) => ("drpc", "ethereum_head", rpc_url),
        _ => ("fixture", "new_signature_range", "fixture://upfront"),
    };
    let source =
        format!("{chain}:e2e-fixture:{source_kind}:{seed_basis}:0=BIGNAME_E2E_FIXTURE_SOURCE");
    let mut command = pipeline_command(repo_root, binary);
    command.env("BIGNAME_E2E_FIXTURE_SOURCE", source_endpoint);
    command
        .args(["redo", "--database-url", database_url, "--manifests-root"])
        .arg(manifests_root)
        .args([
            "--chain",
            chain,
            "--source",
            &source,
            "--phase",
            phase,
            "--from-block",
            &from_block,
            "--to-block",
            &target,
            "--initial-backoff-ms",
            "10",
            "--maximum-backoff-ms",
            "50",
        ]);
    if let Some(rpc_url) = hydration_rpc_url {
        command.args(["--hydration-rpc", &format!("{chain}={rpc_url}")]);
    }
    run_to_completion(command, &format!("phase-runner {phase} redo for {chain}")).await?;
    Ok(())
}

/// Run the production JSON-RPC ingest phase over a bounded local-chain range.
/// Provider-fault scenarios use a non-production chain alias because the
/// production `ethereum-mainnet` source contract requires a local Reth DB.
#[allow(clippy::too_many_arguments)] // Public fault scenarios spell out every ingest boundary.
pub async fn run_rpc_ingest_redo(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain: &str,
    rpc_url: &str,
    from_block: u64,
    to_block: u64,
) -> Result<String> {
    anyhow::ensure!(from_block <= to_block, "RPC ingest redo range is reversed");
    super::facts::seed_anvil_rpc_redo_extent(pool, chain, rpc_url, to_block).await?;
    let repository = bigname_manifests::load_repository(manifests_root)?;
    bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
    let binary = profile_phase_runner(repo_root, manifests_root).await?;
    let source = format!("{chain}:e2e-rpc:rpc:new_signature_range:0=BIGNAME_E2E_RPC_SOURCE");
    let mut command = pipeline_command(repo_root, &binary);
    command.env("BIGNAME_E2E_RPC_SOURCE", rpc_url);
    command
        .args(["redo", "--database-url", database_url, "--manifests-root"])
        .arg(manifests_root)
        .args([
            "--chain",
            chain,
            "--source",
            &source,
            "--phase",
            "ingest",
            "--from-block",
            &from_block.to_string(),
            "--to-block",
            &to_block.to_string(),
            "--initial-backoff-ms",
            "10",
            "--maximum-backoff-ms",
            "50",
        ]);
    let output = run_to_completion(
        command,
        &format!("phase-runner RPC ingest redo for {chain}"),
    )
    .await?;

    // A production redo operates over lineage whose canonicality was already
    // established by live head tracking. This harness bootstraps that recorded
    // extent so the ingest implementation itself can be exercised without
    // first running a long-lived supervisor. The redo still
    // fetches its headers, selected logs, transactions, and receipts from the
    // configured (and potentially faulting) provider.
    let promoted = sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'canonical'
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
           AND canonicality_state = 'observed'",
    )
    .bind(chain)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .execute(pool)
    .await?;
    let expected = to_block - from_block + 1;
    let readable: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT block_number) FROM chain_lineage
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        readable == i64::try_from(expected)?,
        "RPC ingest redo left {readable}/{expected} readable lineage blocks for {chain}; promoted {} observed rows",
        promoted.rows_affected()
    );
    let latest_hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .bind(i64::try_from(to_block)?)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)
         ON CONFLICT (chain_id) DO UPDATE
         SET latest_block_hash = EXCLUDED.latest_block_hash,
             latest_block_number = EXCLUDED.latest_block_number,
             updated_at = now()",
    )
    .bind(chain)
    .bind(latest_hash)
    .bind(i64::try_from(to_block)?)
    .execute(pool)
    .await?;
    Ok(output)
}

/// Interpret and project raw facts already materialized by a real ingest run.
pub async fn run_existing_raw_spine(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain: &str,
    rpc_url: &str,
    to_block: u64,
) -> Result<()> {
    super::facts::seed_downstream_redo_extents(pool, chain, to_block).await?;
    let repository = bigname_manifests::load_repository(manifests_root)?;
    bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
    let binary = profile_phase_runner(repo_root, manifests_root).await?;
    let source = format!("{chain}:e2e-rpc:rpc:new_signature_range:0=BIGNAME_E2E_RPC_SOURCE");
    for phase in ["interpret", "project"] {
        let mut command = pipeline_command(repo_root, &binary);
        command.env("BIGNAME_E2E_RPC_SOURCE", rpc_url);
        command
            .args(["redo", "--database-url", database_url, "--manifests-root"])
            .arg(manifests_root)
            .args([
                "--chain",
                chain,
                "--source",
                &source,
                "--phase",
                phase,
                "--from-block",
                "0",
                "--to-block",
                &to_block.to_string(),
                "--hydration-rpc",
                &format!("{chain}={rpc_url}"),
                "--initial-backoff-ms",
                "10",
                "--maximum-backoff-ms",
                "50",
            ]);
        run_to_completion(command, &format!("phase-runner {phase} redo for {chain}")).await?;
    }
    Ok(())
}

/// Rewind the published head through the production operator command. The
/// command preserves losing raw facts as orphaned lineage and stamps the
/// affected interpret/project range for mandatory replay.
pub async fn rewind_to_ancestor(
    repo_root: &Path,
    database_url: &str,
    chain: &str,
    ancestor_block: u64,
    ancestor_hash: &str,
) -> Result<String> {
    let binary = canonical_phase_runner(repo_root).await?;
    let mut command = pipeline_command(repo_root, binary);
    command.args([
        "rewind",
        "--database-url",
        database_url,
        "--chain",
        chain,
        "--ancestor-block",
        &ancestor_block.to_string(),
        "--ancestor-hash",
        ancestor_hash,
    ]);
    run_to_completion(command, &format!("phase-runner rewind for {chain}")).await
}

/// Complete the exact downstream replay stamped by a production rewind.
/// `redo interpret` owns the projection cascade, so one command consumes both
/// required ranges and preserves the phase runner's recovery contract.
pub async fn run_required_reorg_spine(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain: &str,
    rpc_url: &str,
) -> Result<()> {
    let ranges: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
           AND redo_in_progress
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        ranges.len() == 2
            && ranges[0].0 == "interpret"
            && ranges[1].0 == "project"
            && ranges[0].1 == ranges[1].1
            && ranges[0].2 == ranges[1].2,
        "rewind for {chain} did not stamp one matching interpret/project range: {ranges:?}"
    );
    let from = ranges[0].1.to_string();
    let to = ranges[0].2.to_string();
    let repository = bigname_manifests::load_repository(manifests_root)?;
    bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
    let binary = profile_phase_runner(repo_root, manifests_root).await?;
    let source = format!("{chain}:e2e-rpc:rpc:new_signature_range:0=BIGNAME_E2E_RPC_SOURCE");
    let mut command = pipeline_command(repo_root, &binary);
    command.env("BIGNAME_E2E_RPC_SOURCE", rpc_url);
    command
        .args(["redo", "--database-url", database_url, "--manifests-root"])
        .arg(manifests_root)
        .args([
            "--chain",
            chain,
            "--source",
            &source,
            "--phase",
            "interpret",
            "--from-block",
            &from,
            "--to-block",
            &to,
            "--hydration-rpc",
            &format!("{chain}={rpc_url}"),
            "--initial-backoff-ms",
            "10",
            "--maximum-backoff-ms",
            "50",
        ]);
    run_to_completion(
        command,
        &format!("phase-runner required reorg spine for {chain}"),
    )
    .await?;
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
           AND redo_in_progress",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        remaining == 0,
        "phase-runner left {remaining} required reorg phases active for {chain}"
    );
    Ok(())
}

#[cfg(unix)]
mod unix_process {
    use std::io;

    const SIGKILL: i32 = 9;
    #[cfg(test)]
    const ESRCH: i32 = 3;

    unsafe extern "C" {
        #[link_name = "kill"]
        fn c_kill(pid: i32, signal: i32) -> i32;
        fn getpgrp() -> i32;
    }

    pub fn kill_process_group(process_group: u32) -> io::Result<()> {
        let process_group = positive_pid(process_group)?;
        // SAFETY: getpgrp takes no arguments and has no memory-safety
        // preconditions.
        let harness_process_group = unsafe { getpgrp() };
        if process_group == harness_process_group {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing to signal the harness process group",
            ));
        }
        // SAFETY: a negative, nonzero pid addresses one Unix process group.
        // The equality guard above prevents signaling the harness group.
        if unsafe { c_kill(-process_group, SIGKILL) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(test)]
    pub fn process_exists(pid: u32) -> io::Result<bool> {
        let pid = positive_pid(pid)?;
        // SAFETY: signal 0 performs existence/permission checking only.
        if unsafe { c_kill(pid, 0) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }

    #[cfg(test)]
    pub fn kill_process(pid: u32) -> io::Result<()> {
        let pid = positive_pid(pid)?;
        // SAFETY: pid is a validated positive process id and SIGKILL has no
        // userspace memory-safety preconditions.
        if unsafe { c_kill(pid, SIGKILL) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn positive_pid(pid: u32) -> io::Result<i32> {
        let pid = i32::try_from(pid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds i32"))?;
        if pid == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process id must be positive",
            ));
        }
        Ok(pid)
    }
}

#[derive(Clone, Copy, Debug)]
enum TimeoutTerminationTarget {
    DirectChild,
    #[cfg(unix)]
    ProcessGroup(u32),
}

fn normalize_cargo_path(repo_root: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn pipeline_command(repo_root: &Path, executable: &Path) -> Command {
    let mut command = Command::new(executable);
    command.current_dir(repo_root);
    command.env("BIGNAME_DATABASE_MAX_CONNECTIONS", "4");
    command
}

async fn run_to_completion(command: Command, what: &str) -> Result<String> {
    let timeout_secs = timeout_secs_from_env(
        "BIGNAME_E2E_COMMAND_TIMEOUT_SECS",
        DEFAULT_COMMAND_TIMEOUT_SECS,
    )?;
    run_to_completion_with_timeout(command, what, timeout_secs).await
}

async fn run_to_completion_with_timeout(
    mut command: Command,
    what: &str,
    timeout_secs: u64,
) -> Result<String> {
    let (stdout_path, stdout_file) = create_process_log_file("command-stdout", what)?;
    let (stderr_path, stderr_file) = match create_process_log_file("command-stderr", what) {
        Ok(log) => log,
        Err(error) => {
            std::fs::remove_file(&stdout_path).ok();
            return Err(error);
        }
    };
    isolate_bounded_command(&mut command);
    command
        .kill_on_drop(true)
        .stdout(std::process::Stdio::from(stdout_file))
        .stderr(std::process::Stdio::from(stderr_file));
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            std::fs::remove_file(&stdout_path).ok();
            std::fs::remove_file(&stderr_path).ok();
            return Err(error).with_context(|| format!("spawn {what}"));
        }
    };
    let termination_target = timeout_termination_target(&child);
    let status = match tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait()).await {
        Ok(status) => status.with_context(|| format!("wait for {what}"))?,
        Err(_) => {
            let stop_note = stop_and_reap_timed_out_child(&mut child, termination_target).await;
            bail!(
                "{what} exceeded the configured BIGNAME_E2E_COMMAND_TIMEOUT_SECS deadline of {timeout_secs}s ({stop_note}); stdout log {stdout_path:?}, stderr log {stderr_path:?}; stdout tail (reversed):\n{}\nstderr tail (reversed):\n{}",
                process_log_tail(&stdout_path),
                process_log_tail(&stderr_path)
            );
        }
    };
    let stdout = read_process_log(&stdout_path);
    if !status.success() {
        bail!(
            "{what} failed ({status}); stdout log {stdout_path:?}, stderr log {stderr_path:?}; stdout tail (reversed):\n{}\nstderr tail (reversed):\n{}",
            process_log_tail(&stdout_path),
            process_log_tail(&stderr_path)
        );
    }
    std::fs::remove_file(stdout_path).ok();
    std::fs::remove_file(stderr_path).ok();
    Ok(stdout)
}

#[cfg(unix)]
fn isolate_bounded_command(command: &mut Command) {
    // PGID 0 asks the child to become leader of a new process group. Its
    // descendants inherit that group unless they explicitly leave it.
    command.process_group(0);
}

#[cfg(not(unix))]
fn isolate_bounded_command(_command: &mut Command) {}

fn timeout_termination_target(child: &Child) -> TimeoutTerminationTarget {
    #[cfg(unix)]
    if let Some(process_group) = child.id() {
        return TimeoutTerminationTarget::ProcessGroup(process_group);
    }
    TimeoutTerminationTarget::DirectChild
}

async fn stop_and_reap_timed_out_child(
    child: &mut Child,
    target: TimeoutTerminationTarget,
) -> String {
    let stop_note = request_timeout_stop(child, target);
    let reap = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    match reap {
        Ok(Ok(status)) => format!("{stop_note}; process stopped and reaped with {status}"),
        Ok(Err(wait_error)) => {
            format!("{stop_note}; process reap failed: {wait_error}")
        }
        Err(_) => format!("{stop_note}; process was not reaped within 5s"),
    }
}

fn request_timeout_stop(child: &mut Child, target: TimeoutTerminationTarget) -> String {
    match target {
        TimeoutTerminationTarget::DirectChild => match child.start_kill() {
            Ok(()) => "direct child termination requested".to_string(),
            Err(error) => format!("direct child termination failed: {error}"),
        },
        #[cfg(unix)]
        TimeoutTerminationTarget::ProcessGroup(process_group) => {
            match unix_process::kill_process_group(process_group) {
                Ok(()) => format!("process group {process_group} termination requested"),
                Err(group_error) => match child.start_kill() {
                    Ok(()) => format!(
                        "process group {process_group} termination failed ({group_error}); direct child termination requested"
                    ),
                    Err(child_error) => format!(
                        "process group {process_group} termination failed ({group_error}); direct child termination failed ({child_error})"
                    ),
                },
            }
        }
    }
}

fn timeout_secs_from_env(variable: &str, default: u64) -> Result<u64> {
    match std::env::var(variable) {
        Ok(value) => parse_timeout_secs(variable, &value),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {variable}")),
    }
}

fn parse_timeout_secs(variable: &str, value: &str) -> Result<u64> {
    let seconds = value
        .parse::<u64>()
        .with_context(|| format!("{variable} must be a positive integer number of seconds"))?;
    if seconds == 0 {
        bail!("{variable} must be greater than zero");
    }
    Ok(seconds)
}

pub(crate) fn ready_timeout_secs() -> Result<u64> {
    timeout_secs_from_env("BIGNAME_E2E_READY_TIMEOUT_SECS", DEFAULT_READY_TIMEOUT_SECS)
}

pub(super) fn deadline_after(seconds: u64, what: &str) -> Result<tokio::time::Instant> {
    tokio::time::Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .with_context(|| format!("{what} timeout is too large"))
}

pub(super) async fn await_with_readiness_deadline<F>(
    deadline: tokio::time::Instant,
    ready_timeout_secs: u64,
    what: impl Into<String>,
    future: F,
) -> Result<F::Output>
where
    F: std::future::Future,
{
    let what = what.into();
    match tokio::time::timeout_at(deadline, future).await {
        Ok(output) => Ok(output),
        Err(_) => bail!("{what} exceeded the configured {ready_timeout_secs}s readiness deadline"),
    }
}

#[cfg(test)]
async fn await_supervised_readiness<T, F>(
    child: &mut Child,
    log_path: &Path,
    process_name: &str,
    deadline: tokio::time::Instant,
    ready_timeout_secs: u64,
    what: impl Into<String>,
    future: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    match await_with_readiness_deadline(deadline, ready_timeout_secs, what, future).await {
        Ok(result) => result,
        Err(error) => {
            let stop_note =
                stop_and_reap_timed_out_child(child, TimeoutTerminationTarget::DirectChild).await;
            Err(error.context(format!(
                "{process_name} stopped after readiness timeout ({stop_note}); log tail (reversed) from {log_path:?}:\n{}",
                process_log_tail(log_path)
            )))
        }
    }
}

fn sanitize_log_label(label: &str) -> String {
    let label = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let label = label.trim_matches('-');
    if label.is_empty() {
        "process".to_string()
    } else {
        label.to_string()
    }
}

fn create_process_log_file(process_kind: &str, label: &str) -> Result<(PathBuf, std::fs::File)> {
    let label = sanitize_log_label(label);
    for _ in 0..1000 {
        let sequence = PROCESS_LOG_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-e2e-{process_kind}-{}-{label}-{sequence}.log",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("create process log at {path:?}"));
            }
        }
    }
    bail!("could not allocate a unique {process_kind} log path")
}

fn read_process_log(log_path: &Path) -> String {
    String::from_utf8_lossy(&std::fs::read(log_path).unwrap_or_default()).into_owned()
}

fn process_log_tail(log_path: &Path) -> String {
    let log = read_process_log(log_path);
    log.lines().rev().take(60).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
async fn stop_supervised_child(child: Child, what: &str, log_path: &Path) -> Result<()> {
    stop_supervised_child_with_pre_kill_action(child, what, log_path, None).await
}

#[cfg(test)]
enum PreKillAction {
    #[cfg(target_os = "linux")]
    CloseStdinAndAwaitExitedUnreaped,
}

#[cfg(test)]
async fn stop_supervised_child_with_pre_kill_action(
    mut child: Child,
    what: &str,
    log_path: &Path,
    pre_kill_action: Option<PreKillAction>,
) -> Result<()> {
    if let Some(status) = child.try_wait()? {
        bail!(
            "{what} exited before requested stop ({status}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }

    #[cfg(target_os = "linux")]
    if let Some(PreKillAction::CloseStdinAndAwaitExitedUnreaped) = pre_kill_action {
        let child_id = child
            .id()
            .context("test child has no process ID after its status check")?;
        drop(
            child
                .stdin
                .take()
                .context("test child has no piped stdin to close after its status check")?,
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if process_is_zombie(child_id)? {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("test child did not exit after stdin closed")??;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = pre_kill_action;
    if let Err(kill_error) = child.start_kill() {
        let status = child
            .wait()
            .await
            .with_context(|| format!("failed to reap {what} after stop failed"))?;
        bail!(
            "{what} exited while stop was requested ({status}; stop error: {kill_error}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }
    let status = child
        .wait()
        .await
        .with_context(|| format!("failed to reap {what} after stop"))?;
    if !exited_from_requested_kill(&status) {
        bail!(
            "{what} exited independently while stop was requested ({status}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
fn process_is_zombie(process_id: u32) -> Result<bool> {
    let stat_path = format!("/proc/{process_id}/stat");
    let stat = std::fs::read_to_string(&stat_path)
        .with_context(|| format!("read test child status from {stat_path}"))?;
    let (_, fields) = stat
        .rsplit_once(") ")
        .context("test child status has no process-name boundary")?;
    Ok(fields.starts_with("Z "))
}

#[cfg(test)]
fn exited_from_requested_kill(status: &std::process::ExitStatus) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        // Tokio's start_kill is SIGKILL on Unix. An ordinary nonzero exit or
        // another fatal signal therefore remains distinguishable from the
        // harness-requested stop even if it races with start_kill.
        status.signal() == Some(9)
    }

    #[cfg(not(unix))]
    {
        !status.success()
    }
}

pub type ChainRpcUrl<'a> = (&'a str, &'a str);

pub struct ChainReplayTarget<'a> {
    pub chain_rpc_urls: &'a [ChainRpcUrl<'a>],
    pub chain: &'a str,
    pub target_block: u64,
    pub extra_ready_sql: Option<&'a str>,
}

pub struct FullFixtureReplayTarget<'a> {
    pub chain_rpc_urls: &'a [ChainRpcUrl<'a>],
    pub chain: &'a str,
    pub block_range: std::ops::RangeInclusive<u64>,
}

/// Re-run the fixture-backed spine as a scenario adds blocks. Each call
/// snapshots the requested Anvil range and synchronously executes the real
/// phase-runner interpret/project commands; no background process is implied.
pub struct SequentialFixtureReplay {
    repo_root: PathBuf,
    database_url: String,
    manifests_root: PathBuf,
    chain_rpc_urls: Vec<(String, String)>,
    _binary: Arc<ProfileRunnerBinary>,
}

impl SequentialFixtureReplay {
    pub async fn start_with_chain_rpc_urls(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
    ) -> Result<Self> {
        let binary = profile_phase_runner(repo_root, manifests_root).await?;
        Ok(Self {
            repo_root: repo_root.to_owned(),
            database_url: database_url.to_owned(),
            manifests_root: manifests_root.to_owned(),
            chain_rpc_urls: chain_rpc_urls
                .iter()
                .map(|(chain, url)| ((*chain).to_owned(), (*url).to_owned()))
                .collect(),
            _binary: binary,
        })
    }

    pub async fn replay_current_chain_head(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
    ) -> Result<i64> {
        let url = self.rpc_url(chain)?;
        let target = super::rpc::RpcClient::new(url.to_owned())
            .block_number()
            .await?;
        self.replay_chain_through(pool, chain, target, None).await?;
        Ok(i64::try_from(target)?)
    }

    pub async fn replay_chain_through(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
        target_block: u64,
        extra_ready_sql: Option<&str>,
    ) -> Result<()> {
        let rpc_url = self.rpc_url(chain)?.to_owned();
        let repository = bigname_manifests::load_repository(&self.manifests_root)?;
        bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
        super::facts::seed_anvil_snapshot(pool, chain, &rpc_url, target_block).await?;
        for phase in ["interpret", "project"] {
            run_phase_redo(
                &self.repo_root,
                &self._binary,
                &self.database_url,
                &self.manifests_root,
                chain,
                phase,
                0,
                target_block,
                Some(&rpc_url),
            )
            .await?;
        }
        if let Some(ready_sql) = extra_ready_sql {
            let ready: bool = sqlx::query_scalar(ready_sql)
                .fetch_one(pool)
                .await
                .with_context(|| format!("evaluate post-redo readiness SQL: {ready_sql}"))?;
            anyhow::ensure!(
                ready,
                "post-redo readiness predicate was false: {ready_sql}"
            );
        }
        Ok(())
    }

    pub async fn replay_chain_range(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
        from_block: u64,
        target_block: u64,
        extra_ready_sql: Option<&str>,
    ) -> Result<()> {
        anyhow::ensure!(
            from_block <= target_block,
            "sequential fixture replay range is reversed"
        );
        let rpc_url = self.rpc_url(chain)?.to_owned();
        let previous_hashes: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT phase_name, input_content_hash
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
        )
        .bind(chain)
        .fetch_all(pool)
        .await?;
        anyhow::ensure!(
            previous_hashes.len() == 2,
            "fixture derived phase state is missing"
        );
        anyhow::ensure!(
            previous_hashes.iter().all(|row| row
                .1
                .as_deref()
                .is_some_and(|hash| hash.starts_with("keccak256:"))),
            "incremental fixture replay must continue one compiled-hash epoch"
        );
        // The initial replay already synchronized this immutable deployment profile. Re-syncing
        // between fixture windows would stamp a manifest-authority redo and turn the incremental
        // history test into a full interpretation replay.
        super::facts::seed_anvil_snapshot(pool, chain, &rpc_url, target_block).await?;
        for (phase, content_hash) in previous_hashes {
            sqlx::query(
                "UPDATE chain_phase_state
                 SET input_content_hash = $3
                 WHERE chain_id = $1 AND phase_name = $2",
            )
            .bind(chain)
            .bind(phase)
            .bind(content_hash)
            .execute(pool)
            .await?;
        }
        run_phase_redo(
            &self.repo_root,
            &self._binary,
            &self.database_url,
            &self.manifests_root,
            chain,
            "interpret",
            from_block,
            target_block,
            Some(&rpc_url),
        )
        .await?;
        let interpreter_hash: String = sqlx::query_scalar(
            "SELECT input_content_hash FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'interpret'",
        )
        .bind(chain)
        .fetch_one(pool)
        .await?;
        sqlx::query(
            "UPDATE chain_phase_state SET input_content_hash = $2
             WHERE chain_id = $1 AND phase_name = 'project'",
        )
        .bind(chain)
        .bind(interpreter_hash)
        .execute(pool)
        .await?;
        run_phase_redo(
            &self.repo_root,
            &self._binary,
            &self.database_url,
            &self.manifests_root,
            chain,
            "project",
            from_block,
            target_block,
            Some(&rpc_url),
        )
        .await?;
        if let Some(ready_sql) = extra_ready_sql {
            let ready: bool = sqlx::query_scalar(ready_sql)
                .fetch_one(pool)
                .await
                .with_context(|| format!("evaluate post-redo readiness SQL: {ready_sql}"))?;
            anyhow::ensure!(
                ready,
                "post-redo readiness predicate was false: {ready_sql}"
            );
        }
        Ok(())
    }

    fn rpc_url(&self, chain: &str) -> Result<&str> {
        self.chain_rpc_urls
            .iter()
            .find_map(|(configured_chain, url)| (configured_chain == chain).then_some(url.as_str()))
            .with_context(|| format!("sequential fixture replay has no RPC URL for {chain}"))
    }
}

pub async fn run_full_fixture_replay(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    target: FullFixtureReplayTarget<'_>,
) -> Result<()> {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    anyhow::ensure!(
        *target.block_range.start() == 0,
        "full fixture replay must begin at block zero"
    );
    let options =
        PgConnectOptions::from_str(database_url)?.options([("search_path", "bigname_phase")]);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    run_fixture_spine(
        repo_root,
        database_url,
        &pool,
        manifests_root,
        target.chain_rpc_urls,
        &[(target.chain, *target.block_range.end())],
        None,
    )
    .await?;
    Ok(())
}

/// Materialize one local-chain snapshot, then run the phase-runner
/// interpret/project redo spine through the selected block.
pub async fn run_fixture_spine_through_block(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_url: &str,
    target_block: u64,
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    let chain_rpc_urls = [("ethereum-mainnet", chain_rpc_url)];
    run_fixture_spine_through_chain_block(
        repo_root,
        database_url,
        pool,
        manifests_root,
        ChainReplayTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: "ethereum-mainnet",
            target_block,
            extra_ready_sql,
        },
    )
    .await
}

pub async fn run_fixture_spine_through_chain_block(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    target: ChainReplayTarget<'_>,
) -> Result<()> {
    run_fixture_spine(
        repo_root,
        database_url,
        pool,
        manifests_root,
        target.chain_rpc_urls,
        &[(target.chain, target.target_block)],
        target.extra_ready_sql,
    )
    .await
}

/// Materialize multiple local-chain snapshots and execute each selected
/// phase-runner fixture spine sequentially.
pub async fn run_fixture_spines_through_targets(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_urls: &[ChainRpcUrl<'_>],
    targets: &[(&str, u64)],
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    run_fixture_spine(
        repo_root,
        database_url,
        pool,
        manifests_root,
        chain_rpc_urls,
        targets,
        extra_ready_sql,
    )
    .await
}

pub struct ReplayCompletion {
    pub target_block: u64,
    pub extra_ready_sql: Option<String>,
}

pub async fn run_fixture_spine_with_midpoint<F, Fut>(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_url: &str,
    after_first_replay: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<ReplayCompletion>>,
{
    let first_target = super::rpc::RpcClient::new(chain_rpc_url.to_owned())
        .block_number()
        .await?;
    run_fixture_spine_through_block(
        repo_root,
        database_url,
        pool,
        manifests_root,
        chain_rpc_url,
        first_target,
        None,
    )
    .await?;

    let completion = after_first_replay().await?;
    run_fixture_spine_through_block(
        repo_root,
        database_url,
        pool,
        manifests_root,
        chain_rpc_url,
        completion.target_block,
        completion.extra_ready_sql.as_deref(),
    )
    .await
}

/// Full-range interpretation replay from the already stored immutable facts.
pub async fn phase_runner_replay_normalized_events(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    chain_rpc_url: &str,
    to_block: u64,
) -> Result<String> {
    let binary = profile_phase_runner(repo_root, manifests_root).await?;
    run_phase_redo(
        repo_root,
        &binary,
        database_url,
        manifests_root,
        "ethereum-mainnet",
        "interpret",
        0,
        to_block,
        Some(chain_rpc_url),
    )
    .await?;
    run_phase_redo(
        repo_root,
        &binary,
        database_url,
        manifests_root,
        "ethereum-mainnet",
        "project",
        0,
        to_block,
        Some(chain_rpc_url),
    )
    .await?;
    Ok("phase-runner interpretation replay completed".to_owned())
}

/// Full-range projection replay over the currently normalized schema-v2
/// events, executed by the production phase-runner binary.
pub async fn phase_runner_replay_current_projections(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    chain_rpc_url: &str,
    to_block: u64,
) -> Result<String> {
    let binary = profile_phase_runner(repo_root, manifests_root).await?;
    run_phase_redo(
        repo_root,
        &binary,
        database_url,
        manifests_root,
        "ethereum-mainnet",
        "project",
        0,
        to_block,
        Some(chain_rpc_url),
    )
    .await?;
    Ok("phase-runner projection replay completed".to_owned())
}

/// Direct schema-v2 projection reader. The route-shaped methods preserve
/// recognizable scenario assertions; no API process starts and no public API
/// behavior is implied.
pub struct ProjectionReader {
    pool: sqlx::PgPool,
}

impl ProjectionReader {
    pub async fn start(
        _repo_root: &Path,
        database_url: &str,
        _chain_rpc_urls: &[ChainRpcUrl<'_>],
    ) -> Result<Self> {
        use sqlx::postgres::PgConnectOptions;
        use std::str::FromStr;

        let options =
            PgConnectOptions::from_str(database_url)?.options([("search_path", "bigname_phase")]);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }

    pub async fn get_json(&self, path: &str) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let route = path.split('?').next().unwrap_or(path);
        let segments = route
            .trim_start_matches('/')
            .split('/')
            .map(decode_path_segment)
            .collect::<Vec<_>>();
        if let ["v1", "names", namespace, name] = segments.as_slice() {
            return self.exact_name_projection(namespace, name).await;
        }
        if let ["v1", "names", namespace, name, "records"] = segments.as_slice() {
            return self
                .record_inventory_projection(namespace, name, path)
                .await;
        }
        if let ["v1", "names", namespace, name, "children"] = segments.as_slice() {
            return self.children_projection(namespace, name).await;
        }
        if let ["v1", "addresses", address, "names"] = segments.as_slice() {
            return self.address_names_projection(address, path).await;
        }
        if let ["v1", "primary-names", address] = segments.as_slice() {
            return self.primary_name_projection(address, path).await;
        }
        if let ["v1", "manifests", namespace] = segments.as_slice() {
            return self.manifest_projection(namespace).await;
        }
        Ok((
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": {
                    "code": "unsupported_projection_reader_route",
                    "message": format!("projection reader has no mapping for {path}")
                }
            }),
        ))
    }

    pub async fn post_json(
        &self,
        _path: &str,
        _request: &serde_json::Value,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        Ok((
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"unsupported_projection_reader_route"}}),
        ))
    }

    async fn exact_name_projection(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let row: Option<NameProjectionRow> = sqlx::query_as(
            "SELECT logical_name_id, namespace, raw_name, namehash,
                    resource_id::text, token_lineage_id::text, binding_kind,
                    declared_summary, support_status, unsupported_reason,
                    provenance, chain_positions, canonicality_summary
             FROM name_current
             WHERE namespace = $1 AND raw_name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        let Some((
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            resource_id,
            token_lineage_id,
            binding_kind,
            mut declared_state,
            support_status,
            unsupported_reason,
            provenance,
            chain_positions,
            canonicality_summary,
        )) = row
        else {
            return Ok((
                reqwest::StatusCode::NOT_FOUND,
                serde_json::json!({"error":{"code":"not_found"}}),
            ));
        };

        if let Some(resource_id) = &resource_id {
            let inventory: Option<RecordInventoryRow> = sqlx::query_as(
                "SELECT selectors, unsupported_families, last_change,
                            record_version_boundary, support_status, unsupported_reason
                     FROM record_inventory_current
                     WHERE resource_id = $1::uuid
                     ORDER BY inserted_at DESC LIMIT 1",
            )
            .bind(resource_id)
            .fetch_optional(&self.pool)
            .await?;
            if let Some((
                selectors,
                unsupported_families,
                last_change,
                record_version_boundary,
                inventory_support,
                inventory_reason,
            )) = inventory
                && let Some(object) = declared_state.as_object_mut()
            {
                object.insert(
                    "record_inventory".to_owned(),
                    serde_json::json!({
                        "selectors": selectors,
                        "unsupported_families": unsupported_families,
                        "last_change": last_change,
                        "record_version_boundary": record_version_boundary,
                        "support_status": inventory_support,
                        "unsupported_reason": inventory_reason,
                    }),
                );
            }
        }
        let coverage = declared_state
            .get("coverage")
            .cloned()
            .context("name_current.declared_summary omitted persisted coverage")?;
        Ok((
            reqwest::StatusCode::OK,
            serde_json::json!({
                "data": {
                    "normalized_name": raw_name,
                    "logical_name_id": logical_name_id,
                    "namespace": namespace,
                    "namehash": namehash,
                    "resource_id": resource_id,
                    "token_lineage_id": token_lineage_id,
                    "binding_kind": binding_kind,
                },
                "declared_state": declared_state,
                "coverage": coverage,
                "support_status": support_status,
                "unsupported_reason": unsupported_reason,
                "provenance": provenance,
                "chain_positions": chain_positions,
                "canonicality_summary": canonicality_summary,
            }),
        ))
    }

    async fn record_inventory_projection(
        &self,
        namespace: &str,
        name: &str,
        path: &str,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let current: Option<(Option<String>, Option<String>, Value)> = sqlx::query_as(
            "SELECT resource_id::text, serving_resource_id::text, declared_summary
             FROM name_current WHERE namespace = $1 AND raw_name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        let Some((resource_id, serving_resource_id, declared_summary)) = current else {
            return Ok((
                reqwest::StatusCode::NOT_FOUND,
                serde_json::json!({"error":{"code":"not_found"}}),
            ));
        };
        let resolver_address = declared_summary
            .pointer("/resolver/address")
            .cloned()
            .unwrap_or(Value::Null);
        let inventory: Option<(Value, Value, Value, String, Option<String>)> =
            if let Some(resource_id) = serving_resource_id.or(resource_id) {
                sqlx::query_as(
                    "SELECT entries, selectors, record_version_boundary,
                            support_status, unsupported_reason
                     FROM record_inventory_current
                     WHERE resource_id = $1::uuid
                     ORDER BY inserted_at DESC LIMIT 1",
                )
                .bind(resource_id)
                .fetch_optional(&self.pool)
                .await?
            } else {
                None
            };
        let (entries, selectors, boundary, support_status, unsupported_reason) = inventory
            .unwrap_or_else(|| {
                (
                    serde_json::json!([]),
                    serde_json::json!([]),
                    serde_json::json!({}),
                    "unsupported".to_owned(),
                    Some("name_has_no_current_resource".to_owned()),
                )
            });

        let mut coin_addresses = serde_json::Map::new();
        let mut text_records = serde_json::Map::new();
        let mut content_hash = serde_json::json!({"status":"not_found"});
        let mut name_record = serde_json::json!({"status":"not_found"});
        for entry in entries.as_array().into_iter().flatten() {
            let Some(record_key) = entry.get("record_key").and_then(Value::as_str) else {
                continue;
            };
            let value = entry.clone();
            if let Some(coin_type) = record_key.strip_prefix("addr:") {
                coin_addresses.insert(coin_type.to_owned(), value);
            } else if let Some(text_key) = record_key.strip_prefix("text:") {
                text_records.insert(text_key.to_owned(), value);
            } else if record_key == "contenthash" {
                content_hash = value;
            } else if record_key == "name" {
                name_record = value;
            }
        }
        let query = query_parameters(path);
        for coin_type in comma_values(query.get("coin_types").copied()) {
            coin_addresses
                .entry(coin_type.to_owned())
                .or_insert_with(|| serde_json::json!({"status":"not_found"}));
        }
        for text_key in comma_values(query.get("texts").copied()) {
            text_records
                .entry(text_key.to_owned())
                .or_insert_with(|| serde_json::json!({"status":"not_found"}));
        }
        let known_text_keys = text_records.keys().cloned().collect::<Vec<_>>();
        Ok((
            reqwest::StatusCode::OK,
            serde_json::json!({
                "data": {
                    "resolver_address": resolver_address,
                    "coin_addresses": coin_addresses,
                    "text_records": text_records,
                    "content_hash": content_hash,
                    "name": name_record,
                    "known_text_keys": {
                        "keys": known_text_keys,
                        "status": support_status,
                    },
                },
                "declared_state": {
                    "record_inventory": {
                        "entries": entries,
                        "selectors": selectors,
                        "record_version_boundary": boundary,
                        "support_status": support_status,
                        "unsupported_reason": unsupported_reason,
                    }
                }
            }),
        ))
    }

    async fn children_projection(
        &self,
        namespace: &str,
        parent: &str,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        if parent.contains('[') || parent.contains(']') {
            return Ok((
                reqwest::StatusCode::BAD_REQUEST,
                serde_json::json!({"error":{"code":"invalid_input"}}),
            ));
        }
        let parent_id: Option<String> = sqlx::query_scalar(
            "SELECT logical_name_id FROM name_current
             WHERE namespace = $1 AND raw_name = $2",
        )
        .bind(namespace)
        .bind(parent)
        .fetch_optional(&self.pool)
        .await?;
        let Some(parent_id) = parent_id else {
            return Ok((
                reqwest::StatusCode::NOT_FOUND,
                serde_json::json!({"error":{"code":"not_found"}}),
            ));
        };
        let rows: Vec<ChildProjectionRow> = sqlx::query_as(
            "SELECT child_logical_name_id, decoded_name, decoded_label,
                    namehash, labelhash, owner, registrant, provenance,
                    chain_positions, canonicality_summary
             FROM children_current
             WHERE parent_logical_name_id = $1
             ORDER BY decoded_name NULLS LAST, child_logical_name_id",
        )
        .bind(parent_id)
        .fetch_all(&self.pool)
        .await?;
        let data = rows
            .into_iter()
            .map(
                |(
                    logical_name_id,
                    decoded_name,
                    decoded_label,
                    namehash,
                    labelhash,
                    owner,
                    registrant,
                    provenance,
                    chain_positions,
                    canonicality_summary,
                )| {
                    let normalized_name =
                        decoded_name.unwrap_or_else(|| format!("[{labelhash}].{parent}"));
                    serde_json::json!({
                        "logical_name_id": logical_name_id,
                        "normalized_name": normalized_name,
                        "label": decoded_label,
                        "namehash": namehash,
                        "labelhash": labelhash,
                        "owner": owner,
                        "registrant": registrant,
                        "provenance": provenance,
                        "chain_positions": chain_positions,
                        "canonicality_summary": canonicality_summary,
                    })
                },
            )
            .collect::<Vec<_>>();
        Ok((reqwest::StatusCode::OK, serde_json::json!({"data": data})))
    }

    async fn address_names_projection(
        &self,
        address: &str,
        path: &str,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let query = query_parameters(path);
        let namespace = query.get("namespace").copied();
        let relation = query.get("relation").copied();
        let rows: Vec<AddressNameProjectionRow> = sqlx::query_as(
            "SELECT logical_name_id, relation, namespace, raw_name, namehash,
                    resource_id::text, token_lineage_id::text, binding_kind,
                    support_status, unsupported_reason, provenance,
                    chain_positions, canonicality_summary
             FROM address_names_current
             WHERE lower(address) = lower($1)
               AND ($2::text IS NULL OR namespace = $2)
               AND ($3::text IS NULL OR relation = $3)
             ORDER BY raw_name, relation",
        )
        .bind(address)
        .bind(namespace)
        .bind(relation)
        .fetch_all(&self.pool)
        .await?;
        let data = rows
            .into_iter()
            .map(
                |(
                    logical_name_id,
                    relation,
                    namespace,
                    raw_name,
                    namehash,
                    resource_id,
                    token_lineage_id,
                    binding_kind,
                    support_status,
                    unsupported_reason,
                    provenance,
                    chain_positions,
                    canonicality_summary,
                )| {
                    serde_json::json!({
                        "logical_name_id": logical_name_id,
                        "normalized_name": raw_name,
                        "namespace": namespace,
                        "namehash": namehash,
                        "resource_id": resource_id,
                        "token_lineage_id": token_lineage_id,
                        "binding_kind": binding_kind,
                        "relation": relation,
                        "relation_facets": [relation],
                        "support_status": support_status,
                        "unsupported_reason": unsupported_reason,
                        "provenance": provenance,
                        "chain_positions": chain_positions,
                        "canonicality_summary": canonicality_summary,
                    })
                },
            )
            .collect::<Vec<_>>();
        Ok((reqwest::StatusCode::OK, serde_json::json!({"data": data})))
    }

    async fn primary_name_projection(
        &self,
        address: &str,
        path: &str,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let query = query_parameters(path);
        let namespace = query.get("namespace").copied().unwrap_or("ens");
        let coin_type = query.get("coin_type").copied().unwrap_or("60");
        let mode = query.get("mode").copied().unwrap_or("declared");
        anyhow::ensure!(
            mode == "declared",
            "ProjectionReader exposes only persisted declared primary-name state; mode={mode} requires API lookup coverage"
        );
        let row: Option<PrimaryNameProjectionRow> = sqlx::query_as(
            "SELECT claim_status, raw_claim_name, claim_name_is_normalized,
                    unsupported_reason, claim_provenance
             FROM primary_names_current
             WHERE lower(address) = lower($1) AND namespace = $2 AND coin_type = $3",
        )
        .bind(address)
        .bind(namespace)
        .bind(coin_type)
        .fetch_optional(&self.pool)
        .await?;
        let (status, raw_name, normalized, unsupported_reason, provenance) =
            row.unwrap_or_else(|| {
                (
                    "not_found".to_owned(),
                    None,
                    false,
                    None,
                    serde_json::json!({}),
                )
            });
        let mut claimed = serde_json::json!({
            "status": status,
            "provenance": provenance,
        });
        if status == "success" {
            claimed["name"] = raw_name.clone().map(Value::String).unwrap_or(Value::Null);
        } else if status == "invalid_name" {
            claimed["raw_claim_name"] = raw_name.clone().map(Value::String).unwrap_or(Value::Null);
        }
        if let Some(reason) = unsupported_reason {
            claimed["unsupported_reason"] = Value::String(reason);
        }
        claimed["claim_name_is_normalized"] = Value::Bool(normalized);
        Ok((
            reqwest::StatusCode::OK,
            serde_json::json!({
                "declared_state": {"claimed_primary_name": claimed},
            }),
        ))
    }

    async fn manifest_projection(
        &self,
        namespace: &str,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let prefix = if namespace == "basenames" {
            "basenames%"
        } else {
            "ens%"
        };
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT source_family, chain_id, manifest_version
             FROM manifest_versions WHERE source_family LIKE $1
             ORDER BY source_family, manifest_version",
        )
        .bind(prefix)
        .fetch_all(&self.pool)
        .await?;
        let manifests = rows
            .into_iter()
            .map(|(source_family, chain, version)| {
                serde_json::json!({
                    "source_family":source_family,
                    "chain":chain,
                    "version":version,
                })
            })
            .collect::<Vec<_>>();
        Ok((
            reqwest::StatusCode::OK,
            serde_json::json!({"declared_state":{"manifests":manifests}}),
        ))
    }
}

fn decode_path_segment(segment: &str) -> &str {
    // Only bracket escapes occur in these scenario paths. Returning the
    // original lets the caller reject encoded placeholder names below.
    segment
}

fn query_parameters(path: &str) -> std::collections::BTreeMap<&str, &str> {
    path.split_once('?')
        .map(|(_, query)| {
            query
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .collect()
        })
        .unwrap_or_default()
}

fn comma_values(value: Option<&str>) -> impl Iterator<Item = &str> {
    value.into_iter().flat_map(|value| value.split(','))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::fs::MetadataExt;

    use super::*;

    #[test]
    fn timeout_configuration_requires_positive_integer_seconds() {
        for variable in [
            "BIGNAME_E2E_READY_TIMEOUT_SECS",
            "BIGNAME_E2E_COMMAND_TIMEOUT_SECS",
        ] {
            assert_eq!(parse_timeout_secs(variable, "17").unwrap(), 17);
            for invalid in ["0", "-1", "1.5", "not-a-number"] {
                let error = parse_timeout_secs(variable, invalid)
                    .expect_err("invalid timeout must fail explicitly");
                assert!(format!("{error:#}").contains(variable), "{error:#}");
            }
        }
    }

    #[test]
    fn process_log_files_are_unique_for_repeated_labels() -> Result<()> {
        let (first_path, first_file) = create_process_log_file("runner", "same/label")?;
        let (second_path, second_file) = create_process_log_file("runner", "same/label")?;
        drop(first_file);
        drop(second_file);

        assert_ne!(first_path, second_path);
        assert!(
            first_path
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.contains("same-label"))
        );
        std::fs::remove_file(first_path).ok();
        std::fs::remove_file(second_path).ok();
        Ok(())
    }

    #[test]
    fn profile_runner_binary_and_build_lock_have_scoped_lifetimes() -> Result<()> {
        let (source_path, file) =
            create_process_log_file("phase-runner-hard-link-source", "drop-test")?;
        drop(file);
        let lock_path = source_path.with_extension("profile-build.lock");
        let first_lock = ProfileBuildLock::acquire(&lock_path)?;
        assert!(
            ProfileBuildLock::try_acquire(&lock_path)?.is_none(),
            "a second e2e process must not enter the shared Cargo build/link window"
        );
        let waiter_path = lock_path.clone();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::sync_channel(1);
        let waiter = std::thread::spawn(move || {
            let acquired = ProfileBuildLock::acquire(&waiter_path);
            acquired_tx.send(acquired).ok();
        });
        drop(first_lock);
        let second_lock = acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .context("the deployment-profile build lock must release with its scope")??;
        drop(second_lock);
        waiter
            .join()
            .map_err(|_| anyhow::anyhow!("deployment-profile build-lock waiter panicked"))?;
        let linked_path = hard_link_profile_binary(&source_path)?;
        let source_metadata = std::fs::metadata(&source_path)?;
        let linked_metadata = std::fs::metadata(&linked_path)?;
        let shares_inode = source_metadata.dev() == linked_metadata.dev()
            && source_metadata.ino() == linked_metadata.ino();
        let runner = ProfileRunnerBinary::new(linked_path.clone());

        drop(runner);

        let removed = !linked_path.exists();
        let source_retained = source_path.exists();
        std::fs::remove_file(&linked_path).ok();
        std::fs::remove_file(&lock_path).ok();
        std::fs::remove_file(&source_path).ok();
        assert!(
            shares_inode,
            "deployment-profile runner must be a hard link, not a copy"
        );
        assert!(
            removed,
            "dropping a cached deployment-profile runner must remove its temporary executable"
        );
        assert!(
            source_retained,
            "dropping a deployment-profile runner must not remove Cargo's source executable"
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_shot_command_deadline_stops_and_reaps_the_child() -> Result<()> {
        let label = "unit-one-shot-timeout";
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf 'deliberate-timeout-stdout\\n'; printf 'deliberate-timeout-stderr\\n' >&2; exec sleep 30",
        ]);

        let error = run_to_completion_with_timeout(command, label, 1)
            .await
            .expect_err("a long-running one-shot command must time out");
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "exceeded the configured BIGNAME_E2E_COMMAND_TIMEOUT_SECS deadline of 1s"
            ),
            "{message}"
        );
        assert!(message.contains("stopped and reaped"), "{message}");
        assert!(message.contains("deliberate-timeout-stdout"), "{message}");
        assert!(message.contains("deliberate-timeout-stderr"), "{message}");

        let pid = std::process::id().to_string();
        for entry in std::fs::read_dir(std::env::temp_dir())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("bigname-e2e-command-")
                && name.contains(&pid)
                && name.contains(label)
            {
                std::fs::remove_file(entry.path()).ok();
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_shot_command_deadline_terminates_descendants() -> Result<()> {
        let label = "unit-one-shot-descendant-timeout";
        let (pid_path, pid_file) = create_process_log_file("descendant-pid", label)?;
        drop(pid_file);

        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 30 & child=$!; printf '%s\\n' \"$child\" > \"$1\"; wait")
            .arg("timeout-tree")
            .arg(&pid_path);

        let error = run_to_completion_with_timeout(command, label, 1)
            .await
            .expect_err("a command with a live descendant must time out");
        let message = format!("{error:#}");
        assert!(message.contains("process group"), "{message}");

        let descendant_pid = std::fs::read_to_string(&pid_path)?
            .trim()
            .parse::<u32>()
            .context("parse descendant pid")?;
        let mut descendant_exists = unix_process::process_exists(descendant_pid)?;
        for _ in 0..100 {
            if !descendant_exists {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            descendant_exists = unix_process::process_exists(descendant_pid)?;
        }
        if descendant_exists {
            unix_process::kill_process(descendant_pid).ok();
        }
        assert!(
            !descendant_exists,
            "descendant process {descendant_pid} survived the command timeout"
        );

        std::fs::remove_file(pid_path).ok();
        let harness_pid = std::process::id().to_string();
        for entry in std::fs::read_dir(std::env::temp_dir())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("bigname-e2e-command-")
                && name.contains(&harness_pid)
                && name.contains(label)
            {
                std::fs::remove_file(entry.path()).ok();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn expired_readiness_deadline_bounds_a_pending_probe() {
        let expired = tokio::time::Instant::now() - Duration::from_millis(1);
        let error = await_with_readiness_deadline(
            expired,
            17,
            "deliberately pending readiness probe",
            std::future::pending::<()>(),
        )
        .await
        .expect_err("a readiness probe must not outlive its deadline");
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "deliberately pending readiness probe exceeded the configured 17s readiness deadline"
            ),
            "{message}"
        );
    }

    #[tokio::test]
    async fn supervised_readiness_timeout_stops_and_reaps_child() -> Result<()> {
        let (log_path, log_file) =
            create_process_log_file("readiness-child", "unit-readiness-timeout")?;
        let mut child = Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;
        let expired = tokio::time::Instant::now() - Duration::from_millis(1);

        let error = await_supervised_readiness(
            &mut child,
            &log_path,
            "test readiness child",
            expired,
            23,
            "deliberately pending SQL readiness probe",
            std::future::pending::<Result<()>>(),
        )
        .await
        .expect_err("a timed-out readiness probe must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "deliberately pending SQL readiness probe exceeded the configured 23s readiness deadline"
            ),
            "{message}"
        );
        assert!(message.contains("stopped and reaped"), "{message}");
        assert!(child.id().is_none(), "the readiness child was not reaped");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stop_reports_a_child_that_crashed_after_readiness() -> Result<()> {
        let log_path =
            std::env::temp_dir().join(format!("bigname-e2e-stop-crash-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let child = Command::new("sh")
            .args(["-c", "echo deliberate-child-crash >&2; exit 17"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let error = stop_supervised_child(child, "test child", &log_path)
            .await
            .expect_err("an already-crashed child must make stop fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited before requested stop"),
            "{message}"
        );
        assert!(message.contains("deliberate-child-crash"), "{message}");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn stop_reports_a_child_that_crashes_between_status_check_and_kill() -> Result<()> {
        let log_path =
            std::env::temp_dir().join(format!("bigname-e2e-stop-race-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let child = Command::new("sh")
            .args([
                "-c",
                "read -r _; echo deliberate-stop-race-crash >&2; exit 17",
            ])
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;

        let error = stop_supervised_child_with_pre_kill_action(
            child,
            "test child",
            &log_path,
            Some(PreKillAction::CloseStdinAndAwaitExitedUnreaped),
        )
        .await
        .expect_err("a child crash racing with stop must not be accepted as a requested kill");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited independently while stop was requested"),
            "{message}"
        );
        assert!(message.contains("deliberate-stop-race-crash"), "{message}");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }
}
