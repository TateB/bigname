use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use alloy_primitives::{Address, hex};
use anyhow::{Context, Result, bail};

use crate::model::RawSourceManifest;
use crate::{LoadedManifest, ManifestAbi, ManifestLoadStatus, ManifestLoadSummary};
use crate::{ManifestRepository, SourceManifest};
pub fn load_repository(root: impl AsRef<Path>) -> Result<ManifestRepository> {
    let root = root.as_ref();
    let display_root = canonicalize_for_logging(root);

    if !root.exists() {
        return Ok(ManifestRepository {
            root: display_root.clone(),
            manifests: Vec::new(),
            summary: ManifestLoadSummary {
                root: display_root,
                status: ManifestLoadStatus::MissingRoot,
                namespace_count: 0,
                source_family_count: 0,
                manifest_count: 0,
            },
        });
    }

    if !root.is_dir() {
        return Ok(ManifestRepository {
            root: display_root.clone(),
            manifests: Vec::new(),
            summary: ManifestLoadSummary {
                root: display_root,
                status: ManifestLoadStatus::InvalidRoot,
                namespace_count: 0,
                source_family_count: 0,
                manifest_count: 0,
            },
        });
    }

    let mut manifest_paths = Vec::new();
    collect_manifest_paths(root, &mut manifest_paths)
        .with_context(|| format!("failed to read manifests root {}", root.display()))?;

    let mut manifests = Vec::new();
    let mut namespaces = BTreeSet::new();
    let mut source_families = BTreeSet::new();

    for path in manifest_paths {
        let loaded_manifest = load_manifest_file(root, &path)?;
        namespaces.insert(loaded_manifest.manifest.namespace.clone());
        source_families.insert((
            loaded_manifest.manifest.namespace.clone(),
            loaded_manifest.manifest.source_family.clone(),
        ));
        manifests.push(loaded_manifest);
    }

    let manifest_count = manifests.len();
    let status = if manifests.is_empty() {
        ManifestLoadStatus::Empty
    } else {
        ManifestLoadStatus::Loaded
    };

    Ok(ManifestRepository {
        root: display_root.clone(),
        manifests,
        summary: ManifestLoadSummary {
            root: display_root,
            status,
            namespace_count: namespaces.len(),
            source_family_count: source_families.len(),
            manifest_count,
        },
    })
}

fn load_manifest_file(root: &Path, path: &Path) -> Result<LoadedManifest> {
    let relative_path = path
        .strip_prefix(root)
        .with_context(|| {
            format!(
                "manifest path {} is not under repository root {}",
                path.display(),
                root.display()
            )
        })?
        .to_path_buf();
    let version_tag = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("manifest path {} is missing a file stem", path.display()))?;
    let raw_manifest = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest file {}", path.display()))?;
    let manifest: SourceManifest = toml::from_str::<RawSourceManifest>(&raw_manifest)
        .with_context(|| format!("failed to parse manifest TOML {}", path.display()))?
        .into();

    validate_manifest_metadata(&manifest, path, &relative_path)?;

    Ok(LoadedManifest {
        path: path.to_path_buf(),
        relative_path,
        version_tag,
        manifest,
    })
}

fn validate_manifest_metadata(
    manifest: &SourceManifest,
    path: &Path,
    relative_path: &Path,
) -> Result<()> {
    let parts = relative_path
        .iter()
        .map(|part| {
            part.to_str().with_context(|| {
                format!(
                    "manifest path {} contains non-UTF-8 path component",
                    path.display()
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let (chain_combo_name, namespace_name, source_family_name) = match parts.as_slice() {
        [namespace_name, source_family_name, _version] => {
            (None, *namespace_name, *source_family_name)
        }
        [
            chain_combo_name,
            namespace_name,
            source_family_name,
            _version,
        ] => (
            Some(*chain_combo_name),
            *namespace_name,
            *source_family_name,
        ),
        _ => {
            bail!(
                "manifest path {} must match <namespace>/<source_family>/<version>.toml or <chain_combo>/<namespace>/<source_family>/<version>.toml under the selected manifest root",
                path.display()
            );
        }
    };

    if let Some(chain_combo_name) = chain_combo_name
        && manifest_chain_combo(&manifest.chain) != chain_combo_name
    {
        bail!(
            "manifest chain {} does not match chain directory {} for {}",
            manifest.chain,
            chain_combo_name,
            path.display()
        );
    }

    if manifest.namespace != namespace_name {
        bail!(
            "manifest namespace {} does not match directory {} for {}",
            manifest.namespace,
            namespace_name,
            path.display()
        );
    }

    if manifest.source_family != source_family_name {
        bail!(
            "manifest source_family {} does not match directory {} for {}",
            manifest.source_family,
            source_family_name,
            path.display()
        );
    }

    for root in &manifest.roots {
        validate_start_block_fits_i64(root.start_block, "root", &root.name, path)?;
    }

    for contract in &manifest.contracts {
        validate_start_block_fits_i64(contract.start_block, "contract", &contract.role, path)?;
    }

    validate_manifest_abi(manifest, path)?;

    Ok(())
}

fn manifest_chain_combo(chain: &str) -> &str {
    chain
        .split_once('-')
        .map_or(chain, |(chain_combo, _)| chain_combo)
}

fn validate_start_block_fits_i64(
    start_block: Option<u64>,
    declaration_kind: &str,
    declaration_name: &str,
    path: &Path,
) -> Result<()> {
    if let Some(start_block) = start_block
        && i64::try_from(start_block).is_err()
    {
        bail!(
            "manifest {declaration_kind} {declaration_name} in {} has start_block {start_block} that does not fit into BIGINT",
            path.display()
        );
    }

    Ok(())
}

fn validate_manifest_abi(manifest: &SourceManifest, path: &Path) -> Result<()> {
    validate_manifest_abi_fragments(&manifest.abi, path)?;

    let contract_roles = manifest
        .contracts
        .iter()
        .map(|contract| contract.role.as_str())
        .collect::<BTreeSet<_>>();

    for event in &manifest.abi.events {
        for role in &event.emitter_roles {
            if !contract_roles.contains(role.as_str()) {
                bail!(
                    "manifest ABI event {} in {} references unknown emitter role {}",
                    event.name,
                    path.display(),
                    role
                );
            }
        }
    }

    for call in &manifest.abi.calls {
        for role in &call.target_roles {
            if !contract_roles.contains(role.as_str()) {
                bail!(
                    "manifest ABI call {} in {} references unknown target role {}",
                    call.name,
                    path.display(),
                    role
                );
            }
        }
    }

    Ok(())
}

fn validate_manifest_abi_fragments(abi: &ManifestAbi, path: &Path) -> Result<()> {
    let mut event_signatures = BTreeSet::new();
    for event in &abi.events {
        let fragment = event.fragment.trim();
        if !fragment.starts_with("event ") {
            bail!(
                "manifest ABI event {} in {} must use an event fragment",
                event.name,
                path.display()
            );
        }
        let parsed = event.parsed_event_view().with_context(|| {
            format!(
                "manifest ABI event {} in {} has invalid fragment",
                event.name,
                path.display()
            )
        })?;
        let signature = parsed.canonical_signature();
        if !event_signatures.insert(signature.clone()) {
            bail!(
                "manifest ABI event {} in {} duplicates event signature {}",
                event.name,
                path.display(),
                signature
            );
        }
    }

    let mut call_signatures = BTreeSet::new();
    for call in &abi.calls {
        let fragment = call.fragment.trim();
        if !fragment.starts_with("function ") {
            bail!(
                "manifest ABI call {} in {} must use a function fragment",
                call.name,
                path.display()
            );
        }
        let parsed = call.parsed_function_view().with_context(|| {
            format!(
                "manifest ABI call {} in {} has invalid fragment",
                call.name,
                path.display()
            )
        })?;
        let signature = parsed.canonical_signature();
        if !call_signatures.insert(signature.clone()) {
            bail!(
                "manifest ABI call {} in {} duplicates function signature {}",
                call.name,
                path.display(),
                signature
            );
        }
    }

    Ok(())
}

fn read_dir_sorted(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to iterate directory {}", path.display()))?;
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn collect_manifest_paths(directory: &Path, manifests: &mut Vec<PathBuf>) -> Result<()> {
    for entry in read_dir_sorted(directory)? {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect {}", path.display()))?;

        if file_type.is_dir() {
            collect_manifest_paths(&path, manifests)
                .with_context(|| format!("failed to read manifest directory {}", path.display()))?;
        } else if file_type.is_file()
            && path.extension().and_then(|part| part.to_str()) == Some("toml")
        {
            manifests.push(path);
        }
    }

    Ok(())
}

fn canonicalize_for_logging(root: &Path) -> PathBuf {
    fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

pub(crate) fn normalize_address(value: &str) -> String {
    normalize_alloy_address(value).unwrap_or_else(|| value.to_ascii_lowercase())
}

fn normalize_alloy_address(value: &str) -> Option<String> {
    if value.len() != 42 || (!value.starts_with("0x") && !value.starts_with("0X")) {
        return None;
    }

    let address = value.parse::<Address>().ok()?;
    Some(format_prefixed_hex(address.as_slice()))
}

fn format_prefixed_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}
