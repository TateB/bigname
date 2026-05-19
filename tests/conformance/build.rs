use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let source_path = manifest_dir.join("../../apps/api/src/main.rs");
    let source = inline_local_includes(&source_path);
    let rewritten = rewrite_openapi_components(&strip_crate_recursion_limit(&source))
        .replace("crate::", "crate::shipped_api::")
        .replace("\n#[cfg(test)]\nmod tests;\n", "\n");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("out dir"));
    let out_path = out_dir.join("api_main.rs");

    fs::write(&out_path, rewritten)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", out_path.display()));

    let docs_source_path = manifest_dir.join("../../apps/api/src/openapi/docs.html");
    let docs_out_path = out_dir.join("docs.html");
    println!("cargo:rerun-if-changed={}", docs_source_path.display());
    fs::copy(&docs_source_path, &docs_out_path).unwrap_or_else(|error| {
        panic!(
            "failed to copy {} to {}: {error}",
            docs_source_path.display(),
            docs_out_path.display()
        )
    });
}

fn inline_local_includes(source_path: &Path) -> String {
    println!("cargo:rerun-if-changed={}", source_path.display());

    let source = fs::read_to_string(source_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", source_path.display()));
    let mut rewritten = String::with_capacity(source.len());
    let lines: Vec<&str> = source.split_inclusive('\n').collect();
    let mut index = 0;
    let mut previous_cfg_test = false;

    while index < lines.len() {
        let line = lines[index];
        let trimmed = line.trim();
        if let Some(include_path) = parse_include_path(trimmed) {
            let resolved = source_path
                .parent()
                .unwrap_or_else(|| panic!("{} has no parent directory", source_path.display()))
                .join(include_path);
            rewritten.push_str(&inline_local_includes(&resolved));
            if !rewritten.ends_with('\n') {
                rewritten.push('\n');
            }
            previous_cfg_test = false;
            index += 1;
            continue;
        }

        if let Some((resolved, consumed)) =
            parse_concat_manifest_include(&lines[index..], source_path)
        {
            rewritten.push_str(&inline_local_includes(&resolved));
            if !rewritten.ends_with('\n') {
                rewritten.push('\n');
            }
            previous_cfg_test = false;
            index += consumed;
            continue;
        }

        if let Some(path_attr) = parse_path_attr(trimmed) {
            if let Some(next_line) = lines.get(index + 1) {
                if let Some((visibility, module_name)) = parse_mod_decl(next_line.trim()) {
                    let resolved = source_path
                        .parent()
                        .unwrap_or_else(|| {
                            panic!("{} has no parent directory", source_path.display())
                        })
                        .join(path_attr);
                    push_inlined_module(&mut rewritten, visibility, module_name, &resolved);
                    previous_cfg_test = false;
                    index += 2;
                    continue;
                }
            }
        }

        if !previous_cfg_test {
            if let Some((visibility, module_name)) = parse_mod_decl(trimmed) {
                if let Some(resolved) = resolve_module_path(source_path, module_name) {
                    push_inlined_module(&mut rewritten, visibility, module_name, &resolved);
                    previous_cfg_test = false;
                    index += 1;
                    continue;
                }
            }
        }

        rewritten.push_str(line);
        previous_cfg_test = trimmed == "#[cfg(test)]";
        index += 1;
    }

    rewritten
}

fn parse_include_path(line: &str) -> Option<&str> {
    line.strip_prefix("include!(\"")?.strip_suffix("\");")
}

fn parse_concat_manifest_include(lines: &[&str], source_path: &Path) -> Option<(PathBuf, usize)> {
    if lines.len() < 4 || lines[0].trim() != "include!(concat!(" {
        return None;
    }
    if lines[1].trim() != r#"env!("CARGO_MANIFEST_DIR"),"# || lines[3].trim() != "));" {
        return None;
    }

    let manifest_path = lines[2]
        .trim()
        .strip_prefix('"')?
        .strip_suffix("\",")
        .or_else(|| lines[2].trim().strip_prefix('"')?.strip_suffix('"'))?;
    Some((resolve_manifest_include_path(source_path, manifest_path), 4))
}

fn resolve_manifest_include_path(source_path: &Path, manifest_path: &str) -> PathBuf {
    let mut current = source_path
        .parent()
        .unwrap_or_else(|| panic!("{} has no parent directory", source_path.display()));

    loop {
        if current.file_name().is_some_and(|name| name == "src") {
            let manifest_dir = current
                .parent()
                .unwrap_or_else(|| panic!("{} has no parent directory", current.display()));
            return manifest_dir.join(manifest_path.trim_start_matches('/'));
        }

        current = current
            .parent()
            .unwrap_or_else(|| panic!("failed to find src ancestor for {}", source_path.display()));
    }
}

fn parse_path_attr(line: &str) -> Option<&str> {
    line.strip_prefix("#[path = \"")?.strip_suffix("\"]")
}

fn parse_mod_decl(line: &str) -> Option<(&str, &str)> {
    let declaration = line.strip_suffix(';')?;
    for prefix in ["pub(super) mod ", "pub(crate) mod ", "pub mod ", "mod "] {
        if let Some(module_name) = declaration.strip_prefix(prefix) {
            let visibility = prefix.strip_suffix("mod ").unwrap_or(prefix).trim_end();
            return Some((visibility, module_name));
        }
    }
    None
}

fn resolve_module_path(source_path: &Path, module_name: &str) -> Option<PathBuf> {
    let parent = source_path.parent()?;
    let file_path = parent.join(format!("{module_name}.rs"));
    if file_path.exists() {
        return Some(file_path);
    }

    let mod_path = parent.join(module_name).join("mod.rs");
    mod_path.exists().then_some(mod_path)
}

fn push_inlined_module(
    rewritten: &mut String,
    visibility: &str,
    module_name: &str,
    source_path: &Path,
) {
    if !visibility.is_empty() {
        rewritten.push_str(visibility);
        rewritten.push(' ');
    }
    rewritten.push_str("mod ");
    rewritten.push_str(module_name);
    rewritten.push_str(" {\n");
    rewritten.push_str(&inline_local_includes(source_path));
    if !rewritten.ends_with('\n') {
        rewritten.push('\n');
    }
    rewritten.push_str("}\n");
}

fn strip_crate_recursion_limit(source: &str) -> String {
    let mut removed = false;
    let mut rewritten = String::with_capacity(source.len());

    for line in source.split_inclusive('\n') {
        if !removed && line.trim_end() == "#![recursion_limit = \"256\"]" {
            removed = true;
            continue;
        }

        rewritten.push_str(line);
    }

    if !removed && source == "#![recursion_limit = \"256\"]" {
        return String::new();
    }

    rewritten
}

fn rewrite_openapi_components(source: &str) -> String {
    const START: &str = "fn openapi_components() -> JsonValue {";
    const CURRENT_END: &str = "\npub(super) fn schema_ref(";
    const LEGACY_END: &str = "\nfn declared_response_schema(";
    const LEGACY_VISIBLE_END: &str = "\npub(super) fn declared_response_schema(";
    const SPLIT_SCHEMA_END: &str = "\nfn primary_name_claimed_result_schema(";
    const SPLIT_SCHEMA_VISIBLE_END: &str = "\npub(super) fn primary_name_claimed_result_schema(";
    const REPLACEMENT: &str = r###"fn openapi_components() -> JsonValue {
    let mut schemas = serde_json::Map::new();
    schemas.insert("JsonObject".to_owned(), json_object_schema());
    schemas.insert("NullValue".to_owned(), json!({ "type": "null" }));
    schemas.insert(
        "Consistency".to_owned(),
        json!({
            "type": "string",
            "enum": ["head", "safe", "finalized"],
        }),
    );
    schemas.insert(
        "Provenance".to_owned(),
        json!({
            "type": "object",
            "required": [
                "normalized_event_ids",
                "raw_fact_refs",
                "manifest_versions",
                "execution_trace_id",
                "derivation_kind",
            ],
            "properties": {
                "normalized_event_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                },
                "raw_fact_refs": {
                    "type": "array",
                    "items": {},
                },
                "manifest_versions": {
                    "type": "array",
                    "items": {},
                },
                "execution_trace_id": {
                    "type": ["string", "null"],
                },
                "derivation_kind": {
                    "type": "string",
                },
            },
        }),
    );
    schemas.insert(
        "CoverageResponse".to_owned(),
        json!({
            "type": "object",
            "required": [
                "status",
                "exhaustiveness",
                "source_classes_considered",
                "enumeration_basis",
                "unsupported_reason",
            ],
            "properties": {
                "status": { "type": "string" },
                "exhaustiveness": { "type": "string" },
                "source_classes_considered": {
                    "type": "array",
                    "items": { "type": "string" },
                },
                "enumeration_basis": { "type": "string" },
                "unsupported_reason": {
                    "type": ["string", "null"],
                },
            },
        }),
    );
    schemas.insert(
        "ChainPositionResponse".to_owned(),
        json!({
            "type": "object",
            "required": ["chain_id", "block_number", "block_hash", "timestamp"],
            "properties": {
                "chain_id": { "type": "string" },
                "block_number": { "type": "integer" },
                "block_hash": { "type": "string" },
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                },
            },
        }),
    );
    schemas.insert(
        "ChainPositions".to_owned(),
        json!({
            "type": "object",
            "additionalProperties": schema_ref("ChainPositionResponse"),
        }),
    );
    schemas.insert(
        "HistoryPageResponse".to_owned(),
        json!({
            "type": "object",
            "required": ["cursor", "next_cursor", "page_size", "sort"],
            "properties": {
                "cursor": { "type": ["string", "null"] },
                "next_cursor": { "type": ["string", "null"] },
                "page_size": {
                    "type": "integer",
                    "minimum": 0,
                },
                "sort": { "type": "string" },
            },
        }),
    );
    schemas.insert(
        "ExactNameData".to_owned(),
        json!({
            "type": "object",
            "required": [
                "logical_name_id",
                "namespace",
                "normalized_name",
                "canonical_display_name",
                "namehash",
                "resource_id",
                "token_lineage_id",
                "binding_kind",
            ],
            "properties": {
                "logical_name_id": { "type": "string" },
                "namespace": { "type": "string" },
                "normalized_name": { "type": "string" },
                "canonical_display_name": { "type": "string" },
                "namehash": { "type": "string" },
                "resource_id": {
                    "type": ["string", "null"],
                    "format": "uuid",
                },
                "token_lineage_id": {
                    "type": ["string", "null"],
                    "format": "uuid",
                },
                "binding_kind": {
                    "type": ["string", "null"],
                },
            },
        }),
    );
    schemas.insert(
        "ResolverData".to_owned(),
        json!({
            "type": "object",
            "required": ["chain_id", "resolver_address"],
            "properties": {
                "chain_id": { "type": "string" },
                "resolver_address": { "type": "string" },
            },
        }),
    );
    schemas.insert(
        "PrimaryNameData".to_owned(),
        json!({
            "type": "object",
            "required": ["address", "namespace", "coin_type"],
            "properties": {
                "address": { "type": "string" },
                "namespace": {
                    "type": "string",
                    "enum": PUBLIC_NAMESPACES,
                },
                "coin_type": { "type": "string" },
            },
        }),
    );
    schemas.insert(
        "PrimaryNameClaimedResult".to_owned(),
        primary_name_claimed_result_schema(),
    );
    schemas.insert(
        "PrimaryNameDeclaredState".to_owned(),
        json!({
            "type": "object",
            "required": ["claimed_primary_name"],
            "properties": {
                "claimed_primary_name": schema_ref("PrimaryNameClaimedResult"),
            },
            "additionalProperties": false,
        }),
    );
    schemas.insert(
        "PrimaryNameVerifiedState".to_owned(),
        json!({
            "type": "object",
            "required": ["verified_primary_name"],
            "properties": {
                "verified_primary_name": schema_ref("PrimaryNameVerifiedResult"),
            },
            "additionalProperties": false,
        }),
    );
    schemas.insert(
        "PrimaryNameVerifiedResult".to_owned(),
        primary_name_verified_result_schema(),
    );
    schemas.insert(
        "PrimaryNameVerifiedResultProvenance".to_owned(),
        primary_name_verified_result_provenance_schema(),
    );
    schemas.insert(
        "ExactNameResponse".to_owned(),
        declared_response_schema(schema_ref("ExactNameData"), schema_ref("JsonObject")),
    );
    schemas.insert(
        "ResolverResponse".to_owned(),
        declared_response_schema(schema_ref("ResolverData"), schema_ref("JsonObject")),
    );
    schemas.insert(
        "ResolutionResponse".to_owned(),
        mixed_response_schema(schema_ref("ExactNameData")),
    );
    schemas.insert("PrimaryNameResponse".to_owned(), primary_name_response_schema());
    schemas.insert(
        "CollectionResponse".to_owned(),
        paginated_declared_response_schema(
            json!({
                "type": "array",
                "items": schema_ref("JsonObject"),
            }),
            schema_ref("JsonObject"),
        ),
    );
    schemas.insert(
        "NamespaceData".to_owned(),
        json!({
            "type": "object",
            "required": ["namespace"],
            "properties": {
                "namespace": {
                    "type": "string",
                    "enum": PUBLIC_NAMESPACES,
                },
            },
        }),
    );
    schemas.insert(
        "NamespaceMetadataDeclaredState".to_owned(),
        json!({
            "type": "object",
            "required": [
                "active_manifest_count",
                "active_source_families",
                "chains",
                "normalizer_versions",
            ],
            "properties": {
                "active_manifest_count": {
                    "type": "integer",
                    "minimum": 0,
                },
                "active_source_families": {
                    "type": "array",
                    "items": { "type": "string" },
                },
                "chains": {
                    "type": "array",
                    "items": { "type": "string" },
                },
                "normalizer_versions": {
                    "type": "array",
                    "items": { "type": "string" },
                },
            },
        }),
    );
    schemas.insert(
        "NamespaceMetadataResponse".to_owned(),
        declared_response_schema(
            schema_ref("NamespaceData"),
            schema_ref("NamespaceMetadataDeclaredState"),
        ),
    );
    schemas.insert(
        "CapabilityFlag".to_owned(),
        json!({
            "type": "object",
            "required": ["status", "notes"],
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["unsupported", "shadow", "supported"],
                },
                "notes": {
                    "type": ["string", "null"],
                },
            },
        }),
    );
    schemas.insert(
        "NamespaceManifestEntry".to_owned(),
        json!({
            "type": "object",
            "required": [
                "manifest_version",
                "source_family",
                "chain",
                "deployment_epoch",
                "normalizer_version",
                "capability_flags",
            ],
            "properties": {
                "manifest_version": {
                    "type": "integer",
                    "minimum": 1,
                },
                "source_family": { "type": "string" },
                "chain": { "type": "string" },
                "deployment_epoch": { "type": "string" },
                "normalizer_version": { "type": "string" },
                "capability_flags": {
                    "type": "object",
                    "additionalProperties": schema_ref("CapabilityFlag"),
                },
            },
        }),
    );
    schemas.insert(
        "NamespaceManifestsDeclaredState".to_owned(),
        json!({
            "type": "object",
            "required": ["manifests"],
            "properties": {
                "manifests": {
                    "type": "array",
                    "items": schema_ref("NamespaceManifestEntry"),
                },
            },
        }),
    );
    schemas.insert(
        "NamespaceManifestsResponse".to_owned(),
        declared_response_schema(
            schema_ref("NamespaceData"),
            schema_ref("NamespaceManifestsDeclaredState"),
        ),
    );
    schemas.insert(
        "HealthResponse".to_owned(),
        json!({
            "type": "object",
            "required": ["service", "phase", "status"],
            "properties": {
                "service": { "type": "string" },
                "phase": { "type": "string" },
                "status": { "type": "string" },
            },
        }),
    );
    schemas.insert(
        "ErrorBody".to_owned(),
        json!({
            "type": "object",
            "required": ["code", "message", "details"],
            "properties": {
                "code": { "type": "string" },
                "message": { "type": "string" },
                "details": {
                    "type": "object",
                    "additionalProperties": { "type": "string" },
                },
            },
        }),
    );
    schemas.insert(
        "ErrorResponse".to_owned(),
        json!({
            "type": "object",
            "required": ["error"],
            "properties": {
                "error": schema_ref("ErrorBody"),
            },
        }),
    );

    let mut components = serde_json::Map::new();
    components.insert("schemas".to_owned(), JsonValue::Object(schemas));
    JsonValue::Object(components)
}
"###;

    let start = source
        .find(START)
        .unwrap_or_else(|| panic!("failed to find `{START}` in copied api source"));
    let end = [
        CURRENT_END,
        LEGACY_END,
        LEGACY_VISIBLE_END,
        SPLIT_SCHEMA_END,
        SPLIT_SCHEMA_VISIBLE_END,
    ]
        .into_iter()
        .filter_map(|marker| source[start..].find(marker).map(|offset| start + offset))
        .min()
        .unwrap_or_else(|| {
            panic!(
                "failed to find an OpenAPI component helper after `{START}` in copied api source"
            )
        });

    let mut rewritten = String::with_capacity(source.len() + REPLACEMENT.len());
    rewritten.push_str(&source[..start]);
    rewritten.push_str(REPLACEMENT);
    rewritten.push_str(&source[end..]);
    rewritten
}
