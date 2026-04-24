pub(super) fn canonicality_consistency(canonicality_summary: &JsonValue) -> &'static str {
    match string_field(provenance_field(canonicality_summary, "status")).as_deref() {
        Some("safe") => "safe",
        Some("finalized") => "finalized",
        _ => "head",
    }
}

pub(super) fn collection_consistency<'a>(
    summaries: impl Iterator<Item = &'a JsonValue>,
) -> &'static str {
    let mut consistency = "finalized";
    let mut saw_any = false;

    for summary in summaries {
        saw_any = true;
        match canonicality_consistency(summary) {
            "head" => return "head",
            "safe" => consistency = "safe",
            "finalized" => {}
            _ => consistency = "head",
        }
    }

    if saw_any { consistency } else { "head" }
}
