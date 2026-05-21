#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReverseIdentityPageRow {
    pub(super) input_index: usize,
    pub(super) logical_name_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReverseIdentityFirstPageRow {
    pub(super) input_index: usize,
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) canonical_display_name: String,
    pub(super) normalized_name: String,
    pub(super) namehash: String,
    pub(super) chain_positions: serde_json::Value,
    pub(super) coverage: serde_json::Value,
    pub(super) is_primary: bool,
    pub(super) relation_facets: Vec<crate::address_names::AddressNameRelation>,
}

impl From<&ReverseIdentityFirstPageRow> for ReverseIdentityPageRow {
    fn from(value: &ReverseIdentityFirstPageRow) -> Self {
        Self {
            input_index: value.input_index,
            logical_name_id: value.logical_name_id.clone(),
        }
    }
}
