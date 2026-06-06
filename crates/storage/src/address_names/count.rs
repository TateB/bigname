use anyhow::Result;
use sqlx::PgPool;

use crate::{
    NameCurrentAddressFilter, NameCurrentAddressRelationFilter, NameCurrentListFilter,
    count_name_current_list,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentCountFilter {
    pub address: String,
    pub namespace: Option<String>,
    pub relation: NameCurrentAddressRelationFilter,
    pub prefix: Option<String>,
    pub contains: Option<String>,
    pub contains_nocase: Option<String>,
    pub resolver: Option<String>,
}

impl AddressNamesCurrentCountFilter {
    pub fn relation_label(&self) -> &'static str {
        self.relation.as_str()
    }
}

pub async fn count_address_names_current_for_app_filter(
    pool: &PgPool,
    filter: &AddressNamesCurrentCountFilter,
) -> Result<u64> {
    count_name_current_list(
        pool,
        &NameCurrentListFilter {
            namespace: filter.namespace.clone(),
            name: None,
            prefix: filter.prefix.clone(),
            contains: filter.contains.clone(),
            contains_nocase: filter.contains_nocase.clone(),
            resolver: filter.resolver.clone(),
            address: Some(NameCurrentAddressFilter {
                address: filter.address.clone(),
                relation: filter.relation,
                addresses: None,
            }),
            is_migrated: None,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AddressNameRelation;

    #[test]
    fn address_names_current_count_filter_exposes_relation_labels() {
        let any = AddressNamesCurrentCountFilter {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: None,
            relation: NameCurrentAddressRelationFilter::Any,
            prefix: None,
            contains: None,
            contains_nocase: None,
            resolver: None,
        };
        assert_eq!(any.relation_label(), "any");

        let holder = AddressNamesCurrentCountFilter {
            relation: NameCurrentAddressRelationFilter::Relation(AddressNameRelation::TokenHolder),
            ..any
        };
        assert_eq!(holder.relation_label(), "token_holder");
    }
}
