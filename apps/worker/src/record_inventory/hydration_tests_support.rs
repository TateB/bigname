use super::*;

struct StaticTextHydrationClient {
    values: BTreeMap<(String, String, String), TextHydrationOutcome>,
}

impl TextHydrationClient for StaticTextHydrationClient {
    fn hydrate<'a>(
        &'a self,
        _chain_id: &'a str,
        _position: &'a TextHydrationChainPosition,
        calls: &'a [TextHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<TextHydrationOutcome>>> {
        async move {
            Ok(calls
                .iter()
                .map(|call| {
                    self.values
                        .get(&(
                            normalize_address(&call.resolver_address),
                            call.name.clone(),
                            call.text_key.clone(),
                        ))
                        .cloned()
                        .unwrap_or_else(|| {
                            TextHydrationOutcome::Failed("missing mock value".to_owned())
                        })
                })
                .collect())
        }
        .boxed()
    }
}

pub(crate) async fn hydrate_with_values(
    pool: &PgPool,
    resource_id: Option<&str>,
    values: &[(&str, &str, &str, &str)],
) -> Result<RecordInventoryTextHydrationSummary> {
    let client = StaticTextHydrationClient {
        values: values
            .iter()
            .map(|(resolver_address, name, text_key, value)| {
                (
                    (
                        normalize_address(resolver_address),
                        (*name).to_owned(),
                        (*text_key).to_owned(),
                    ),
                    TextHydrationOutcome::Success((*value).to_owned()),
                )
            })
            .collect(),
    };
    hydrate_record_inventory_text_values_with_client(pool, resource_id, &client).await
}
