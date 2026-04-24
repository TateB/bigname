mod decode;
mod orphaning;
mod reads;
mod types;
mod upsert;
mod validation;

#[cfg(test)]
use crate::CanonicalityState;
#[cfg(test)]
use anyhow::Context;

pub use orphaning::mark_block_derived_normalized_events_range_orphaned;
pub use reads::{load_normalized_event_counts_by_kind, load_normalized_events_by_namespace};
pub use types::NormalizedEvent;
pub use upsert::upsert_normalized_events;

#[cfg(test)]
mod tests;
