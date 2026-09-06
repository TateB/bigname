//! ENSv1, ENSv2, and Basenames event normalization adapters.

#[allow(dead_code)]
mod evm_abi;
pub mod schema_v2;

pub use schema_v2::{
    AdapterSession as SchemaV2AdapterSession, AddressAdmissionInput,
    BatchInput as SchemaV2BatchInput, BatchOutput as SchemaV2BatchOutput, DiscoveryRuleInput,
    InterpreterStateRequest, ManifestInput as SchemaV2ManifestInput,
    NormalizedEvent as SchemaV2NormalizedEvent, PriorEventInput,
    RawLogInput as SchemaV2RawLogInput, StateCacheCapacity, begin_schema_v2_adapter_restore,
    begin_schema_v2_adapter_restore_with_provenance, interpret_schema_v2_batch,
    prepare_schema_v2_batch_incremental, prepare_schema_v2_batch_incremental_with_provenance,
};
