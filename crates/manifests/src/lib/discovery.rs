#[path = "discovery/admission.rs"]
mod admission;
#[path = "discovery/loading.rs"]
mod loading;
#[path = "discovery/persistence.rs"]
mod persistence;
#[path = "discovery/provenance.rs"]
mod provenance;
#[path = "discovery/reconciliation.rs"]
mod reconciliation;
#[path = "discovery/types.rs"]
mod types;

pub use admission::DiscoveryAdmissionState;
pub use loading::load_discovery_admission_state;
pub use persistence::persist_discovery_observation;
pub use reconciliation::{
    reconcile_discovery_observations, reconcile_scoped_discovery_observations,
};
pub use types::{
    AdmittedDiscoveryEdge, DiscoveryCandidate, DiscoveryObservation, DiscoveryPersistenceSummary,
    DiscoveryReconciliationSummary,
};
