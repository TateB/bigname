//! Narrow shared bootstrap surface for the repo.

pub mod normalization;

/// The current repository phase.
pub const fn bootstrap_phase() -> &'static str {
    "phase-1-bootstrap"
}
