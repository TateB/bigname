#[cfg(feature = "reth-db")]
mod enabled;
#[cfg(not(feature = "reth-db"))]
mod unavailable;

#[cfg(feature = "reth-db")]
pub use enabled::RethDbProvider;
#[cfg(not(feature = "reth-db"))]
pub use unavailable::RethDbProvider;
