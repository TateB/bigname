use super::*;

pub(super) fn apply_observation(
    history: &mut NameHistory,
    observation: AuthorityObservation,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    match observation {
        AuthorityObservation::RegistrationGranted(event) => {
            apply_registration_granted(history, event, block_index)?;
        }
        AuthorityObservation::RegistrationRenewed(event) => {
            apply_registration_renewed(history, event, block_index)?;
        }
        AuthorityObservation::TokenTransferred(event) => {
            apply_token_transferred(history, event)?;
        }
        AuthorityObservation::WrapperNameWrapped(event) => {
            apply_wrapper_name_wrapped(history, event)?;
        }
        AuthorityObservation::WrapperNameUnwrapped(event) => {
            apply_wrapper_name_unwrapped(history, event)?;
        }
        AuthorityObservation::WrapperFusesSet(event) => {
            apply_wrapper_fuses_set(history, event)?;
        }
        AuthorityObservation::WrapperExpiryExtended(event) => {
            apply_wrapper_expiry_extended(history, event)?;
        }
        AuthorityObservation::WrapperTokenTransferred(event) => {
            apply_wrapper_token_transferred(history, event)?;
        }
        AuthorityObservation::ResolverChanged(event) => {
            apply_resolver_changed(history, event)?;
        }
        AuthorityObservation::RecordChanged(event) => {
            apply_record_changed(history, event)?;
        }
        AuthorityObservation::RecordVersionChanged(event) => {
            apply_record_version_changed(history, event)?;
        }
        AuthorityObservation::RegistryOwnerChanged(event) => {
            apply_registry_owner_changed(history, event)?;
        }
    }

    Ok(())
}
