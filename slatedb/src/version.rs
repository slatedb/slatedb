use crate::error::SlateDBError;
use crate::flatbuffer_types::SLATEDB_VERSION;

/// Represents different roles that can write to manifest files
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ManifestWriterRole {
    /// Database writer - owns the manifest and can upgrade/rollback versions
    DbWriter,
    /// Compactor - can write to manifest but must match version exactly
    Compactor,
    /// CLI tools - can write to manifest but must match version exactly  
    Cli,
    /// Admin tools - can write to manifest but must match version exactly
    Admin,
}

impl std::fmt::Display for ManifestWriterRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestWriterRole::DbWriter => write!(f, "DbWriter"),
            ManifestWriterRole::Compactor => write!(f, "Compactor"),
            ManifestWriterRole::Cli => write!(f, "CLI"),
            ManifestWriterRole::Admin => write!(f, "Admin"),
        }
    }
}

/// Validates that a manifest write operation is compatible with the stored version
/// 
/// # Arguments
/// * `stored_version` - The version stored in the existing manifest (None for new manifests)
/// * `writer_role` - The role of the component attempting to write
/// 
/// # Returns
/// * `Ok(())` if the write is allowed
/// * `Err(SlateDBError::SlateDBVersionMismatch)` if the versions are incompatible
pub(crate) fn check_manifest_version_compatibility(
    stored_version: Option<&str>,
    writer_role: ManifestWriterRole,
) -> Result<(), SlateDBError> {
    let current_version = SLATEDB_VERSION;
    
    match stored_version {
        None => {
            // New manifest - always allow
            Ok(())
        }
        Some(stored) if stored == current_version => {
            // Same version - always allow
            Ok(())
        }
        Some(stored) => {
            match writer_role {
                ManifestWriterRole::DbWriter => {
                    // DB writers (manifest owners) can upgrade or rollback versions
                    // This allows for controlled upgrades and rollbacks
                    log::warn!(
                        "DB writer updating manifest from version {} to version {}. \
                         This may be an upgrade or rollback operation.",
                        stored, current_version
                    );
                    Ok(())
                }
                _ => {
                    // Non-owners must match the version exactly to prevent data loss
                    Err(SlateDBError::SlateDBVersionMismatch {
                        expected_version: stored.to_string(),
                        actual_version: current_version.to_string(),
                        role: writer_role.to_string(),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_manifest_always_allowed() {
        // New manifests should always be allowed regardless of role
        assert!(check_manifest_version_compatibility(None, ManifestWriterRole::DbWriter).is_ok());
        assert!(check_manifest_version_compatibility(None, ManifestWriterRole::Compactor).is_ok());
        assert!(check_manifest_version_compatibility(None, ManifestWriterRole::Cli).is_ok());
        assert!(check_manifest_version_compatibility(None, ManifestWriterRole::Admin).is_ok());
    }

    #[test]
    fn test_same_version_always_allowed() {
        let current_version = SLATEDB_VERSION;
        
        // Same versions should always be allowed regardless of role
        assert!(check_manifest_version_compatibility(Some(current_version), ManifestWriterRole::DbWriter).is_ok());
        assert!(check_manifest_version_compatibility(Some(current_version), ManifestWriterRole::Compactor).is_ok());
        assert!(check_manifest_version_compatibility(Some(current_version), ManifestWriterRole::Cli).is_ok());
        assert!(check_manifest_version_compatibility(Some(current_version), ManifestWriterRole::Admin).is_ok());
    }

    #[test]
    fn test_db_writer_can_upgrade_rollback() {
        // DB writers should be allowed to change versions (upgrade/rollback)
        assert!(check_manifest_version_compatibility(Some("0.7.0"), ManifestWriterRole::DbWriter).is_ok());
        assert!(check_manifest_version_compatibility(Some("0.9.0"), ManifestWriterRole::DbWriter).is_ok());
    }

    #[test]
    fn test_non_owners_must_match_version() {
        // Non-owners should not be allowed to change versions
        let result = check_manifest_version_compatibility(Some("0.7.0"), ManifestWriterRole::Compactor);
        assert!(result.is_err());
        if let Err(SlateDBError::SlateDBVersionMismatch { expected_version, actual_version, role }) = result {
            assert_eq!(expected_version, "0.7.0");
            assert_eq!(actual_version, SLATEDB_VERSION);
            assert_eq!(role, "Compactor");
        } else {
            panic!("Expected SlateDBVersionMismatch error");
        }

        let result = check_manifest_version_compatibility(Some("0.7.0"), ManifestWriterRole::Cli);
        assert!(result.is_err());

        let result = check_manifest_version_compatibility(Some("0.7.0"), ManifestWriterRole::Admin);
        assert!(result.is_err());
    }
}