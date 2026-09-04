//! Filesystem permission enforcement for secrets at rest.

use std::path::Path;

/// Tighten `path` to owner-only (0600) on Unix. A missing file is not an
/// error (callers use this for optional SQLite sidecars); a file that is
/// group- or world-accessible but cannot be repaired is, so callers fail
/// closed on secrets they cannot secure. SQLite creates `-wal`, `-journal`,
/// and `-shm` sidecars with exactly the database file's mode, so securing
/// the database file before its first transaction secures the sidecars too.
pub fn restrict_to_owner(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(format!("stat {}: {error}", path.display())),
        };
        if metadata.permissions().mode() & 0o077 == 0 {
            return Ok(());
        }
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions)
            .map_err(|error| format!("restrict {}: {error}", path.display()))?;
        tracing::warn!(path = %path.display(), "tightened file permissions to 0600");
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn loose_files_are_tightened_and_missing_files_are_ignored() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!("open-ssp-mode-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, b"secret").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions).unwrap();

        restrict_to_owner(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        // Already-owner-only files are left untouched (idempotent).
        restrict_to_owner(&path).unwrap();
        // Missing files are not an error.
        restrict_to_owner(&path.with_extension("missing")).unwrap();
        std::fs::remove_file(&path).unwrap();
    }
}
