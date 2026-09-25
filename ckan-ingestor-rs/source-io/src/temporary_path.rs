use std::path::{Path, PathBuf};

/// Owns a temporary path and removes it when dropped.
///
/// Unlike the former cleanup guard, this type has no commit operation: code
/// that needs a file to outlive a scope transfers this owner explicitly.
pub struct TemporaryPath {
    path: PathBuf,
}

impl TemporaryPath {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
