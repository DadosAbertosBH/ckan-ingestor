// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use std::path::{Path, PathBuf};

pub struct TempFileCleanup {
    path: PathBuf,
    committed: bool,
}

impl TempFileCleanup {
    pub fn from_path(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TempFileCleanup;
    use std::fs::File;

    #[test]
    fn removes_uncommitted_files() -> anyhow::Result<()> {
        let path =
            std::env::temp_dir().join(format!("temp-file-cleanup-test-{}", uuid::Uuid::new_v4()));
        File::create(&path)?;

        {
            let _cleanup = TempFileCleanup::from_path(path.clone());
        }

        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn preserves_committed_files() -> anyhow::Result<()> {
        let path =
            std::env::temp_dir().join(format!("temp-file-cleanup-test-{}", uuid::Uuid::new_v4()));
        File::create(&path)?;

        {
            let mut cleanup = TempFileCleanup::from_path(path.clone());
            cleanup.commit();
        }

        assert!(path.exists());
        std::fs::remove_file(path)?;
        Ok(())
    }
}
