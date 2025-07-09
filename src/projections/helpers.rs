use std::fs::create_dir_all;
use std::path::Path;

pub fn create_folder(path: &Path) {
    let is_file = path.extension().is_some();

    // If the path is a file, ensure the parent directory exists
    if is_file {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                if let Err(e) = create_dir_all(parent) {
                    panic!("Failed to create parent folder {}: {}", parent.display(), e);
                }
            }
        }
    } else if !path.exists() {
        if let Err(e) = create_dir_all(path) {
            panic!("Failed to create folder {}: {}", path.display(), e);
        }
    }
}
