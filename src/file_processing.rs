/// Gets list of `.txt` files from the specified directory.
///
/// # Arguments
/// * `dir_path` - Directory path to scan for `.txt` files.
///
/// # Returns
/// * `Result<Vec<String>>` - List of file paths.
pub fn get_list_files_in_dir<P: AsRef<std::path::Path>>(dir_path: P, ext_filter: Option<&str>) -> anyhow::Result<Vec<String>> {
    let mut files: Vec<String> = Vec::new();

    let dir_read_result: Result<std::fs::ReadDir, std::io::Error> = std::fs::read_dir(dir_path.as_ref());
    let dir_entries = match dir_read_result {
        Ok(entries) => entries,
        Err(e) => return Err(e.into()),
    };

    for entry in dir_entries {
        let dir_entry = match entry {
            Ok(entry) => entry,
            Err(e) => return Err(e.into()),
        };
        let path = dir_entry.path();

        if path.is_file() {
            if let Some(filter) = ext_filter {
                if path.extension().map(|e| e == filter).unwrap_or(false) {
                    files.push(path.to_string_lossy().into_owned());
                }
            }
        }
    }
    Ok(files)
}

/// Checks if the provided path is a valid directory.
///
/// # Arguments
/// * `path_argument` - Path to validate.
///
/// # Returns
/// * `Result<()>` - Success if path is a directory.
pub fn check_path<P: AsRef<std::path::Path>> (path_argument: P) -> anyhow::Result<()>{
    let metadata_path_in = std::fs::metadata(&path_argument)?;

    if !metadata_path_in.is_dir() {
        return Err(anyhow::anyhow!("Provide diretcory, not file!"));
    } else { Ok(()) }
}

/// Ensures parent directory exists; creates it if necessary.
///
/// Also ensures the target output directory exists and is clean.
///
/// # Arguments
/// * `out_dir_path` - Output directory path.
///
/// # Returns
/// * `Result<()>` - Success or error if creation fails.
pub fn ensure_parent_dir_exist<P: AsRef<std::path::Path>>(out_dir_path: P) -> anyhow::Result<()> {
    let out_dir = out_dir_path.as_ref();
    let parent_dir = match std::path::Path::new(out_dir_path.as_ref()).parent() {
        Some(dir) => dir.to_path_buf(),
        None => return Err(anyhow::anyhow!("Pls input correct path of output dir of Parquet files")),
    };
    if !parent_dir.exists() {
        std::fs::create_dir_all(parent_dir)?;
    }
    if !out_dir.exists() {
        std::fs::create_dir_all(out_dir)?;
    } else {
        for entry in std::fs::read_dir(out_dir)? {
            let path = entry?.path();
            if path.is_file() {
                std::fs::remove_file(path)?;
            }
        }
    }

    Ok(())
}
