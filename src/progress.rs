use crate::cli;
use crate::csv_processor;
use crate::file_processing;

use rayon::prelude::*;

/// Processes each CSV file in parallel, converting to Parquet with progress tracking.
///
/// # Arguments
/// * `csv_path` - Input directory with CSV files.
/// * `out_dir_path` - Output directory for Parquet files.
///
/// # Returns
/// * `Result<()>` - Success or error if any conversion fails.
pub fn process_files<P: AsRef<std::path::Path> + std::marker::Sync>(csv_path: P, out_dir_path: P, storage_format: cli::StorageFormat) -> anyhow::Result<()> {
    let files_list: Vec<String> = file_processing::get_list_files_in_dir(&csv_path, Some("txt"))?;
    println!("📂 Found {} file(s) to convert", files_list.len());

    let m = indicatif::MultiProgress::new();
    let pb = m.add(indicatif::ProgressBar::new(files_list.len() as u64));
    pb.set_style(indicatif::ProgressStyle::default_bar()
        .template("[{wide_bar}] {pos}/{len} files converted ({percent}%))")?
        .progress_chars("=>-"));

    let log_pb = m.add(indicatif::ProgressBar::new(4));
    log_pb.set_style(indicatif::ProgressStyle::default_spinner());

    let files_processed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    files_list.par_iter().for_each(|file| {
        let input_path = std::path::Path::new(&file);
        let file_stem = match input_path.file_stem() {
            Some(stem) => stem.to_str().unwrap_or("output"),
            None => "output",
        };
        let output_file_name = match storage_format {
            cli::StorageFormat::Aos => {
                format!("{}.aos.bin", file_stem)
            }
            cli::StorageFormat::Soa => {
                format!("{}.soa.bin", file_stem)
            }
        };
        let output_path = out_dir_path.as_ref().join(output_file_name);
        let start = std::time::Instant::now();

        match csv_processor::convert_csv_to_flatbuffer(input_path, &output_path, storage_format.clone()) {
            Ok(_) => {
                let duration = start.elapsed();
                m.println(format!(
                    "✅ Converted '{}' in {:.2}s",
                    file,
                    duration.as_secs_f64()
                )).unwrap();
            },
            Err(e) => {
                m.println(format!("❌ Failed to convert file {}: {}", file, e)).unwrap();
            }
        }

        pb.inc(1);
        files_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });

    pb.finish_with_message("✅ All files converted");
    m.clear().unwrap();
    
    Ok(())
}
