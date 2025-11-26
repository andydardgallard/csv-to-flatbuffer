use crate::cli;
use crate::utils;
use crate::resample;
use crate::ohlcv_generated;
use crate::ohlcv_soa_generated;

use rayon::prelude::*;

/// Determines the storage format (AOS or SOA) based on the file name extension.
/// 
/// Checks if the file name ends with `.aos.bin` or `.soa.bin`.
/// 
/// # Arguments
/// * `path` - The path to the FlatBuffer file (.bin).
/// 
/// # Returns
/// * `Some(StorageFormat)` if the format can be determined, `None` otherwise.
fn determine_storage_format_from_path<P: AsRef<std::path::Path>>(path: P) -> Option<cli::StorageFormat> {
    let file_name = path.as_ref().file_name()?.to_str()?;
    if file_name.ends_with(".aos.bin") {
        Some(cli::StorageFormat::Aos)
    } else if file_name.ends_with(".soa.bin") {
        Some(cli::StorageFormat::Soa)
    } else {
        None
    }
}

/// Reads .aos/.soa and .idx files, optionally resamples data,
/// and prints first 5 bars in human-readable format.
///
/// Uses mmap for zero-copy reading and supports multiple timeframes.
/// This function now identifies the storage format (AOS/SOA) from the file name
/// and processes the file accordingly.
///
/// # Arguments
/// * `output_dir_path` - Directory with .bin files.
/// * `resample` - Optional timeframe: "1min", "2min", "3min", "4min", "5min", "1d".
///
/// # Returns
/// * `anyhow::Result<()>`
pub fn read_flatbuffers<P: AsRef<std::path::Path> + Send + Sync>(
    output_dir_path: P,
    resample: Option<String>,
) -> anyhow::Result<()> {
    let paths = std::fs::read_dir(output_dir_path.as_ref())?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            path.extension().map_or(false, |ext| ext == "bin")
        })
        .collect::<Vec<_>>();

    paths.par_iter().try_for_each(|entry| {
        let path = entry.path();
        if let Some(format) = determine_storage_format_from_path(&path) {
            process_file(&path, &resample, format)?;
        } else {
            println!("⚠️ Skipping file with unknown format: {}", path.display());
        }

        Ok::<_, anyhow::Error>(())
    })?;
    
    Ok(())
}

/// Processes a single .bin file: reads, resamples, prints.
/// 
/// This function handles the core logic for reading a FlatBuffer file,
/// performing optional resampling based on the specified format (AOS/SOA),
/// and printing results. It uses `mmap` for efficient, zero-copy access.
///
/// # Arguments
/// * `path` - Path to the .bin file.
/// * `resample` - Optional timeframe string (e.g., "1min", "5min", "1d").
/// * `storage_format` - The format of the FlatBuffer data (AOS or SOA).
///
/// # Returns
/// * `anyhow::Result<()>`
fn process_file<P: AsRef<std::path::Path>>(
    path: P,
    resample: &Option<String>,
    storage_format: cli::StorageFormat,
) -> anyhow::Result<()> {
    println!("Processing reading in thread: {:?} fo file {:?}", std::thread::current().id(), path.as_ref());
    
    let file = std::fs::File::open(&path)?;
    let mmap = unsafe {memmap2::Mmap::map(&file)? };

    let idx_path = path.as_ref().with_extension("idx");
    let full_index = utils::load_full_index(&idx_path)?;
    let start = std::time::Instant::now();

    match storage_format {
        cli::StorageFormat::Aos => {
            // --- AOS Processing ---
            let ohlcv_list = ohlcv_generated::root_as_ohlcvlist(&mmap)
                .map_err(|_| anyhow::anyhow!("Failed to parse root as OHLCVList"))?;
            let items = ohlcv_list.items().unwrap_or_default();

            match resample.as_deref() {
                Some("1min") => {
                    println!("📄 Read first 5 1min bars (AOS)");
                    utils::print_bars_aos(&items, 5)?;
                }
                Some(tf) if ["2min", "3min", "4min", "5min"].contains(&tf) => {
                    let timeframe_sec = match tf {
                        "2min" => 120,
                        "3min" => 180,
                        "4min" => 240,
                        "5min" => 300,
                        _ => unreachable!(),
                    };
                    let resampled = resample::resample_ohlcv_aos(&items, &full_index.time_index, timeframe_sec)?;
                    println!("📈 Resampled to {} timeframe (AOS)", tf);
                    utils::print_bars_resampled(&resampled, 5)?;
                }
                Some("1d") => {
                    let daily_bars = resample::resample_daily_aos(&items, &full_index.daily_index)?;
                    println!("📈 Resampled to daily timeframe (AOS)");
                    utils::print_bars_resampled(&daily_bars, 5)?;
                }
                _ => {
                    println!("📄 Read first 5 OHLCV entries for file {} (AOS)", path.as_ref().display());
                    utils::print_bars_aos(&items, 5)?;
                }
            }
            
            println!(
                "✅ Resampling completed in {:?} seconds",
                start.elapsed().as_secs_f64()
            );
        }
        cli::StorageFormat::Soa => {
            // --- SOA Processing ---
            let ohlcv_list_soa = ohlcv_soa_generated::root_as_ohlcvlist_soa(&mmap)
                .map_err(|_| anyhow::anyhow!("Failed to parse root as OHLCVListSOA (SOA)"))?;
            let data_soa = ohlcv_list_soa.data().unwrap();

            match resample.as_deref() {
                Some("1min") => {
                    println!("📄 Read first 5 1min bars (SOA)");
                    utils::print_bars_soa(data_soa, 5)?;
                }
                Some(tf) if ["2min", "3min", "4min", "5min"].contains(&tf) => {
                    let timeframe_sec = match tf {
                        "2min" => 120,
                        "3min" => 180,
                        "4min" => 240,
                        "5min" => 300,
                        _ => unreachable!(),
                    };
                    let resampled = resample::resample_ohlcv_soa(data_soa, &full_index.time_index, timeframe_sec)?;
                    println!("📈 Resampled to {} timeframe (SOA)", tf);
                    utils::print_bars_resampled(&resampled, 5)?;
                }
                Some("1d") => {
                    let daily_bars = resample::resample_daily_soa(data_soa, &full_index.daily_index)?;
                    println!("📈 Resampled to daily timeframe (SOA)");
                    utils::print_bars_resampled(&daily_bars, 5)?;
                }
                _ => {
                    println!("📄 Read first 5 OHLCV entries for file {}", path.as_ref().display());
                    utils::print_bars_soa(data_soa, 5)?;
                }
            }

            println!(
                "✅ Resampling completed in {:?} seconds",
                start.elapsed().as_secs_f64()
            );
        }
    }

    anyhow::Ok(())
}
