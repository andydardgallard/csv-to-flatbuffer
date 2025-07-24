use rayon::prelude::*;
use crate::utils;
use crate::resample;
use crate::ohlcv_generated;

/// Reads .bin and .idx files, optionally resamples data,
/// and prints first 5 bars in human-readable format.
///
/// Uses mmap for zero-copy reading and supports multiple timeframes.
///
/// # Arguments
/// * `output_dir_path` - Directory with .bin files.
/// * `resample` - Optional timeframe: "1min", "5min", "1d".
///
/// # Returns
/// * `anyhow::Result<()>`
pub fn read_flatbuffers<P: AsRef<std::path::Path> + Send + Sync>(
    output_dir_path: P,
    resample: Option<String>,
) -> anyhow::Result<()> {
    let paths = std::fs::read_dir(output_dir_path.as_ref())?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();

    paths.par_iter().try_for_each(|entry| {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "bin") {
            process_file(&path, &resample)?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
    
    Ok(())
}

/// Processes a single .bin file: reads, resamples, prints.
///
/// # Arguments
/// * `path` - Path to .bin file.
/// * `resample` - Optional timeframe.
///
/// # Returns
/// * `anyhow::Result<()>`
fn process_file<P: AsRef<std::path::Path>>(
    path: P,
    resample: &Option<String>,
) -> anyhow::Result<()> {
    println!("Processing reading in thread: {:?} fo file {:?}", std::thread::current().id(), path.as_ref());
    
    let file = std::fs::File::open(&path)?;
    let mmap = unsafe {memmap2::Mmap::map(&file)? };
    let ohlcv_list = ohlcv_generated::root_as_ohlcvlist(&mmap)
    .map_err(|_| anyhow::anyhow!("Failed to parse root as OHLCVList"))?;
    let items = ohlcv_list.items().unwrap_or_default();

    let idx_path = path.as_ref().with_extension("idx");
    let full_index = utils::load_full_index(&idx_path)?;

    let start = std::time::Instant::now();
    match resample.as_deref() {
        Some("1min") => {
            println!("📄 Read first 5 1min bars");
            utils::print_bars(&items, 5)?;
        }
        Some(tf) if ["2min", "3min", "4min", "5min"].contains(&tf) => {
            let timeframe_sec = match tf {
                "2min" => 120,
                "3min" => 180,
                "4min" => 240,
                "5min" => 300,
                _ => unreachable!(),
            };
            let resampled = resample::resample_ohlcv(&items, &full_index.time_index, timeframe_sec)?;
            println!("📈 Resampled to {} timeframe", tf);
            utils::print_bars_resampled(&resampled, 5)?;
        }
        Some("1d") => {
            let daily_bars = resample::resample_daily(&items, &full_index.daily_index)?;
            println!("📈 Resampled to daily timeframe");
            utils::print_bars_resampled(&daily_bars, 5)?;
        }
        _ => {
            println!("📄 Read first 5 OHLCV entries for file {}", path.as_ref().display());
            utils::print_bars(&items, 5)?;
        }
    }
    
    println!(
        "✅ Resampling completed in {:?} seconds",
        start.elapsed().as_secs_f64()
    );
    
    anyhow::Ok(())
}
