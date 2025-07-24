use chrono::TimeZone;
use crate::index;
use crate::resample;
use crate::ohlcv_generated;

/// Configures a custom Rayon thread pool with specified size.
///
/// # Arguments
/// * `num_threads` - Desired number of threads.
///
/// # Returns
/// * `Result<ThreadPool>` - Created thread pool or error.
pub fn configure_thread_pool(num_threads: usize) -> anyhow::Result<rayon::ThreadPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build thread pool: {}", e))
}

/// Loads FullIndex from .idx file serialized with bincode.
///
/// # Arguments
/// * `idx_path` - Path to .idx file.
///
/// # Returns
/// * Deserialized `FullIndex` or error.
pub fn load_full_index<P: AsRef<std::path::Path> + Send + Sync>(idx_path: P) -> anyhow::Result<index::FullIndex> {
    let data = std::fs::read(idx_path)?;
    let index = bincode::deserialize(&data)?;
    anyhow::Ok(index)
}

/// Converts a date string in the format `%Y-%m-%d` to a Unix timestamp (in seconds).
///
/// This function assumes that the time part is midnight (00:00:00 UTC).
///
/// # Arguments
///
/// * `date_str` - A string slice representing the date in the format `%Y-%m-%d`.
///
/// # Returns
///
/// * `anyhow::Result<u64>` - The corresponding Unix timestamp, or an error if parsing fails.
///
/// # Examples
///
/// ```
/// let ts = parse_date_to_timestamp("2025-07-08").unwrap();
/// assert_eq!(ts, 1751980800);
/// ``` 
pub fn parse_date_to_timestamp(date_str: &str) -> anyhow::Result<u64> {
    let dt = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
    let dt = dt.and_hms_opt(0, 0, 0).unwrap();
    let timestamp = dt.and_utc().timestamp() as u64;
    Ok(timestamp)
}

/// Formats Unix timestamp into readable string: YYYYMMDD HHMMSS.
///
/// # Arguments
/// * `ts` - Unix timestamp in seconds.
///
/// # Returns
/// * Formatted string or error if invalid timestamp.
pub fn format_timestamp(ts: u64) -> anyhow::Result<String> {
    let dt = chrono::Utc.timestamp_opt(ts as i64, 0).unwrap();
    let output = dt.format("%Y%m%d %H%M%S").to_string();
    anyhow::Ok(output)
}

/// Prints the first `count` OHLCV bars from a FlatBuffers Vector.
///
/// This function iterates through the first `count` elements of a `flatbuffers::Vector<OHLCV>`,
/// retrieves each bar's fields using the generated FlatBuffers accessor methods (e.g., `.timestamp()`, `.open()`),
/// formats the timestamp into a human-readable string, and prints the data.
///
/// It is designed for displaying raw, unmodified OHLCV data loaded directly from a `.bin` file.
///
/// # Arguments
/// * `items` - A reference to the FlatBuffers vector containing `OHLCV` objects.
/// * `count` - The maximum number of bars to print (e.g., first 5).
///
/// # Returns
/// * `anyhow::Result<()>` - Indicates success or an error during timestamp formatting or printing.
///
/// # Example Output
/// ```text
///  - ts: 20231214 090000, open: 90302.00, high: 90399.00, low: 90120.00, close: 90265.00, vol: 1320
///  - ts: 20231214 090100, open: 90252.00, high: 90255.00, low: 90224.00, close: 90234.00, vol: 154
/// ```
///
/// # Notes
/// * Uses zero-copy access via `items.get(i)`.
/// * Relies on `utils::format_timestamp` for readable datetime strings.
pub fn print_bars(
    items: &flatbuffers::Vector<flatbuffers::ForwardsUOffset<ohlcv_generated::OHLCV<'_>>>,
    count: usize
) -> anyhow::Result<()>
{
    for i in 0..std::cmp::min(count, items.len()) {
        let item = items.get(i);
        let ts = item.timestamp();
        let formated = format_timestamp(ts)?;
        println!(
            " - ts: {}, open: {:.2}, high: {:.2}, low: {:.2}, close: {:.2}, vol: {}",
            formated,
            item.open(),
            item.high(),
            item.low(),
            item.close(),
            item.volume(),
        );
    }
    
    anyhow::Ok(())
}

/// Prints the first `count` resampled OHLCV bars from a slice of `OHLCVBar` structs.
///
/// This function is used to display aggregated OHLCV data (e.g., 5-minute bars created from 1-minute data).
/// Each `OHLCVBar` is a plain Rust struct with owned `f64`/`u64` fields, making it suitable for post-processing.
///
/// It formats the timestamp into a human-readable string and prints key price/volume data.
///
/// # Arguments
/// * `items` - A slice of `OHLCVBar` structs produced by resampling logic.
/// * `count` - The maximum number of bars to print (e.g., first 5).
///
/// # Returns
/// * `anyhow::Result<()>` - Indicates success or an error during timestamp formatting or printing.
///
/// # Example Output
/// ```text
///  - ts: 20231214 090000, open: 90302.00, high: 90399.00, low: 90120.00, close: 90265.00, vol: 1320
///  - ts: 20231214 090500, open: 90252.00, high: 90455.00, low: 90224.00, close: 90334.00, vol: 2154
/// ```
///
/// # Notes
/// * Designed for use with resampled data stored in `Vec<OHLCVBar>`.
/// * Does not involve FlatBuffers; operates on standard Rust structs.
/// * Uses `utils::format_timestamp` for readable datetime strings.
pub fn print_bars_resampled(
    items: &[resample::OHLCVBar],
    count: usize
) -> anyhow::Result<()>
{
    for i in 0..std::cmp::min(count, items.len()) {
        let item = &items[i];
        let ts = item.timestamp;
        let formated = format_timestamp(ts)?;
        println!(
            " - ts: {}, open: {:.2}, high: {:.2}, low: {:.2}, close: {:.2}, vol: {}",
            formated,
            item.open,
            item.high,
            item.low,
            item.close,
            item.volume,
        );
    }
    
    anyhow::Ok(())
}
