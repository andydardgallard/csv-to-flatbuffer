use crate::utils;
use crate::index;
use crate::ohlcv_generated;
use crate::ohlcv_soa_generated;

/// Represents a single aggregated OHLCV bar after resampling.
///
/// This struct holds the essential fields for an OHLCV bar:
/// - `timestamp`: The start time of the bar (Unix timestamp).
/// - `open`, `high`, `low`, `close`: Price values.
/// - `volume`: Trading volume during the bar period.
#[derive(Debug, Clone, Copy)]
pub struct OHLCVBar {
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

// --- AOS Resampling Functions ---

/// Resamples a vector of OHLCV records (AOS format) into daily OHLCV bars using a daily index.
///
/// This function groups OHLCV records by day using the provided `daily_index`.
/// Each group is aggregated into a single daily bar with:
/// - Open: First bar's open
/// - High: Max high across all bars in the day
/// - Low: Min low across all bars in the day
/// - Close: Last bar's close
/// - Volume: Sum of volumes
///
/// # Arguments
///
/// * `items` - A FlatBuffers vector of OHLCV objects (Array of Structures format).
/// * `daily_index` - A slice of `DailyIndexEntry` indicating the start and end indices for each day.
///
/// # Returns
///
/// * `anyhow::Result<Vec<OHLCVBar>>` - A vector of daily OHLCV bars or an error.
pub fn resample_daily_aos(
    items: &flatbuffers::Vector<flatbuffers::ForwardsUOffset<ohlcv_generated::OHLCV>>,
    daily_index: &[index::DailyIndexEntry],
) -> anyhow::Result<Vec<OHLCVBar>> {
    let mut resampled = Vec::new();

    for entry in daily_index {
        let start = entry.start_index as usize;
        let end = entry.end_index as usize;

        if start >= items.len() || end >= items.len() || start > end {
            continue;
        }
        let first = items.get(start);
        let mut bar = OHLCVBar {
            timestamp: utils::parse_date_to_timestamp(&entry.date)?,
            open: first.open(),
            high: first.high(),
            low: first.low(),
            close: first.close(),
            volume: first.volume(),
        };
        for i in start + 1..= end {
            let item = items.get(i);
            bar.high = bar.high.max(item.high());
            bar.low = bar.low.min(item.low());
            bar.close = item.close();
            bar.volume += item.volume();
        }
        resampled.push(bar);
    }
    Ok(resampled)
} 

/// Resamples a vector of OHLCV records (AOS format) into bars of a specified timeframe.
///
/// This function groups OHLCV records into bars of `timeframe_sec` duration.
/// It aggregates each group into a single bar with:
/// - Open: First bar's open
/// - High: Max high across all bars in the timeframe
/// - Low: Min low across all bars in the timeframe
/// - Close: Last bar's close
/// - Volume: Sum of volumes
///
/// # Arguments
///
/// * `items` - A FlatBuffers vector of OHLCV objects (Array of Structures format).
/// * `time_index` - A slice of `TimeIndexEntry` linking timestamps to indices in the AOS vector.
/// * `timeframe_sec` - The desired timeframe in seconds (e.g., 180 for 3 minutes).
///
/// # Returns
///
/// * `anyhow::Result<Vec<OHLCVBar>>` - A vector of resampled OHLCV bars or an error.
pub fn resample_ohlcv_aos(
    items: &flatbuffers::Vector<flatbuffers::ForwardsUOffset<ohlcv_generated::OHLCV>>,
    time_index: &[index::TimeIndexEntry],
    timeframe_sec: u64,
) -> anyhow::Result<Vec<OHLCVBar>> {
    let mut resampled = Vec::new();
    let mut current_bar: Option<OHLCVBar> = None;

    for entry in time_index {
        let i = entry.index as usize;
        if i >= items.len() {
            continue;
        }

        let item = items.get(i);
        let bar_start = item.timestamp() - (item.timestamp() % timeframe_sec);

        match current_bar {
            Some(ref mut bar) if bar.timestamp == bar_start => {
                bar.high = bar.high.max(item.high());
                bar.low = bar.low.min(item.low());
                bar.close = item.close();
                bar.volume += item.volume();
            }
            Some(bar) => {
                resampled.push(bar);
                current_bar = Some(OHLCVBar {
                    timestamp: bar_start,
                    open: item.open(),
                    high: item.high(),
                    low: item.low(),
                    close: item.close(),
                    volume: item.volume(),
                });
            }
            None => {
                current_bar = Some(OHLCVBar {
                    timestamp: bar_start,
                    open: item.open(),
                    high: item.high(),
                    low: item.low(),
                    close: item.close(),
                    volume: item.volume(),
                });
            }
        }
    }

    if let Some(bar) = current_bar {
        resampled.push(bar);
    }

    Ok(resampled)
}

// --- SOA Resampling Functions ---

/// Resamples OHLCV data (SOA format) into daily OHLCV bars using a daily index.
///
/// This function groups OHLCV records by day using the provided `daily_index`.
/// It accesses data from the separate arrays within the `OHLCVSOA` object (Structure of Arrays).
/// Each group is aggregated into a single daily bar with:
/// - Open: First bar's open
/// - High: Max high across all bars in the day
/// - Low: Min low across all bars in the day
/// - Close: Last bar's close
/// - Volume: Sum of volumes
///
/// # Arguments
///
/// * `data_soa` - The FlatBuffers OHLCVSOA object containing separate arrays for each field.
/// * `daily_index` - A slice of `DailyIndexEntry` indicating the start and end indices for each day.
///
/// # Returns
///
/// * `anyhow::Result<Vec<OHLCVBar>>` - A vector of daily OHLCV bars or an error.
pub fn resample_daily_soa(
    data_soa: ohlcv_soa_generated::OHLCVSOA,
    daily_index: &[index::DailyIndexEntry],
) -> anyhow::Result<Vec<OHLCVBar>> {
    let timestamps = data_soa.timestamps().unwrap_or_default();
    let opens = data_soa.opens().unwrap_or_default();
    let highs = data_soa.highs().unwrap_or_default();
    let lows = data_soa.lows().unwrap_or_default();
    let closes = data_soa.closes().unwrap_or_default();
    let volumes = data_soa.volumes().unwrap_or_default();

    let mut resampled = Vec::new();
    for entry in daily_index {
        let start = entry.start_index as usize;
        let end = entry.end_index as usize;

        let len = std::cmp::min(timestamps.len(), opens.len());
        let len = std::cmp::min(len, highs.len());
        let len = std::cmp::min(len, lows.len());
        let len = std::cmp::min(len, closes.len());
        let len = std::cmp::min(len, volumes.len());

        if start >= len || end >= len || start > end {
            continue;
        }

        // let first_ts = timestamps.get(start);
        let first_open = opens.get(start);
        let first_high = highs.get(start);
        let first_low = lows.get(start);
        let first_close = closes.get(start);
        let first_vol = volumes.get(start);

        let mut bar = OHLCVBar {
            timestamp: utils::parse_date_to_timestamp(&entry.date)?,
            open: first_open,
            high: first_high,
            low: first_low,
            close: first_close,
            volume: first_vol,
        };
        for i in start + 1..= end {
            // let ts = timestamps.get(i);
            // let open = opens.get(i);
            let high = highs.get(i);
            let low = lows.get(i);
            let close = closes.get(i);
            let vol = volumes.get(i);

            bar.high = bar.high.max(high);
            bar.low = bar.low.min(low);
            bar.close = close;
            bar.volume += vol;
        }
        resampled.push(bar);
    }
    
    anyhow::Ok(resampled)
}

/// Resamples OHLCV data (SOA format) into bars of a specified timeframe.
///
/// This function groups OHLCV records into bars of `timeframe_sec` duration.
/// It accesses data from the separate arrays within the `OHLCVSOA` object (Structure of Arrays).
/// It uses the `time_index` (which maps timestamps to their original vector indices) to find data points.
/// It aggregates each group into a single bar with:
/// - Open: First bar's open
/// - High: Max high across all bars in the timeframe
/// - Low: Min low across all bars in the timeframe
/// - Close: Last bar's close
/// - Volume: Sum of volumes
///
/// # Arguments
///
/// * `data_soa` - The FlatBuffers OHLCVSOA object containing separate arrays for each field.
/// * `time_index` - A slice of `TimeIndexEntry` linking timestamps to their original vector indices (used to access SOA arrays).
/// * `timeframe_sec` - The desired timeframe in seconds (e.g., 180 for 3 minutes).
///
/// # Returns
///
/// * `anyhow::Result<Vec<OHLCVBar>>` - A vector of resampled OHLCV bars or an error.
pub fn resample_ohlcv_soa(
    data_soa: ohlcv_soa_generated::OHLCVSOA,
    time_index: &[index::TimeIndexEntry],
    timeframe_sec: u64,
) -> anyhow::Result<Vec<OHLCVBar>> {
    let timestamps = data_soa.timestamps().unwrap_or_default();
    let opens = data_soa.opens().unwrap_or_default();
    let highs = data_soa.highs().unwrap_or_default();
    let lows = data_soa.lows().unwrap_or_default();
    let closes = data_soa.closes().unwrap_or_default();
    let volumes = data_soa.volumes().unwrap_or_default();

    let mut resampled = Vec::new();
    let mut current_bar: Option<OHLCVBar> = None;

    for entry in time_index {
        let i = entry.index as usize;

        let len = std::cmp::min(timestamps.len(), opens.len());
        let len = std::cmp::min(len, highs.len());
        let len = std::cmp::min(len, lows.len());
        let len = std::cmp::min(len, closes.len());
        let len = std::cmp::min(len, volumes.len());

        if i >= len {
            continue;
        }

        let ts = timestamps.get(i);
        let open = opens.get(i);
        let high = highs.get(i);
        let low = lows.get(i);
        let close = closes.get(i);
        let vol = volumes.get(i);

        let bar_start = ts - (ts % timeframe_sec);
        match current_bar {
            Some(ref mut bar) if bar.timestamp == bar_start => {
                bar.high = bar.high.max(high);
                bar.low = bar.low.min(low);
                bar.close = close;
                bar.volume += vol;
            }
            Some(bar) => {
                resampled.push(bar);
                current_bar = Some(OHLCVBar {
                    timestamp: bar_start,
                    open,
                    high,
                    low,
                    close,
                    volume: vol,
                }); 
            }
            None => {
                current_bar = Some(OHLCVBar {
                    timestamp: bar_start,
                    open,
                    high,
                    low,
                    close,
                    volume: vol,
                });
            }
        }
    }

    if let Some(bar) = current_bar {
        resampled.push(bar);
    }

    anyhow::Ok(resampled)
}
