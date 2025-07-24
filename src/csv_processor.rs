use crate::index;
use crate::ohlcv_generated;

/// Represents a single record from input CSV.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CsvRecord {
    #[serde(rename = "<DATE>")]
    date: String,
    #[serde(rename = "<TIME>")]
    time: String,
    #[serde(rename = "<OPEN>")]
    open: f64,
    #[serde(rename = "<HIGH>")]
    high: f64,
    #[serde(rename = "<LOW>")]
    low: f64,
    #[serde(rename = "<CLOSE>")]
    close: f64,
    #[serde(rename = "<VOL>")]
    vol: u64,
}

/// Intermediate processed record with timestamp.
#[derive(Debug, serde::Serialize)]
pub struct ProcessedRecord {
    timestamp: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    vol: u64,
}

/// Contains index data generated during the conversion from CSV to FlatBuffer format.
///
/// This struct holds various indices that enable fast lookups and resampling
/// of the OHLCV data stored in the resulting `.bin` FlatBuffer file.
///
/// These indices are serialized into a separate `.idx` file using `bincode`
/// for efficient loading during backtesting or analysis.
///
/// # Fields
///
/// * `time_index` - Maps timestamps to their positions (indices) within the FlatBuffer vector.
///                   Enables fast seeking to specific points in time.
/// * `daily_index` - Provides start and end indices for each trading day.
///                    Useful for quickly accessing data for a particular day without scanning the whole file.
/// * `timeframe_index` - Precomputed lists of timestamps for common resampling intervals (e.g., "1m", "5m").
///                       Facilitates rapid aggregation of data into larger timeframes.
pub struct ProcessedData {
    pub time_index: Vec<index::TimeIndexEntry>,
    pub daily_index: Vec<index::DailyIndexEntry>,
    pub timeframe_index: std::collections::HashMap<String, Vec<u64>>,
}

/// Processes CSV records and builds FlatBuffers OHLCV entries with indexing.
///
/// This function:
/// 1. Reads OHLCV records from a CSV reader.
/// 2. Parses datetime strings into Unix timestamps.
/// 3. Creates FlatBuffers OHLCV objects using the provided builder.
/// 4. Tracks time-based indices for fast lookup and resampling.
/// 5. Builds daily index entries for efficient day-based navigation.
/// 6. Maintains a map of supported timeframe timestamps for future resampling.
///
/// # Arguments
/// * `reader` - CSV reader for input data.
/// * `builder` - FlatBufferBuilder to create OHLCV objects.
/// * `time_index` - Output vector to store timestamp-to-offset mappings.
/// * `daily_index` - Output vector to store daily OHLCV ranges.
/// * `tf_index_map` - Output map to store timeframe-specific timestamps.
/// * `ohlcv_offsets` - Output vector to store FlatBuffer offsets for created OHLCVs.
///
/// # Returns
/// * `anyhow::Result<()>` - Success or an error if processing fails.
///
/// # Errors
/// * If datetime parsing fails.
/// * If CSV deserialization fails.
fn process_csv_records<'a, R: std::io::Read>(
    reader: &mut csv::Reader<R>,
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    time_index: &mut Vec<index::TimeIndexEntry>,
    daily_index: &mut Vec<index::DailyIndexEntry>,
    tf_index_map: &mut std::collections::HashMap<String, Vec<u64>>,
    ohlcv_offsets: &mut Vec<flatbuffers::WIPOffset<ohlcv_generated::OHLCV<'a>>>
) -> anyhow::Result<()> {
    let mut index_in_vector = 0u64;
    let mut current_day = None::<String>;
    let mut day_start_index = 0u64;

    let supported_timeframes = vec![
        ("1m", 60),
        ("2m", 120),
        ("3m", 180),
        ("4m", 240),
        ("5m", 300),
    ];
    
    for result in reader.deserialize::<CsvRecord>() {
        let record: CsvRecord = result?;
        let date_str = &record.date;
        let time_str = &record.time;
        let dt_str = format!("{} {}", date_str, time_str);
        
        let dt = chrono::NaiveDateTime::parse_from_str(&dt_str, "%Y%m%d %H%M%S")
        .map_err(|e| anyhow::anyhow!("Failed to parse datetime: {}", e))?;
        let timestamp = dt.and_utc().timestamp() as u64;
        
        let ohlcv_args = ohlcv_generated::OHLCVArgs {
            timestamp,
            open: record.open,
            high: record.high,
            low: record.low,
            close: record.close,
            volume: record.vol,
        };
    
        let ohlcv = ohlcv_generated::OHLCV::create(builder, &ohlcv_args);
        ohlcv_offsets.push(ohlcv);
        
        // index by time
        time_index.push(index::TimeIndexEntry {
            timestamp,
            index: index_in_vector,
        });
        
        //index by day
        let date_key = dt.format("%Y-%m-%d").to_string();
        if let Some(ref d) = current_day {
            if d != &date_key {
                if let Some(day) = current_day.take() {
                    let entry = index::DailyIndexEntry {
                        date: day,
                        start_index: day_start_index,
                        end_index: index_in_vector - 1,
                    };
                    daily_index.push(entry);
                }
                day_start_index = index_in_vector;
                current_day = Some(date_key.clone());
            }
        } else {
            current_day = Some(date_key.clone());
            day_start_index = index_in_vector;
        }
        index_in_vector += 1;
    
        // index by timeframes
        for (tf_name, tf_sec) in &supported_timeframes {
            if timestamp % tf_sec == 0 {
                tf_index_map
                .entry(tf_name.to_string())
                .or_insert_with(Vec::new)
                .push(timestamp);
            }
        }  
    }

    // last day
    if let Some(day) = current_day.take() {
        daily_index.push(index::DailyIndexEntry { 
            date: day,
            start_index: day_start_index,
            end_index: index_in_vector - 1,
        });
    }

    anyhow::Ok(())
}
    
/// Converts CSV data to a FlatBuffer binary file (.bin) and generates index data.
///
/// This function orchestrates the conversion process:
/// 1. Opens and reads the input CSV file.
/// 2. Initializes a FlatBufferBuilder and index collections.
/// 3. Calls `process_csv_records` to populate the builder and indices.
/// 4. Finalizes the FlatBuffer and writes the binary data to the output file.
/// 5. Packages the generated index data for later use.
///
/// # Arguments
/// * `input_dir_path` - Path to the input CSV file.
/// * `output_path` - Path for the output .bin file.
///
/// # Returns
/// * `anyhow::Result<ProcessedData>` - The generated index data or an error.
///
/// # Errors
/// * If file I/O fails.
/// * If `process_csv_records` fails.
fn save_flatbuffer<P: AsRef<std::path::Path>>(
    input_dir_path: P,
    output_path: P,
) -> anyhow::Result<ProcessedData> {
    let input_file = std::fs::File::open(input_dir_path)?;
    let mut reader = csv::ReaderBuilder::new().has_headers(true).from_reader(input_file);  
    
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024 * 1024);
    let mut time_index: Vec<index::TimeIndexEntry> = Vec::new();
    let mut daily_index: Vec<index::DailyIndexEntry> = Vec::new();
    let mut tf_index_map: std::collections::HashMap<String, Vec<u64>> = std::collections::HashMap::new();
    let mut ohlcv_offsets = Vec::new();

    process_csv_records(
        &mut reader,
        &mut builder,
        &mut time_index,
        &mut daily_index,
        &mut tf_index_map,
        &mut ohlcv_offsets,
    )?;

    let items = builder.create_vector(&ohlcv_offsets);
    let ohlcv_list = {
        let mut list_builder = ohlcv_generated::OHLCVListBuilder::new(&mut builder);
        list_builder.add_items(items);
        list_builder.finish()
    };
    builder.finish(ohlcv_list, None);
    std::fs::write(output_path.as_ref(), builder.finished_data())?;

    let processed_data = ProcessedData{
        time_index: time_index,
        daily_index: daily_index,
        timeframe_index: tf_index_map,
    };

    anyhow::Ok(processed_data)
}

/// Serializes and saves index data to a companion .idx file.
///
/// This function takes the generated time, daily, and timeframe indices,
/// packages them into a `FullIndex` struct, serializes it using `bincode`,
/// and writes it to a file with the same name as the output but with a `.idx` extension.
///
/// The .idx file enables fast random access and resampling without loading the full dataset.
///
/// # Arguments
/// * `time_index` - Vector of timestamp-to-index mappings.
/// * `daily_index` - Vector of daily OHLCV range mappings.
/// * `timeframe_index` - Map of timeframe names to lists of timestamps.
/// * `output_path` - Path to the main .bin file (used to derive .idx path).
///
/// # Returns
/// * `anyhow::Result<()>` - Success or an error if writing fails.
///
/// # Errors
/// * If serialization or file I/O fails.
fn save_index<P: AsRef<std::path::Path>>(
    time_index: &[index::TimeIndexEntry],
    daily_index: &[index::DailyIndexEntry],
    timeframe_index: &std::collections::HashMap<String, Vec<u64>>,
    output_path: P,
) -> anyhow::Result<()> {
    let idx_path = output_path.as_ref().with_extension("idx");
    let full_index = index::FullIndex {
        time_index: time_index.to_vec(),
        daily_index: daily_index.to_vec(),
        timeframe_index: timeframe_index.clone(),
    };
    
    let data = bincode::serialize(&full_index)?;
    std::fs::write(idx_path, data)?;

    anyhow::Ok(())
}

/// Public entry point to convert a CSV file to FlatBuffer format with indexing.
///
/// This function provides a high-level interface for the conversion process.
/// It delegates to `save_flatbuffer` for the core logic and `save_index` for
/// persisting the generated indices. It's designed to be called from `main.rs`
/// or other modules needing to trigger the conversion.
///
/// # Arguments
/// * `input_dir_path` - Path to the input CSV file.
/// * `output_path` - Path for the output .bin file.
///
/// # Returns
/// * `anyhow::Result<()>` - Success or an error if conversion or saving fails.
///
/// # Errors
/// * Propagates errors from `save_flatbuffer` or `save_index`.
pub fn convert_csv_to_flatbuffer<P: AsRef<std::path::Path>>(input_dir_path: P, output_path: P) -> anyhow::Result<()> {
    let processed_data = save_flatbuffer(
        input_dir_path.as_ref(),
        output_path.as_ref(),
    )?;
    save_index(
        &processed_data.time_index,
        &processed_data.daily_index,
        &processed_data.timeframe_index,
        output_path.as_ref()
    )?;

    anyhow::Ok(())
}
