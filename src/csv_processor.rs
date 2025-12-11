use crate::cli;
use crate::index;
use crate::ohlcv_generated;
use crate::ohlcv_soa_generated;

/// Represents a single record from input CSV.
/// 
/// This struct maps the columns of the input CSV file using serde attributes.
/// The expected CSV format is: <DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>
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
/// 
/// This struct holds OHLCV data after parsing the datetime string into a Unix timestamp.
/// It's used to accumulate raw data before FlatBuffer creation, facilitating both AOS and SOA processing.
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

// --- SOA Builder Implementation ---
// The SOABuilder struct and its implementation handle the creation of FlatBuffer data
// in the Structure of Arrays (SOA) format.

/// A builder for creating FlatBuffer data in Structure of Arrays (SOA) format.
/// 
/// This struct accumulates OHLCV data into separate vectors for each field
/// before finalizing the FlatBuffer binary representation.
struct SOABuilder<'a> {
    builder: flatbuffers::FlatBufferBuilder<'a>,
    timestamps: Vec<u64>,
    opens: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,
    closes: Vec<f64>,
    volumes: Vec<u64>,
}

impl<'a> SOABuilder<'a> {
    /// Creates a new `SOABuilder` with an initial buffer capacity.
    pub fn new() -> Self {
        Self {
            builder: flatbuffers::FlatBufferBuilder::with_capacity(1024 * 1024),
            timestamps: Vec::new(),
            opens: Vec::new(),
            highs: Vec::new(),
            lows: Vec::new(),
            closes: Vec::new(),
            volumes: Vec::new(),
        }
    }

    /// Adds a single OHLCV record to the builder's internal vectors.
    pub fn add_ohlcv(&mut self, timestamp: u64, open: f64, high: f64, low: f64, close: f64, volume: u64) {
        self.timestamps.push(timestamp);
        self.opens.push(open);
        self.highs.push(high);
        self.lows.push(low);
        self.closes.push(close);
        self.volumes.push(volume);
    }

    /// Finalizes the FlatBuffer data by creating the SOA structure and returning the binary vector.
    /// 
    /// This method takes ownership of `self`, constructs the FlatBuffer objects for the SOA layout,
    /// and returns the final binary representation.
    pub fn finish_buffer(self) -> Vec<u8> {
        // Destructure `self` to get access to the builder and the accumulated vectors
        let Self { mut builder, timestamps, opens, highs, lows, closes, volumes } = self;

        // Create FlatBuffer vectors from the accumulated data
        let timestamps_vec = builder.create_vector(&timestamps);
        let opens_vec = builder.create_vector(&opens);
        let highs_vec = builder.create_vector(&highs);
        let lows_vec = builder.create_vector(&lows);
        let closes_vec = builder.create_vector(&closes);
        let volumes_vec = builder.create_vector(&volumes);

        // Build the OHLCVSOa object containing the separate vectors
        let ohlcv_soa = {
            let mut ohlcv_soa_builder = ohlcv_soa_generated::OHLCVSOABuilder::new(&mut builder);
            ohlcv_soa_builder.add_timestamps(timestamps_vec);
            ohlcv_soa_builder.add_opens(opens_vec);
            ohlcv_soa_builder.add_highs(highs_vec);
            ohlcv_soa_builder.add_lows(lows_vec);
            ohlcv_soa_builder.add_closes(closes_vec);
            ohlcv_soa_builder.add_volumes(volumes_vec);
            ohlcv_soa_builder.finish()
        };

        // Build the root OHLCVListSOa object containing the OHLCVSOa
        let ohlcv_list_soa = {
            let mut list_builder = ohlcv_soa_generated::OHLCVListSOABuilder::new(&mut builder);
            list_builder.add_data(ohlcv_soa);
            list_builder.finish()
        };

        builder.finish(ohlcv_list_soa, None);
        builder.finished_data().to_vec()
    }
}

// --- /SOA Builder Implementation ---

/// Processes CSV records, accumulates raw data, and builds index structures.
///
/// This function reads OHLCV records from a CSV reader, parses datetime strings
/// into Unix timestamps, and populates index collections (time, daily, timeframe).
/// Crucially, it accumulates the raw OHLCV data into a `Vec<ProcessedRecord>`,
/// which is then used by `save_flatbuffer` to create either AOS or SOA FlatBuffers.
///
/// The `timeframe_index` is generated to include ALL possible timeframe boundaries
/// within the data's time range, ensuring no gaps for resampling purposes, even if
/// some boundaries have no corresponding raw data.
///
/// # Arguments
/// * `reader` - CSV reader for input data.
/// * `time_index` - Output vector to store timestamp-to-index mappings.
/// * `daily_index` - Output vector to store daily OHLCV ranges.
/// * `tf_index_map` - Output map to store timeframe-specific timestamps.
/// * `raw_data` - Output vector to store raw ProcessedRecord data for FlatBuffer creation.
///
/// # Returns
/// * `anyhow::Result<()>` - Success or an error if processing fails.
///
/// # Errors
/// * If datetime parsing fails.
/// * If CSV deserialization fails.
fn process_csv_records<R: std::io::Read>(
    reader: &mut csv::Reader<R>,
    time_index: &mut Vec<index::TimeIndexEntry>,
    daily_index: &mut Vec<index::DailyIndexEntry>,
    tf_index_map: &mut std::collections::HashMap<String, Vec<u64>>,
    raw_data: &mut Vec<ProcessedRecord>
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
        ("1d", 86400),
    ];

    // --- Collect raw data and basic indices first ---
    let mut all_timestamps = Vec::new(); // Collect all timestamps for min/max calculation

    for result in reader.deserialize::<CsvRecord>() {
        let record: CsvRecord = result?;
        let date_str = &record.date;
        let time_str = &record.time;
        let dt_str = format!("{} {}", date_str, time_str);
        let dt = chrono::NaiveDateTime::parse_from_str(&dt_str, "%Y%m%d %H%M%S")
        .map_err(|e| anyhow::anyhow!("Failed to parse datetime: {}", e))?;
        let timestamp = dt.and_utc().timestamp() as u64;

        let processed_record = ProcessedRecord {
            timestamp,
            open: record.open,
            high: record.high,
            low: record.low,
            close: record.close,
            vol: record.vol,
        };
        raw_data.push(processed_record);
        all_timestamps.push(timestamp);

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
    }

    // last day
    if let Some(day) = current_day.take() {
        daily_index.push(index::DailyIndexEntry { 
            date: day,
            start_index: day_start_index,
            end_index: index_in_vector - 1,
        });
    }

    // --- Generate comprehensive timeframe indices ---
    if !all_timestamps.is_empty() {
        let min_ts = *all_timestamps.iter().min().unwrap();
        let max_ts = *all_timestamps.iter().max().unwrap();

        for (tf_name, tf_sec) in &supported_timeframes {
            let start_boundary = (min_ts / tf_sec) * tf_sec; // First boundary >= min_ts
            let end_boundary = (max_ts / tf_sec) * tf_sec;   // Last boundary <= max_ts

            let mut timeframe_timestamps = Vec::new();
            let mut current_boundary = start_boundary;

            // Populate all boundaries within the range
            while current_boundary <= end_boundary {
                timeframe_timestamps.push(current_boundary);
                current_boundary += tf_sec;
            }

            tf_index_map.insert(tf_name.to_string(), timeframe_timestamps);
        }
    }

    anyhow::Ok(())
}

/// Converts CSV data to a FlatBuffer binary file (.bin) in AOS or SOA format and generates index data.
///
/// This function orchestrates the conversion process based on the specified `storage_format`:
/// 1. Opens and reads the input CSV file.
/// 2. Initializes index collections.
/// 3. Calls `process_csv_records` to accumulate raw data and populate indices.
/// 4. Based on `storage_format`, creates the FlatBuffer data (either AOS or SOA).
/// 5. Writes the binary FlatBuffer data to the output file.
/// 6. Packages the generated index data for later use.
///
/// # Arguments
/// * `input_dir_path` - Path to the input CSV file.
/// * `output_path` - Path for the output .bin file.
/// * `storage_format` - The desired FlatBuffer storage format (AOS or SOA).
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
    storage_format: cli::StorageFormat,
) -> anyhow::Result<ProcessedData> {
    let input_file = std::fs::File::open(input_dir_path)?;
    let mut reader = csv::ReaderBuilder::new().has_headers(true).from_reader(input_file);  
    
    let mut time_index: Vec<index::TimeIndexEntry> = Vec::new();
    let mut daily_index: Vec<index::DailyIndexEntry> = Vec::new();
    let mut tf_index_map: std::collections::HashMap<String, Vec<u64>> = std::collections::HashMap::new();
    let mut raw_data = Vec::new();

    // Accumulate raw data and indices
    process_csv_records(
        &mut reader,
        &mut time_index,
        &mut daily_index,
        &mut tf_index_map,
        &mut raw_data,
    )?;

    // --- Create FlatBuffer Data based on Storage Format ---
    let flatbuffer_data = match storage_format {
        cli::StorageFormat::Aos => {
            // --- AOS Logic ---
            let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024 * 1024);
            let mut ohlcv_offsets = Vec::with_capacity(raw_data.len());
            for record in &raw_data {
                let ohlcv_args = ohlcv_generated::OHLCVArgs {
                    timestamp: record.timestamp,
                    open: record.open,
                    high: record.high,
                    low: record.low,
                    close: record.close,
                    volume: record.vol,
                };
                let ohlcv = ohlcv_generated::OHLCV::create(&mut builder, &ohlcv_args);
                ohlcv_offsets.push(ohlcv);
            }

            let items = builder.create_vector(&ohlcv_offsets);
            let ohlcv_list = {
                let mut list_builder = ohlcv_generated::OHLCVListBuilder::new(&mut builder);
                list_builder.add_items(items);
                list_builder.finish()
            };
            builder.finish(ohlcv_list, None);
            builder.finished_data().to_vec()
        }
        cli::StorageFormat::Soa => {
            // --- SOA Logic ---
            let mut soa_builder = SOABuilder::new();
            for record in &raw_data {
                soa_builder.add_ohlcv(
                    record.timestamp,
                    record.open,
                    record.high,
                    record.low,
                    record.close,
                    record.vol
                );
            }
            soa_builder.finish_buffer()
        }
    };

    // --- /Create FlatBuffer Data ---

    // Write the generated FlatBuffer binary data to the output file
    std::fs::write(output_path.as_ref(), flatbuffer_data)?;

    // Package the generated index data
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
    let idx_path = std::path::Path::new(output_path.as_ref()).with_extension("idx");  
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
/// It delegates to `save_flatbuffer` for the core logic (reading CSV, creating FlatBuffer based on format)
/// and `save_index` for persisting the generated indices. It's designed to be called from `main.rs`
/// or other modules needing to trigger the conversion.
///
/// # Arguments
/// * `input_dir_path` - Path to the input CSV file.
/// * `output_path` - Path for the output .bin file (e.g., filename.aos.bin or filename.soa.bin).
/// * `storage_format` - The desired FlatBuffer storage format (AOS or SOA).
///
/// # Returns
/// * `anyhow::Result<()>` - Success or an error if conversion or saving fails.
///
/// # Errors
/// * Propagates errors from `save_flatbuffer` or `save_index`.
pub fn convert_csv_to_flatbuffer<P: AsRef<std::path::Path>>(input_dir_path: P, output_path: P, storage_format: cli::StorageFormat) -> anyhow::Result<()> {
    let processed_data = save_flatbuffer(
        input_dir_path.as_ref(),
        output_path.as_ref(),
        storage_format.clone(),
    )?;
    save_index(
        &processed_data.time_index,
        &processed_data.daily_index,
        &processed_data.timeframe_index,
        output_path.as_ref(),
    )?;

    anyhow::Ok(())
}
