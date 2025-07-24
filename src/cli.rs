/// Structure representing command-line arguments.
#[derive(Debug)]
pub struct Args {
    pub input: std::path::PathBuf,
    pub output: std::path::PathBuf,
    pub threads: Option<usize>,
    pub check: bool,
    pub resample: Option<String>,
}

/// Command-line arguments parser using Clap.
///
/// Supports input/output paths, threading, and optional resampling with validation.
impl Args {
    /// Parses command-line arguments using `clap`.
    ///
    /// # Returns
    /// * `Args` - Struct containing parsed arguments.
    ///
    /// # Errors
    /// * If required arguments are missing or invalid.    
    pub fn parse() -> Self {
        let matches = clap::Command::new("csv_to_flatbuffer")
            .version("0.1.0")
            .author("AndyDar")
            .about("Convert CSV/TXT files to flatbuffer")
            .arg(
                clap::Arg::new("input")
                    .short('i')
                    .long("input")
                    .help("Path to input directory with CSV/TXT files")
                    .required(true)
                    .num_args(1),
            )
            .arg(
                clap::Arg::new("output")
                .short('o')
                .long("output")
                .help("Path to output directory for flatbuffer files")
                .required(true)
                .num_args(1),
            )
            .arg(
                clap::Arg::new("threads")
                .short('t')
                .long("threads")
                .help("Number of threads to use (default: all available)")
                .num_args(1)
                .value_parser(clap::builder::ValueParser::new(parse_usize_positive)),
            )
            .arg(
                clap::Arg::new("check")
                .short('c')
                .long("check")
                .help("After conversion, read .bin file and print first 5 rows as DataFrame")
                .required(false)
                .action(clap::ArgAction::SetTrue)
            )
            .arg(
                clap::Arg::new("resample")
                .short('r')
                .long("resample")
                .help("Resample data to specified timeframe. Available: 1min, 2min, 3min, 4min, 5min, 1d")
                .value_parser(["1min", "2min", "3min", "4min", "5min", "1d"])
                .required(false)
                .num_args(1)
                .requires("check")
            )
            .get_matches();

        Args {
            input: std::path::PathBuf::from(matches.get_one::<String>("input").unwrap()),
            output: std::path::PathBuf::from(matches.get_one::<String>("output").unwrap()),
            threads: matches.get_one::<usize>("threads").cloned(),
            check: matches.get_flag("check"),
            resample: matches.get_one::<String>("resample").cloned(),
        }
    }
}

/// Validates that the number of threads is a positive integer.
///
/// # Arguments
/// * `s` - String representation of the number of threads.
///
/// # Returns
/// * `Result<usize>` - Validated number of threads.
fn parse_usize_positive(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(0) => Err("Must be a positive integer".to_string()),
        Ok(n) => Ok(n),
        Err(e) => Err(format!("Not a valid number: {}", e)),
    }
}
