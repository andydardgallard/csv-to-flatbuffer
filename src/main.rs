#[allow(dead_code)]
#[allow(unused_imports)]
#[allow(clippy::all)]
#[allow(unsafe_op_in_unsafe_fn)]
mod ohlcv_generated;

#[allow(dead_code)]
#[allow(unused_imports)]
#[allow(clippy::all)]
#[allow(unsafe_op_in_unsafe_fn)]
mod ohlcv_soa_generated;

// Minimal imports required for the main logic
mod cli;
mod utils;
mod index;
mod resample;
mod progress;
mod csv_processor;
mod file_processing;
mod read_flatbuffers;

/// Main entry point of the application.
///
/// This function orchestrates the entire workflow:
/// 1. Parses command-line arguments.
/// 2. Validates input/output paths.
/// 3. Determines the number of threads to use.
/// 4. Converts CSV files to FlatBuffer binary format.
/// 5. Optionally reads and displays the first few bars from the output.
///
/// # Returns
///
/// * `anyhow::Result<()>` - Success or an error if any step fails.
fn main() -> anyhow::Result<()> {
    let total_start = std::time::Instant::now();
    let args = cli::Args::parse();
    println!("Start conversion...");

    file_processing::check_path(&args.input)?;
    file_processing::ensure_parent_dir_exist(&args.output)?;

    let effective_threads = match args.threads {
        Some(n) if n > 0 => {
            let max_threads = num_cpus::get();
            if n > max_threads {
                println!("⚠️ Warning: Limiting thread count to {} (max available)", max_threads);
                max_threads
            } else { n }
        }
        Some(_) => return Err(anyhow::anyhow!("Number of threads must be a positive integer")),
        None => {
            let default_threads = rayon::current_num_threads();
            default_threads
        }
    };
    println!("🚀 Using {} thread(s)", effective_threads);

    if let Some(n) = args.threads {
        let local_pool = utils::configure_thread_pool(n)?;
        local_pool.install(|| progress::process_files(&args.input, &args.output, args.storage_format))?;
    } else {
        progress::process_files(&args.input, &args.output, args.storage_format)?;
    }

    let duration = total_start.elapsed();
    println!(
        "✅ Conversion completed in {:?} seconds",
        duration.as_secs_f64()
    );

    if args.check {
        println!("Start reading...");
        let start = std::time::Instant::now();

        if let Some(n) = args.threads {
            let local_pool = utils::configure_thread_pool(n)?;
            local_pool.install(||  read_flatbuffers::read_flatbuffers(args.output, args.resample))?;
        } else {
            read_flatbuffers::read_flatbuffers(args.output, args.resample)?;
        }
        println!(
            "✅ Reading files complete in {:?} seconds", 
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}
