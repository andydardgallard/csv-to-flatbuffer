// This file is executed at compile time by Cargo.
// It runs the FlatBuffers compiler (`flatc`) to generate Rust code from `.fbs` schema files.
//
// Purpose:
// - Automatically generate `src/ohlcv_generated.rs` from `ohlcv.fbs`
// - Keep generated code in sync with schema changes
// - Avoid manual regeneration steps

use flatc_rust; // Import the `flatc-rust` crate which wraps the `flatc` CLI tool

fn main() {
    // Run the FlatBuffers compiler with specified arguments
    flatc_rust::run(flatc_rust::Args {
        // List of input `.fbs` schema files to compile
        inputs: &[std::path::Path::new("ohlcv.fbs")],

        // Output directory for generated Rust code
        out_dir: std::path::Path::new("src"),

        // Use default settings for all other options (e.g., language=rust, no suffix, etc.)
        ..Default::default()
    }).unwrap(); // Panic on failure — stops compilation if schema is invalid
}
