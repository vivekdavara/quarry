//! `tpch-gen`: writes the TPC-H-style tables as Parquet files.
//!
//! ```text
//! cargo run --release -p quarry-tpch -- SCALE DIR
//! ```
//!
//! Writes `DIR/<table>.parquet` for each of the seven tables and prints each file's rows and size. Scale 1 is about
//! 6 million lines; scale 0.01 about 60,000.

use std::process::ExitCode;
use std::time::Instant;

use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, scale, dir] = args.as_slice() else {
        eprintln!("usage: tpch-gen SCALE DIR");
        return ExitCode::from(2);
    };
    let Ok(scale) = scale.parse::<f64>() else {
        eprintln!("error: scale factor {scale:?} is not a number");
        return ExitCode::from(2);
    };
    let start = Instant::now();
    match quarry_tpch::generate(scale, dir.as_ref()) {
        Ok(paths) => {
            for path in paths {
                let file = std::fs::File::open(&path).expect("just written");
                let bytes = file.metadata().expect("just written").len();
                let rows = SerializedFileReader::new(file)
                    .expect("just written")
                    .metadata()
                    .file_metadata()
                    .num_rows();
                println!("{}: {rows} rows, {bytes} bytes", path.display());
            }
            println!("{:.1} s", start.elapsed().as_secs_f64());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}
