//! Writes a synthetic `orders` table to a Parquet file, for measuring queries on more than a handful of rows.
//!
//! ```text
//! cargo run --release -p quarry --example gen_orders -- ROWS PATH
//! ```
//!
//! Columns: `order_id` (1..=ROWS), `customer` (one of 10,000, skewed: low numbers are more frequent), `status`
//! (shipped 70%, pending 20%, cancelled 10%), `total` (0.01 to 1,000.00, NULL 5% of the time), `qty` (1 to 10) and
//! `placed` (a day in 2020-2023). The data come from a fixed-seed generator, so the same ROWS give the same file.

use std::fs::File;
use std::sync::Arc;

use quarry::arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::parquet::arrow::ArrowWriter;
use quarry::parquet::basic::{Compression, ZstdLevel};
use quarry::parquet::file::properties::WriterProperties;

/// xorshift64*: small, fast and deterministic; plenty for synthetic data.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const BATCH: usize = 65_536;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (rows, path) = match args.as_slice() {
        [_, rows, path] => (rows.parse::<usize>().expect("ROWS is a number"), path),
        _ => {
            eprintln!("usage: gen_orders ROWS PATH");
            std::process::exit(2);
        }
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("total", DataType::Float64, true),
        Field::new("qty", DataType::Int64, false),
        Field::new("placed", DataType::Date32, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let file = File::create(path).expect("can create the file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let customers: Vec<String> = (0..10_000).map(|i| format!("c{i:05}")).collect();
    let epoch_2020 = 18_262; // 2020-01-01
    let mut next_id = 1i64;
    let mut left = rows;
    while left > 0 {
        let n = left.min(BATCH);
        left -= n;
        let ids: Int64Array = (0..n as i64).map(|i| next_id + i).collect();
        next_id += n as i64;
        // The smaller of two uniform draws: customer i is about twice as likely as customer 10,000 - i.
        let customer: StringArray = (0..n)
            .map(|_| {
                let c = rng.below(10_000).min(rng.below(10_000));
                Some(customers[c as usize].as_str())
            })
            .collect();
        let status: StringArray = (0..n)
            .map(|_| {
                Some(match rng.below(10) {
                    0..=6 => "shipped",
                    7 | 8 => "pending",
                    _ => "cancelled",
                })
            })
            .collect();
        let total: Float64Array = (0..n)
            .map(|_| (rng.below(20) != 0).then(|| (rng.below(100_000) + 1) as f64 / 100.0))
            .collect();
        let qty: Int64Array = (0..n).map(|_| rng.below(10) as i64 + 1).collect();
        let placed =
            Date32Array::from_iter_values((0..n).map(|_| epoch_2020 + rng.below(1461) as i32));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(ids),
            Arc::new(customer),
            Arc::new(status),
            Arc::new(total),
            Arc::new(qty),
            Arc::new(placed),
        ];
        writer
            .write(&RecordBatch::try_new(schema.clone(), columns).unwrap())
            .unwrap();
    }
    writer.close().unwrap();
}
