//! TPC-H-style data for testing and measuring Quarry.
//!
//! [`generate`] writes seven tables of the TPC-H schema (`region`, `nation`, `supplier`, `customer`, `part`, `orders`,
//! `lineitem`; not `partsupp`, which none of the queries here reads) as Parquet files, following the value rules of
//! the TPC-H specification's data generator, simplified. The rows come from fixed-seed generators, so a scale factor
//! always gives byte-identical files, on every platform.
//!
//! What is the same as TPC-H: the columns and their meaning, the row counts per scale factor (10,000 suppliers,
//! 150,000 customers, 200,000 parts, 1,500,000 orders and 1 to 7 lines per order at scale 1), the 25 nations and 5
//! regions, the lists of segments, priorities, ship modes, instructions, part types, brands and containers, and the
//! rules that tie columns together: dates (ship 1-121 days after the order, commit 30-90, receipt 1-30 after the
//! ship), return flags and line statuses against the "current date" 1995-06-17, an order's status from its lines'
//! statuses, its total price from its lines, a part's retail price from its key, and only two in three customers
//! placing orders.
//!
//! What is simpler (so results aren't comparable with published TPC-H numbers): prices, quantities, discounts and taxes
//! are `DOUBLE` (exact in cents when written, not in arithmetic), not `DECIMAL`; order keys are dense (1 to the number
//! of orders, where TPC-H leaves gaps); a line's part and supplier are independent uniform draws; names, addresses and
//! comments come from short fixed vocabularies instead of the specification's text grammar.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;

/// Generation's errors: a bad scale factor, or the file system's or Parquet writer's.
pub type Result<T> = std::result::Result<T, ParquetError>;

/// The tables [`generate`] writes, in the order it writes them.
pub const TABLES: [&str; 7] = [
    "region", "nation", "supplier", "customer", "part", "orders", "lineitem",
];

/// Rows per Parquet row group: small enough that a scale-1 `lineitem` has about 46 of them for row-group pruning.
pub const ROW_GROUP_ROWS: usize = 1 << 17;

/// Orders generated per batch (with their 1-7 lines each).
const ORDERS_PER_BATCH: i64 = 16_384;

/// Days since 1970-01-01 of TPC-H's dates: the first and last order date's bounds (1992-01-01, 1998-12-31) and the
/// "current date" (1995-06-17) that decides return flags and line statuses.
pub const START_DATE: i32 = 8_035;
pub const END_DATE: i32 = 10_591;
pub const CURRENT_DATE: i32 = 9_298;

/// How many rows each scaled table has at a scale factor: TPC-H's counts at scale 1, times the factor, rounded, and
/// at least 1. `lineitem` has 1-7 lines per order (4 on average). `region` and `nation` always have 5 and 25.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub supplier: i64,
    pub customer: i64,
    pub part: i64,
    pub orders: i64,
    /// Clerks an order's `o_clerk` is drawn from (1,000 at scale 1).
    pub clerks: i64,
}

impl Counts {
    /// The counts at a scale factor; refuses a factor that isn't a positive finite number, or one over 1,000 (6
    /// billion lines: past that, keys would still fit but the files wouldn't fit on any disk this is meant for).
    pub fn at(scale: f64) -> Result<Counts> {
        if !(scale.is_finite() && scale > 0.0 && scale <= 1000.0) {
            return Err(ParquetError::General(format!(
                "scale factor {scale}: expected a number above 0 and at most 1000"
            )));
        }
        let scaled = |base: f64| ((base * scale).round() as i64).max(1);
        Ok(Counts {
            supplier: scaled(10_000.0),
            customer: scaled(150_000.0),
            part: scaled(200_000.0),
            orders: scaled(1_500_000.0),
            clerks: scaled(1_000.0),
        })
    }
}

/// Writes every table in [`TABLES`] to `<dir>/<table>.parquet` (creating `dir` if needed) and returns the paths in
/// that order. Existing files of those names are replaced.
pub fn generate(scale: f64, dir: &Path) -> Result<Vec<PathBuf>> {
    let counts = Counts::at(scale)?;
    std::fs::create_dir_all(dir)?;
    let path = |table: &str| dir.join(format!("{table}.parquet"));
    write_table(&path("region"), region())?;
    write_table(&path("nation"), nation())?;
    write_table(&path("supplier"), supplier(&counts))?;
    write_table(&path("customer"), customer(&counts))?;
    write_table(&path("part"), part(&counts))?;
    write_orders_and_lines(&counts, &path("orders"), &path("lineitem"))?;
    Ok(TABLES.iter().map(|t| path(t)).collect())
}

/// A table's schema. Every column is non-nullable; keys and counts are BIGINT, money and quantities DOUBLE, dates
/// DATE, the rest VARCHAR.
pub fn schema(table: &str) -> Option<SchemaRef> {
    use DataType::{Date32 as D, Float64 as F, Int64 as I, Utf8 as S};
    let columns: &[(&str, DataType)] = match table {
        "region" => &[("r_regionkey", I), ("r_name", S), ("r_comment", S)],
        "nation" => &[
            ("n_nationkey", I),
            ("n_name", S),
            ("n_regionkey", I),
            ("n_comment", S),
        ],
        "supplier" => &[
            ("s_suppkey", I),
            ("s_name", S),
            ("s_address", S),
            ("s_nationkey", I),
            ("s_phone", S),
            ("s_acctbal", F),
            ("s_comment", S),
        ],
        "customer" => &[
            ("c_custkey", I),
            ("c_name", S),
            ("c_address", S),
            ("c_nationkey", I),
            ("c_phone", S),
            ("c_acctbal", F),
            ("c_mktsegment", S),
            ("c_comment", S),
        ],
        "part" => &[
            ("p_partkey", I),
            ("p_name", S),
            ("p_mfgr", S),
            ("p_brand", S),
            ("p_type", S),
            ("p_size", I),
            ("p_container", S),
            ("p_retailprice", F),
            ("p_comment", S),
        ],
        "orders" => &[
            ("o_orderkey", I),
            ("o_custkey", I),
            ("o_orderstatus", S),
            ("o_totalprice", F),
            ("o_orderdate", D),
            ("o_orderpriority", S),
            ("o_clerk", S),
            ("o_shippriority", I),
            ("o_comment", S),
        ],
        "lineitem" => &[
            ("l_orderkey", I),
            ("l_partkey", I),
            ("l_suppkey", I),
            ("l_linenumber", I),
            ("l_quantity", F),
            ("l_extendedprice", F),
            ("l_discount", F),
            ("l_tax", F),
            ("l_returnflag", S),
            ("l_linestatus", S),
            ("l_shipdate", D),
            ("l_commitdate", D),
            ("l_receiptdate", D),
            ("l_shipinstruct", S),
            ("l_shipmode", S),
            ("l_comment", S),
        ],
        _ => return None,
    };
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, ty)| Field::new(*name, ty.clone(), false))
        .collect();
    Some(Arc::new(Schema::new(fields)))
}

/// A part's retail price in cents, from its key alone (TPC-H 4.2.3): 900.00 to 2,098.99.
pub fn retail_price_cents(partkey: i64) -> i64 {
    90_000 + (partkey / 10) % 20_001 + 100 * (partkey % 1_000)
}

/// A line's contribution to its order's `o_totalprice`, in cents: `extendedprice * (1 + tax) * (1 - discount)` with
/// tax and discount in percent, rounded half up to a cent.
pub fn line_total_cents(extended_cents: i64, discount_pct: i64, tax_pct: i64) -> i64 {
    (extended_cents * (100 - discount_pct) * (100 + tax_pct) + 5_000) / 10_000
}

pub const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

/// The 25 nations and their regions (indexes into [`REGIONS`]), as in TPC-H.
pub const NATIONS: [(&str, i64); 25] = [
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];

pub const SEGMENTS: [&str; 5] = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "HOUSEHOLD",
    "MACHINERY",
];
pub const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
pub const SHIP_MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
pub const INSTRUCTIONS: [&str; 4] = [
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN",
];
/// A part's type is one word from each list (150 types).
pub const TYPE_WORDS: [&[&str]; 3] = [
    &["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"],
    &["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"],
    &["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"],
];
/// A container is one word from each list (40 containers).
pub const CONTAINER_WORDS: [&[&str]; 2] = [
    &["SM", "LG", "MED", "JUMBO", "WRAP"],
    &["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"],
];
/// Words for part names (five each); TPC-H draws from 92 colours.
const COLOURS: [&str; 20] = [
    "almond",
    "antique",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanched",
    "blue",
    "blush",
    "brown",
    "burlywood",
    "chartreuse",
    "chiffon",
    "chocolate",
    "coral",
    "cornflower",
    "cornsilk",
    "cream",
    "cyan",
];
/// Words for comments.
const WORDS: [&str; 24] = [
    "furiously",
    "quickly",
    "carefully",
    "blithely",
    "slyly",
    "final",
    "regular",
    "special",
    "express",
    "ironic",
    "pending",
    "bold",
    "even",
    "silent",
    "packages",
    "requests",
    "accounts",
    "deposits",
    "foxes",
    "ideas",
    "theodolites",
    "pinto",
    "beans",
    "instructions",
];
const ADDRESS_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,";

/// xorshift64*: small, fast and the same on every platform; plenty for synthetic data. Each table has its own seed,
/// so adding a column to one table doesn't change another.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `lo..=hi` (up to a modulo bias far too small to matter here).
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }

    fn pick<'a>(&mut self, list: &[&'a str]) -> &'a str {
        list[self.range(0, list.len() as i64 - 1) as usize]
    }

    /// `min..=max` words from `list`, separated by spaces.
    fn words(&mut self, list: &[&str], min: i64, max: i64) -> String {
        let n = self.range(min, max);
        let mut out = String::new();
        for i in 0..n {
            if i > 0 {
                out.push(' ');
            }
            out.push_str(self.pick(list));
        }
        out
    }

    fn comment(&mut self) -> String {
        self.words(&WORDS, 2, 6)
    }

    fn address(&mut self) -> String {
        let n = self.range(10, 25);
        (0..n)
            .map(|_| ADDRESS_CHARS[self.range(0, ADDRESS_CHARS.len() as i64 - 1) as usize] as char)
            .collect()
    }

    /// TPC-H's phone format: the nation's country code (key + 10), then three groups of digits.
    fn phone(&mut self, nation: i64) -> String {
        format!(
            "{}-{}-{}-{}",
            nation + 10,
            self.range(100, 999),
            self.range(100, 999),
            self.range(1_000, 9_999)
        )
    }

    /// An account balance: -999.99 to 9,999.99.
    fn balance(&mut self) -> f64 {
        cents(self.range(-99_999, 999_999))
    }
}

fn cents(c: i64) -> f64 {
    c as f64 / 100.0
}

fn int(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn float(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

fn date(values: Vec<i32>) -> ArrayRef {
    Arc::new(Date32Array::from(values))
}

fn text<S: AsRef<str>>(values: Vec<S>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn batch(table: &str, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    let schema = schema(table).expect("a known table");
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn writer(path: &Path, table: &str) -> Result<ArrowWriter<File>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();
    let file = File::create(path)?;
    ArrowWriter::try_new(file, schema(table).expect("a known table"), Some(props))
}

fn write_table(path: &Path, batch: Result<RecordBatch>) -> Result<()> {
    let batch = batch?;
    let table = path
        .file_stem()
        .and_then(|s| s.to_str())
        .expect("named after its table");
    let mut writer = writer(path, table)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn region() -> Result<RecordBatch> {
    let mut rng = Rng(0x5EED_0001_9E37_79B9);
    batch(
        "region",
        vec![
            int((0..5).collect()),
            text(REGIONS.to_vec()),
            text((0..5).map(|_| rng.comment()).collect()),
        ],
    )
}

fn nation() -> Result<RecordBatch> {
    let mut rng = Rng(0x5EED_0002_9E37_79B9);
    batch(
        "nation",
        vec![
            int((0..25).collect()),
            text(NATIONS.iter().map(|(name, _)| *name).collect()),
            int(NATIONS.iter().map(|(_, region)| *region).collect()),
            text((0..25).map(|_| rng.comment()).collect()),
        ],
    )
}

fn supplier(counts: &Counts) -> Result<RecordBatch> {
    let mut rng = Rng(0x5EED_0003_9E37_79B9);
    let n = counts.supplier;
    let (mut address, mut nation, mut phone, mut balance, mut comment) =
        (vec![], vec![], vec![], vec![], vec![]);
    for _ in 0..n {
        address.push(rng.address());
        let k = rng.range(0, 24);
        nation.push(k);
        phone.push(rng.phone(k));
        balance.push(rng.balance());
        comment.push(rng.comment());
    }
    batch(
        "supplier",
        vec![
            int((1..=n).collect()),
            text((1..=n).map(|k| format!("Supplier#{k:09}")).collect()),
            text(address),
            int(nation),
            text(phone),
            float(balance),
            text(comment),
        ],
    )
}

fn customer(counts: &Counts) -> Result<RecordBatch> {
    let mut rng = Rng(0x5EED_0004_9E37_79B9);
    let n = counts.customer;
    let (mut address, mut nation, mut phone, mut balance, mut segment, mut comment) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for _ in 0..n {
        address.push(rng.address());
        let k = rng.range(0, 24);
        nation.push(k);
        phone.push(rng.phone(k));
        balance.push(rng.balance());
        segment.push(rng.pick(&SEGMENTS));
        comment.push(rng.comment());
    }
    batch(
        "customer",
        vec![
            int((1..=n).collect()),
            text((1..=n).map(|k| format!("Customer#{k:09}")).collect()),
            text(address),
            int(nation),
            text(phone),
            float(balance),
            text(segment),
            text(comment),
        ],
    )
}

fn part(counts: &Counts) -> Result<RecordBatch> {
    let mut rng = Rng(0x5EED_0005_9E37_79B9);
    let n = counts.part;
    let (mut name, mut mfgr, mut brand, mut ty, mut size, mut container, mut comment) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for _ in 0..n {
        name.push(rng.words(&COLOURS, 5, 5));
        let m = rng.range(1, 5);
        mfgr.push(format!("Manufacturer#{m}"));
        brand.push(format!("Brand#{m}{}", rng.range(1, 5)));
        ty.push(format!(
            "{} {} {}",
            rng.pick(TYPE_WORDS[0]),
            rng.pick(TYPE_WORDS[1]),
            rng.pick(TYPE_WORDS[2])
        ));
        size.push(rng.range(1, 50));
        container.push(format!(
            "{} {}",
            rng.pick(CONTAINER_WORDS[0]),
            rng.pick(CONTAINER_WORDS[1])
        ));
        comment.push(rng.comment());
    }
    batch(
        "part",
        vec![
            int((1..=n).collect()),
            text(name),
            text(mfgr),
            text(brand),
            text(ty),
            int(size),
            text(container),
            float((1..=n).map(|k| cents(retail_price_cents(k))).collect()),
            text(comment),
        ],
    )
}

/// One batch's worth of `orders` columns.
#[derive(Default)]
struct Orders {
    key: Vec<i64>,
    customer: Vec<i64>,
    status: Vec<&'static str>,
    total: Vec<f64>,
    date: Vec<i32>,
    priority: Vec<&'static str>,
    clerk: Vec<String>,
    comment: Vec<String>,
}

/// One batch's worth of `lineitem` columns.
#[derive(Default)]
struct Lines {
    order: Vec<i64>,
    part: Vec<i64>,
    supplier: Vec<i64>,
    number: Vec<i64>,
    quantity: Vec<f64>,
    extended: Vec<f64>,
    discount: Vec<f64>,
    tax: Vec<f64>,
    return_flag: Vec<&'static str>,
    status: Vec<&'static str>,
    ship: Vec<i32>,
    commit: Vec<i32>,
    receipt: Vec<i32>,
    instruct: Vec<&'static str>,
    mode: Vec<&'static str>,
    comment: Vec<String>,
}

/// `orders` and `lineitem` together, since an order's status and total come from its lines. One generator runs
/// through all orders in key order, so the batch size doesn't change the data.
fn write_orders_and_lines(counts: &Counts, orders_path: &Path, lines_path: &Path) -> Result<()> {
    let mut rng = Rng(0x5EED_0006_9E37_79B9);
    let mut orders_writer = writer(orders_path, "orders")?;
    let mut lines_writer = writer(lines_path, "lineitem")?;
    let mut first = 1;
    while first <= counts.orders {
        let last = (first + ORDERS_PER_BATCH - 1).min(counts.orders);
        let mut o = Orders::default();
        let mut l = Lines::default();
        for key in first..=last {
            order(&mut rng, counts, key, &mut o, &mut l);
        }
        orders_writer.write(&batch(
            "orders",
            vec![
                int(o.key),
                int(o.customer),
                text(o.status),
                float(o.total),
                date(o.date),
                text(o.priority),
                text(o.clerk),
                int(vec![0; (last - first + 1) as usize]),
                text(o.comment),
            ],
        )?)?;
        lines_writer.write(&batch(
            "lineitem",
            vec![
                int(l.order),
                int(l.part),
                int(l.supplier),
                int(l.number),
                float(l.quantity),
                float(l.extended),
                float(l.discount),
                float(l.tax),
                text(l.return_flag),
                text(l.status),
                date(l.ship),
                date(l.commit),
                date(l.receipt),
                text(l.instruct),
                text(l.mode),
                text(l.comment),
            ],
        )?)?;
        first = last + 1;
    }
    orders_writer.close()?;
    lines_writer.close()?;
    Ok(())
}

/// Appends one order and its lines.
fn order(rng: &mut Rng, counts: &Counts, key: i64, o: &mut Orders, l: &mut Lines) {
    // Only customers whose key isn't a multiple of 3 place orders (a third never do), as in TPC-H.
    let customer = loop {
        let c = rng.range(1, counts.customer);
        if counts.customer < 3 || c % 3 != 0 {
            break c;
        }
    };
    let order_date = rng.range(START_DATE as i64, END_DATE as i64 - 151) as i32;
    let lines = rng.range(1, 7);
    let mut total = 0;
    let mut shipped = 0;
    for number in 1..=lines {
        let part = rng.range(1, counts.part);
        let quantity = rng.range(1, 50);
        let extended = quantity * retail_price_cents(part);
        let discount = rng.range(0, 10);
        let tax = rng.range(0, 8);
        let ship = order_date + rng.range(1, 121) as i32;
        let commit = order_date + rng.range(30, 90) as i32;
        let receipt = ship + rng.range(1, 30) as i32;
        let return_flag = if receipt <= CURRENT_DATE {
            if rng.range(0, 1) == 0 { "R" } else { "A" }
        } else {
            "N"
        };
        let status = if ship > CURRENT_DATE { "O" } else { "F" };
        shipped += (status == "F") as i64;
        total += line_total_cents(extended, discount, tax);
        l.order.push(key);
        l.part.push(part);
        l.supplier.push(rng.range(1, counts.supplier));
        l.number.push(number);
        l.quantity.push(quantity as f64);
        l.extended.push(cents(extended));
        l.discount.push(discount as f64 / 100.0);
        l.tax.push(tax as f64 / 100.0);
        l.return_flag.push(return_flag);
        l.status.push(status);
        l.ship.push(ship);
        l.commit.push(commit);
        l.receipt.push(receipt);
        l.instruct.push(rng.pick(&INSTRUCTIONS));
        l.mode.push(rng.pick(&SHIP_MODES));
        l.comment.push(rng.comment());
    }
    o.key.push(key);
    o.customer.push(customer);
    o.status.push(match shipped {
        0 => "O",
        s if s == lines => "F",
        _ => "P",
    });
    o.total.push(cents(total));
    o.date.push(order_date);
    o.priority.push(rng.pick(&PRIORITIES));
    o.clerk
        .push(format!("Clerk#{:09}", rng.range(1, counts.clerks)));
    o.comment.push(rng.comment());
}

/// One of the TPC-H queries Quarry runs, with DuckDB 2.0.0's answer over the scale-[`ANSWER_SCALE`] tables.
#[derive(Debug, Clone, Copy)]
pub struct Query {
    /// `q01` and so on, after TPC-H's numbers.
    pub name: &'static str,
    pub title: &'static str,
    pub sql: &'static str,
    /// CSV with a header: `scripts/tpch_answers.py` wrote it from DuckDB's result (floats as Python's `repr`).
    pub answer: &'static str,
}

/// The scale factor of the tables [`Query::answer`] was computed over.
pub const ANSWER_SCALE: f64 = 0.01;

macro_rules! query {
    ($name:literal, $title:literal) => {
        Query {
            name: $name,
            title: $title,
            sql: include_str!(concat!("../queries/", $name, ".sql")),
            answer: include_str!(concat!("../answers/", $name, ".csv")),
        }
    };
}

/// The TPC-H queries that need nothing Quarry lacks (subqueries, `EXTRACT`, `INTERVAL`, views): 8 of the 22, written
/// as the specification writes them with its validation parameters, except that dates are literals (`DATE
/// '1998-12-01' - 90` for `- interval '90' day`), Q6's discount bounds are written out (see its file), and Q3 and Q10
/// order by their keys after the specification's order, so that ties come out in one order.
pub const QUERIES: [Query; 8] = [
    query!("q01", "pricing summary report"),
    query!("q03", "shipping priority"),
    query!("q05", "local supplier volume"),
    query!("q06", "forecasting revenue change"),
    query!("q10", "returned item reporting"),
    query!("q12", "shipping modes and order priority"),
    query!("q14", "promotion effect"),
    query!("q19", "discounted revenue"),
];
