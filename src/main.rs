use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

use clap::Parser;
use fxhash::FxHashMap;
use memmap2::Mmap;
use rayon::{iter::ParallelIterator, slice::ParallelSlice};

fn main() -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let args = Args::parse();
    let file = File::open(&args.input_file)?;
    let content = unsafe { Mmap::map(&file)? };
    let content_slice = if content[content.len() - 1] == b'\n' {
        &content[..content.len() - 1]
    } else {
        &content
    };
    eprintln!("Setup took {:?}", start.elapsed());
    let start_parsing = std::time::Instant::now();
    let registry = content_slice
        .par_split(|&b| b == b'\n')
        .fold(Registry::default, |mut registry, line| {
            let (name, temp) = parse_line(line);
            registry
                .entry(name)
                .or_insert_with(Aggregation::new)
                .update(temp);
            registry
        })
        .reduce(Registry::default, |mut a, b| {
            b.into_iter().for_each(|(name, aggregation)| {
                a.entry(name)
                    .or_insert_with(Aggregation::new)
                    .merge(&aggregation);
            });
            a
        });
    let elapsed = start_parsing.elapsed();
    eprintln!("Aggregation took {:?}", elapsed);

    let start_sorting = std::time::Instant::now();
    let mut name_aggregations = registry.into_iter().collect::<Vec<_>>();
    name_aggregations.sort_unstable_by_key(|&(name, _)| name);
    let elapsed = start_sorting.elapsed();
    eprintln!("Sorting took {:?}", elapsed);

    let handle = std::io::stdout().lock();
    let mut writer = BufWriter::new(handle);

    let start_writing = std::time::Instant::now();
    writer.write_all(b"{")?;
    let (first_name, first_aggregation) = name_aggregations.first().unwrap();
    push_aggregation(&mut writer, first_name, first_aggregation)?;
    for (name, aggregation) in &name_aggregations[1..] {
        writer.write_all(b", ")?;
        push_aggregation(&mut writer, name, aggregation)?;
    }
    writer.write_all(b"}")?;
    let elapsed = start_writing.elapsed();
    eprintln!("Writing took {:?}", elapsed);

    Ok(())
}

fn parse_line(line: &[u8]) -> (&[u8], i32) {
    let (name, is_negative, tens, ones, decimal) = match line {
        [name @ .., b';', b'-', tens, ones, b'.', decimal] => (name, true, *tens, *ones, *decimal),
        [name @ .., b';', b'-', ones, b'.', decimal] => (name, true, b'0', *ones, *decimal),
        [name @ .., b';', tens, ones, b'.', decimal] => (name, false, *tens, *ones, *decimal),
        [name @ .., b';', ones, b'.', decimal] => (name, false, b'0', *ones, *decimal),
        _ => panic!("Invalid line format {}", String::from_utf8_lossy(line)),
    };
    let zero = b'0' as i32;
    let value =
        ((tens as i32 - zero) * 100) + ((ones as i32 - zero) * 10) + (decimal as i32 - zero);
    if is_negative {
        (name, -value)
    } else {
        (name, value)
    }
}

fn push_aggregation(
    writer: &mut impl Write,
    name: &[u8],
    aggregation: &Aggregation,
) -> anyhow::Result<()> {
    writer.write_all(name)?;
    writer.write_all(b"=")?;
    push_float(writer, aggregation.min)?;
    writer.write_all(b"/")?;
    push_float(writer, aggregation.mean())?;
    writer.write_all(b"/")?;
    push_float(writer, aggregation.max)?;
    Ok(())
}

fn push_float(writer: &mut impl Write, mut value: i32) -> anyhow::Result<()> {
    if value < 0 {
        writer.write_all(b"-")?;
        value = -value;
    }
    if value >= 100 {
        writer.write_all(&[(value / 100) as u8 + b'0'])?;
    }
    writer.write_all(&[((value / 10) % 10) as u8 + b'0'])?;
    writer.write_all(b".")?;
    writer.write_all(&[(value % 10) as u8 + b'0'])?;
    Ok(())
}

type Registry<'a> = FxHashMap<&'a [u8], Aggregation>;

struct Aggregation {
    min: i32,
    max: i32,
    sum: i32,
    count: u32,
}

impl Aggregation {
    fn new() -> Self {
        Self {
            min: i32::MAX,
            max: i32::MIN,
            sum: 0,
            count: 0,
        }
    }

    fn update(&mut self, value: i32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn mean(&self) -> i32 {
        let mean_10 = self.sum * 10 / self.count as i32;
        let remainder = mean_10 % 10;
        if remainder >= 5 {
            mean_10 / 10 + 1
        } else {
            mean_10 / 10
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to measurements file
    #[arg(short, long)]
    input_file: PathBuf,
}
