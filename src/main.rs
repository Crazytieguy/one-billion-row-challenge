use std::{
    array,
    fs::File,
    io::{BufWriter, Read, Write},
    path::PathBuf,
    thread,
};

use clap::Parser;
use fxhash::FxHashMap;
use itertools::Itertools;

const PARALLELISM: usize = 8;
const BUFFER_SIZE: usize = 128 * 1024 * 1024;

fn main() -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let args = Args::parse();
    let mut file = File::open(&args.input_file)?;
    eprintln!("Setup took {:?}", start.elapsed());
    let start_parsing = std::time::Instant::now();
    let mut working_buffer = vec![0_u8; BUFFER_SIZE];
    let mut loading_buffer = vec![0_u8; BUFFER_SIZE];
    let mut registries: [Registry; PARALLELISM] = array::from_fn(|_| Registry::default());
    file.read(&mut working_buffer)?;
    loop {
        let (remainder, to_process) = working_buffer
            .rsplitn(2, |&b| b == b'\n')
            .collect_tuple()
            .ok_or_else(|| anyhow::anyhow!("No newline found in working buffer"))?;
        let chunks = chunk_at_newlines(to_process);
        let read = thread::scope(|s| {
            chunks
                .iter()
                .zip(registries.iter_mut())
                .for_each(|(chunk, mut registry)| {
                    s.spawn(move || {
                        let mut start = 0;
                        for end in memchr::memchr_iter(b'\n', chunk).chain([chunk.len()]) {
                            process_line(&mut registry, &chunk[start..end]);
                            start = end + 1;
                        }
                    });
                });
            loading_buffer[..remainder.len()].copy_from_slice(remainder);
            file.read(&mut loading_buffer[remainder.len()..])
        })?;
        if read == 0 {
            break;
        }
        if read + remainder.len() < loading_buffer.len() {
            loading_buffer.drain((read + remainder.len())..);
        }
        std::mem::swap(&mut working_buffer, &mut loading_buffer);
    }
    let registry = registries
        .into_iter()
        .reduce(|mut a, b| {
            for (name, aggregation) in b {
                match a.get_mut(&name) {
                    Some(existing) => existing.merge(&aggregation),
                    None => {
                        a.insert(name, aggregation);
                    }
                }
            }
            a
        })
        .expect("At least one registry");
    let elapsed = start_parsing.elapsed();
    eprintln!("Aggregation took {:?}", elapsed);

    let start_sorting = std::time::Instant::now();
    let mut name_aggregations = registry.into_iter().collect::<Vec<_>>();
    name_aggregations.sort_unstable_by(|(name1, _), (name2, _)| name1.cmp(name2));
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

fn chunk_at_newlines(to_chunk: &[u8]) -> [&[u8]; PARALLELISM] {
    let chunk_size = to_chunk.len() / PARALLELISM;
    let mut start = 0;
    array::from_fn(|i| {
        let end = if i == PARALLELISM - 1 {
            to_chunk.len()
        } else {
            memchr::memrchr(b'\n', &to_chunk[..(start + chunk_size)])
                .expect("There should always be a newline")
        };
        let ret = &to_chunk[start..end];
        start = end + 1;
        ret
    })
}

fn process_line(registry: &mut Registry, line: &[u8]) {
    let (name, temp) = parse_line(line);
    match registry.get_mut(name) {
        Some(aggregation) => aggregation.update(temp),
        None => {
            let mut aggregation = Aggregation::new();
            aggregation.update(temp);
            registry.insert(name.to_vec(), aggregation);
        }
    }
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

type Registry = FxHashMap<Vec<u8>, Aggregation>;

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
