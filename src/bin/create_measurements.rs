#![warn(clippy::pedantic)]
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use humansize::{format_size, BINARY};
use indicatif::ProgressBar;
use rand::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of records to create
    #[arg(help = "Positive integer number of records to create")]
    num_records: usize,

    /// Input file containing weather station names
    #[arg(short, long, help = "Path to the input weather stations file")]
    input_file: PathBuf,

    /// Output file for measurements
    #[arg(short, long, help = "Path to the output measurements file")]
    output_file: PathBuf,
}

fn build_weather_station_name_list(input_file: &PathBuf) -> Result<Vec<String>> {
    let file = File::open(input_file).with_context(|| {
        format!(
            "Failed to open weather stations file: {}",
            input_file.display()
        )
    })?;
    let reader = BufReader::new(file);

    Ok(reader
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.contains('#'))
        .filter_map(|line| {
            let (name, _) = line.split_once(';')?;
            Some(name.to_string())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect())
}

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
fn estimate_file_size(weather_station_names: &[String], num_rows_to_create: usize) -> String {
    let total_name_bytes: usize = weather_station_names.iter().map(String::len).sum();
    let avg_name_bytes = total_name_bytes as f64 / weather_station_names.len() as f64;
    let avg_temp_bytes = 4.400_200_100_050_025;
    let avg_line_length = avg_name_bytes + avg_temp_bytes + 2.0;
    let estimated_size = num_rows_to_create as f64 * avg_line_length;

    format_size(estimated_size as u64, BINARY)
}

use std::io::BufWriter;

fn build_test_data(
    weather_station_names: &[String],
    num_rows_to_create: usize,
    output_file: &PathBuf,
) -> Result<()> {
    let start_time = Instant::now();
    let coldest_temp = -99.9;
    let hottest_temp = 99.9;
    let mut rng = rand::thread_rng();
    let station_names_10k_max: Vec<_> = weather_station_names
        .choose_multiple(&mut rng, 10_000)
        .collect();

    eprintln!("Building test data...");

    let file = File::create(output_file).with_context(|| {
        format!(
            "Failed to create measurements file: {}",
            output_file.display()
        )
    })?;
    let mut writer = BufWriter::new(file);

    let pb = ProgressBar::new(num_rows_to_create as u64);

    for i in 0..num_rows_to_create {
        let station = station_names_10k_max
            .choose(&mut rng)
            .ok_or_else(|| anyhow!("Failed to choose a random station"))?;
        let temp = rng.gen_range(coldest_temp..=hottest_temp);
        writeln!(writer, "{station};{temp:.1}").context("Failed to write to measurements file")?;

        if i % 10000 == 0 {
            pb.set_position(i as u64);
        }
    }

    writer.flush().context("Failed to flush writer")?;
    pb.finish_with_message("Test data generation complete");

    let elapsed_time = start_time.elapsed();
    let file_size = std::fs::metadata(output_file)?.len();
    let human_file_size = format_size(file_size, BINARY);

    eprintln!(
        "Test data successfully written to {}",
        output_file.display()
    );
    eprintln!("Actual file size: {human_file_size}");
    eprintln!("Elapsed time: {elapsed_time:?}");

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let weather_station_names = build_weather_station_name_list(&args.input_file)?;
    let estimated_file_size = estimate_file_size(&weather_station_names, args.num_records);
    eprintln!("Estimated file size is: {estimated_file_size}");

    build_test_data(&weather_station_names, args.num_records, &args.output_file)?;
    eprintln!("Test data build complete.");

    Ok(())
}
