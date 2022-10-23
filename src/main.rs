use std::time::{Duration, Instant};

use crate::{cli::parse_args, requests::request_runtime};
use hdrhistogram::Histogram;

mod cli;
mod requests;

const QUANTILES: [f64; 34] = [
    0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80,
    0.85, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 0.999, 0.9999, 0.99999,
    0.999999, 0.9999999, 0.99999999, 1.0,
];

fn main() {
    let args = parse_args();

    let mut histogram = Histogram::<u64>::new(5)
        .expect("histogram should be creatable")
        .into_sync();

    let multi_threaded_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name("hbt")
        .build()
        .expect("runtime should be created");

    let start = Instant::now();
    multi_threaded_runtime.block_on(request_runtime(args, histogram.recorder()));
    let elapsed = start.elapsed();

    histogram.refresh_timeout(Duration::from_millis(10));

    QUANTILES.iter().for_each(|&q| {
        println!(
            "{:>10.6} %    {:>10}    {:>10}",
            q * 100.0,
            histogram.value_at_quantile(q),
            histogram.count_between(0, histogram.value_at_quantile(q))
        )
    });

    println!(
        "max: {:?}, mean: {:?}, median: {:?}, min: {:?}, stddev: {:?}, total: {:?}",
        histogram.max(),
        histogram.mean(),
        histogram.value_at_quantile(0.5),
        histogram.min(),
        histogram.stdev(),
        histogram.count_between(histogram.min(), histogram.max())
    );

    println!("finished all requests in {:?} ms", elapsed.as_millis());
}
