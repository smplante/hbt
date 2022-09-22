use std::{sync::Arc, time::Duration};

use crate::{cli::parse_args, requests::orchestrate_requests};
use futures::future::join_all;
use hdrhistogram::Histogram;
use tokio::time::timeout;

mod cli;
mod requests;

fn main() {
    let args = parse_args();

    let mut histogram = Histogram::<u64>::new_with_max(60 * 1_000_000, 5)
        .expect("histogram should be creatable")
        .into_sync();

    let multi_threaded_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name("hbt")
        .build()
        .expect("runtime should be created");

    let headers = Arc::new(args.headers);

    multi_threaded_runtime.block_on(async {
        let c = reqwest::Client::builder().build().unwrap();
        let _ = c
            .request(args.method.clone(), args.url.as_str())
            .send()
            .await;

        let headers = headers;

        let clients = join_all((0..args.connections).map(|_| async {
            let client = reqwest::Client::builder().build().unwrap();

            let mut req = client.request(args.method.to_owned(), args.url.to_string());
            for header in headers.iter() {
                req = req.header(header.name.to_owned(), header.value.to_owned())
            }
            req.send().await.expect("client failed to warmup");

            client
        }))
        .await;

        let start = std::time::Instant::now();
        let spawns = clients.iter().map(|c| {
            timeout(
                Duration::from_secs(args.duration + args.timeout),
                orchestrate_requests(
                    c.to_owned(),
                    args.method.clone(),
                    headers.clone(),
                    args.url.to_string(),
                    histogram.recorder().into_idle(),
                    args.rps,
                    args.duration,
                ),
            )
        });

        join_all(spawns).await;

        println!(
            "finished all requests in {:?} ms",
            start.elapsed().as_millis()
        )
    });

    histogram.refresh_timeout(Duration::from_millis(10000));

    println!(
        "max: {:?}, mean: {:?}, median: {:?}, min: {:?}, stddev: {:?}, total: {:?}",
        histogram.max(),
        histogram.mean(),
        histogram.value_at_quantile(0.5),
        histogram.min(),
        histogram.stdev(),
        histogram.count_between(histogram.min(), histogram.max())
    );
}
