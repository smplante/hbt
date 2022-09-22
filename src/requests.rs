use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::join_all;
use hdrhistogram::sync::{IdleRecorder, Recorder};
use reqwest::Client;
use tokio::{
    spawn,
    sync::mpsc::{self, UnboundedSender},
};

use crate::cli::Header;

pub(crate) async fn orchestrate_requests(
    client: Client,
    method: http::Method,
    headers: Arc<Vec<Header>>,
    url: String,
    recorder: IdleRecorder<Recorder<u64>, u64>,
    rps: u64,
    duration: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_micros(1_000_000 / rps));
    let mut spawns = Vec::new();
    let (sender, mut receiver) = mpsc::unbounded_channel::<u64>();

    let mut i = 0;

    while i < rps * duration {
        interval.tick().await;
        spawns.push(spawn(capture_request(
            client.to_owned(),
            method.to_owned(),
            headers.to_owned(),
            url.to_string(),
            sender.clone(),
        )));
        i += 1;
    }

    drop(sender);

    let mut recorder = recorder.activate();

    loop {
        match receiver.recv().await {
            Some(n) => recorder
                .record(n)
                .expect("recording value should never fail"),
            None => break,
        };
    }

    join_all(spawns).await;
}

pub(crate) async fn capture_request(
    client: Client,
    method: http::Method,
    headers: Arc<Vec<Header>>,
    url: String,
    sender: UnboundedSender<u64>,
) {
    let elapsed = perform_request(client, method, headers, url).await;
    sender
        .send(elapsed.as_secs() * 1_000_000 + <u32 as Into<u64>>::into(elapsed.subsec_micros()))
        .expect("sending value should never fail");
}

pub(crate) async fn perform_request(
    client: Client,
    method: http::Method,
    headers: Arc<Vec<Header>>,
    url: String,
) -> Duration {
    let mut request = client.request(method, url);
    for header in headers.iter() {
        request = request.header(header.name.to_owned(), header.value.to_owned())
    }

    let start = Instant::now();
    let response = request.send().await;
    let elapsed = start.elapsed();

    match response {
        _ => elapsed,
    }
}
