use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::join_all;
use hdrhistogram::sync::{IdleRecorder, Recorder};
use hyper::client::HttpConnector;
use reqwest::Client;
use tokio::{
    spawn,
    sync::mpsc::{self, UnboundedSender},
};

use crate::cli::Header;

pub(crate) async fn orchestrate_requests(
    client: Arc<hyper::Client<HttpConnector>>,
    method: http::Method,
    headers: Arc<Vec<Header>>,
    url: String,
    // recorder: IdleRecorder<Recorder<u64>, u64>,
    sender: flume::Sender<u64>,
    rps: u64,
    duration: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_micros(1_000_000 / rps));
    let mut spawns = Vec::new();
    // let (sender, mut receiver) = mpsc::unbounded_channel::<u64>();

    // let mut i = 0;

    for _ in 0..(rps * duration) {
        interval.tick().await;
        spawns.push(spawn(capture_request(
            client.clone(),
            method.to_owned(),
            headers.to_owned(),
            url.to_string(),
            sender.clone(),
        )));
    }

    // drop(sender);

    // let mut recorder = recorder.activate();

    // loop {
    //     match receiver.recv().await {
    //         Some(n) => recorder
    //             .record(n)
    //             .expect("recording value should never fail"),
    //         None => break,
    //     };
    // }

    join_all(spawns).await;
}

pub(crate) async fn capture_request(
    client: Arc<hyper::Client<HttpConnector>>,
    method: http::Method,
    headers: Arc<Vec<Header>>,
    url: String,
    sender: flume::Sender<u64>,
) {
    let elapsed = perform_request(client, method, headers, url).await;
    sender
        .send(elapsed.as_secs() * 1_000_000 + <u32 as Into<u64>>::into(elapsed.subsec_micros()))
        .expect("sending value should never fail");
}

pub(crate) async fn perform_request(
    client: Arc<hyper::Client<HttpConnector>>,
    method: http::Method,
    headers: Arc<Vec<Header>>,
    url: String,
) -> Duration {
    let mut r = http::Request::builder()
        .method(method.as_str())
        .uri(url.to_string());
    for header in headers.iter() {
        r = r.header(header.name.to_owned(), header.value.to_owned())
    }
    let rr = r.body(hyper::Body::empty()).unwrap();

    let start = Instant::now();
    let mut response = client.request(rr).await;
    let mut elapsed = start.elapsed();

    while response.is_err() {
        let mut r = http::Request::builder()
            .method(method.as_str())
            .uri(url.to_string());
        for header in headers.iter() {
            r = r.header(header.name.to_owned(), header.value.to_owned())
        }
        let rr = r.body(hyper::Body::empty()).unwrap();
        response = client.request(rr).await;
        elapsed = start.elapsed();
    }

    match response {
        _ => elapsed,
    }
}
