use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{cli::parse_args, requests::orchestrate_requests};
use anyhow::anyhow;
use cli::Args;
use flume::Sender;
use futures::future::join_all;
use hdrhistogram::{sync::Recorder, Histogram, SyncHistogram};
use http::{
    header::{ACCEPT, HOST},
    request::Parts,
    HeaderMap,
};
use hyper::{client::conn::SendRequest, http::Request, Body};
use tokio::time::{timeout, timeout_at};

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

    let start = Instant::now();
    multi_threaded_runtime.block_on(messing_around(parse_args(), histogram.recorder()));
    let elapsed = start.elapsed();
    // multi_threaded_runtime.block_on(async_main(parse_args(), histogram.recorder()));

    histogram.refresh_timeout(Duration::from_millis(10));

    histogram.iter_quantiles(10).for_each(|q| {
        if q.count_at_value() > 0 && q.count_since_last_iteration() > 0 {
            println!(
                "{:>10.6} {:>6} {:>6}",
                q.quantile(),
                q.value_iterated_to(),
                q.count_since_last_iteration()
            )
        }
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

    println!("finished all requests in {:?} ms", elapsed.as_millis())
}

struct ConnectionPool {
    senders: flume::Receiver<SendRequest<Body>>,
    push: flume::Sender<SendRequest<Body>>,
    req_parts: Parts,
    timer: Sender<u64>,
}

impl ConnectionPool {
    async fn new(req_parts: Parts, timer: Sender<u64>) -> Self {
        println!("{:?}", rlimit::getrlimit(rlimit::Resource::NOFILE));
        println!("{:?}", rlimit::increase_nofile_limit(2 << 14));
        println!("{:?}", rlimit::getrlimit(rlimit::Resource::NOFILE));
        let limits = rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap();
        let connections = limits.0 / 2;
        println!("establishing {} connections", connections);
        let (push, senders) = flume::bounded::<SendRequest<Body>>(connections.try_into().unwrap());

        let start = tokio::time::Instant::now();
        let spawns = (0..connections).into_iter().map(|_| async {
            let stream = timeout_at(
                start + Duration::from_secs(1),
                tokio::net::TcpStream::connect("127.0.0.1:8082"),
            )
            .await;

            if stream.is_err() {
                // println!("timed out trying to connect");
                return None;
            }

            let stream = stream.unwrap();

            // let stream = tokio::net::TcpStream::connect("127.0.0.1:8082")
            //     .await;

            if stream.is_err() {
                // println!("couldnt open stream {i}");
                return None;
            }
            let stream = stream.unwrap();
            stream.set_nodelay(true).unwrap();

            let handshake = timeout_at(
                start + Duration::from_secs(2),
                hyper::client::conn::handshake(stream),
            )
            .await;

            if handshake.is_err() {
                // println!("timed out trying to handshake");
                return None;
            }

            let (mut send, conn) = handshake.unwrap().unwrap();

            tokio::spawn(conn);

            // let url = http::Uri::from_str("http://127.0.0.1:8082/hi").unwrap();

            // let req = Request::builder()
            //     .header(ACCEPT, "*/*")
            //     .header(HOST, url.authority().unwrap().as_str())
            //     .method("GET")
            //     .uri("/warmup")
            //     .body(Body::empty())
            //     .unwrap();
            // let resp = send.send_request(req).await.unwrap();
            // println!("response is {:?}", resp);

            Some(send)
        });

        let sender_vec = join_all(spawns).await;

        println!(
            "took {:?} ms to establish connections",
            start.elapsed().as_millis()
        );

        let mut i = 0;

        for sender in sender_vec {
            if let Some(sender) = sender {
                push.send(sender).unwrap();
                i += 1;
            }
        }

        println!(
            "took {:?} ms to open a total of {i} connections",
            start.elapsed().as_millis()
        );

        ConnectionPool {
            senders,
            push,
            req_parts,
            timer,
        }
    }

    async fn perform_request(&self, body: Body) -> Result<Duration, anyhow::Error> {
        let start = Instant::now();

        if let Ok(mut sender) = self.senders.recv_async().await {
            if start.elapsed().as_millis() > 3 {
                println!("waiting on sender... {:?}", start.elapsed().as_millis());
            }
            let mut req = Request::builder()
                .method(self.req_parts.method.as_str())
                .uri(self.req_parts.uri.path());
            req.headers_mut().unwrap().extend(
                self.req_parts
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            let req = req.body(body).unwrap();

            sender.send_request(req).await.unwrap();

            let elapsed = start.elapsed();

            self.push.send(sender).unwrap();

            // println!("request completed in {:?}", elapsed);
            self.timer
                .send_async(
                    elapsed.as_secs() * 1_000_000
                        + <u32 as Into<u64>>::into(elapsed.subsec_micros()),
                )
                .await;

            return Ok(elapsed);
        }
        Err(anyhow!("unable to make request"))
    }
}

async fn messing_around(args: Args, mut recorder: Recorder<u64>) {
    let url = http::Uri::from_str("http://127.0.0.1:8082/hi").unwrap();

    let req = Request::builder()
        .header(ACCEPT, "*/*")
        .header(HOST, url.authority().unwrap().as_str())
        .method("GET")
        .uri("/hi")
        .body(Body::empty())
        .unwrap();
    let (parts, body) = req.into_parts();
    let start = tokio::time::Instant::now();
    println!("kicking off client requests...");

    let (s, r) = flume::unbounded::<u64>();
    {
        let ct = Arc::new(ConnectionPool::new(parts, s).await);

        let spawns = (0..1).into_iter().map(|_| {
            let ct = ct.clone();
            let rps = args.rps;
            let duration = args.duration;

            async move {
                let mut interval = tokio::time::interval(Duration::from_micros(1_000_000 / rps));
                let mut spawns = Vec::new();
                // let (sender, mut receiver) = mpsc::unbounded_channel::<u64>();

                // let mut i = 0;

                for _ in 0..(rps * duration) {
                    interval.tick().await;
                    let ct = ct.clone();
                    let s = tokio::spawn(async move { ct.perform_request(Body::empty()).await });
                    spawns.push(s);
                }

                println!(
                    "finished making all requests in {:?} ms",
                    start.elapsed().as_millis()
                );

                // join_all(spawns).await;
            }
        });

        println!(
            "waiting on client requests... {:?} ms",
            start.elapsed().as_millis()
        );

        // timeout_at(start + Duration::from_secs(args.duration + args.timeout), join_all(spawns)).await;
        join_all(spawns).await;

        println!(
            "completed client requests... {:?} ms",
            start.elapsed().as_millis()
        );
    }
    // drop(ct);

    println!("recording values... {:?} ms", start.elapsed().as_millis());

    let timeout = Instant::now();

    loop {
        match r.recv() {
            Ok(n) => recorder
                .record(n)
                .expect("recording value should never fail"),
            x => {
                println!("{:?}", x);
                break;
            }
        };
        if timeout.elapsed().as_secs() > args.timeout {
            break
        }
    }

    println!(
        "finished executing all requests in {:?} ms",
        start.elapsed().as_millis()
    );

    let elapsed = start.elapsed();

    println!(
        "finished processing all requests in {:?} ms",
        elapsed.as_millis()
    )
}

async fn async_main(args: Args, mut recorder: Recorder<u64>) {
    let headers = Arc::new(args.headers);

    let clients = join_all((0..args.connections).map(|_| async {
        // let stream = tokio::net::TcpStream::connect(args.url.to_string()).await.unwrap();
        // stream.set_nodelay(true).unwrap();
        // // stream.set_keepalive(std::time::Duration::from_secs(1).into())?;
        // let (send, conn) = hyper::client::conn::handshake(stream).await.unwrap();
        // tokio::spawn(conn);

        let c = hyper::Client::builder()
            .retry_canceled_requests(false)
            .build_http();

        let mut r = Request::builder()
            .method(args.method.as_str())
            .uri(args.url.to_string());
        for header in headers.iter() {
            r = r.header(header.name.to_owned(), header.value.to_owned())
        }
        let rr = r.body(hyper::Body::empty()).unwrap();

        c.request(rr).await.expect("client failed to warmup");

        c
    }))
    .await;

    let start = std::time::Instant::now();

    let (sender, receiver) = flume::unbounded::<u64>();

    // let mut interval = tokio::time::interval(Duration::from_micros(1_000_000 / args.rps));

    // for _ in 0..(args.rps * args.duration) {
    //     interval.tick().await;

    // }
    println!("kicking off client requests...");

    let spawns = clients.iter().map(|c| {
        timeout(
            Duration::from_secs(args.duration + args.timeout),
            orchestrate_requests(
                Arc::new(c.to_owned()),
                args.method.clone(),
                headers.clone(),
                args.url.to_string(),
                sender.clone(),
                args.rps,
                args.duration,
            ),
        )
    });
    println!("waiting on client requests...");

    join_all(spawns).await;

    for _ in 0..(args.rps * args.connections * args.duration) {
        match receiver.recv() {
            Ok(n) => recorder
                .record(n)
                .expect("recording value should never fail"),
            x => {
                println!("{:?}", x);
                break;
            }
        };
    }

    println!(
        "finished all requests in {:?} ms",
        start.elapsed().as_millis()
    )
}
