use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::anyhow;
use flume::{Receiver, Sender};
use futures::future::join_all;
use hdrhistogram::sync::Recorder;
use http::{
    header::{ACCEPT, HOST},
    request::Parts,
    Request, Uri,
};
use hyper::{client::conn::SendRequest, Body};
use opentelemetry::{
    global, runtime,
    sdk::{
        trace::{Config},
        Resource,
    },
    trace::{FutureExt, SpanKind, TraceContextExt, TraceError},
    Context, KeyValue,
};
use signal_hook::{consts::SIGINT, iterator::Signals, low_level::exit};
use tokio::time::{interval, timeout_at};
use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};

use crate::cli::Args;

pub(crate) struct ConnectionPool {
    senders: flume::Receiver<SendRequest<Body>>,
    push: flume::Sender<SendRequest<Body>>,
    req_parts: Parts,
    timer: Sender<(u64, bool)>,
    canceled: Arc<AtomicBool>,
}

impl ConnectionPool {
    pub(crate) async fn new(
        connections: u64,
        timer: Sender<(u64, bool)>,
        url: Uri,
        req_parts: Parts,
        canceled: Arc<AtomicBool>,
    ) -> Result<Self, anyhow::Error> {
        let connections = connections.min(1 << 15);
        rlimit::increase_nofile_limit(connections + 100)?;
        let limits = rlimit::getrlimit(rlimit::Resource::NOFILE)?;
        let connections = limits.0.min(connections);
        let (push, senders) = flume::bounded::<SendRequest<Body>>(connections.try_into()?);

        let resolver = trust_dns_resolver::TokioAsyncResolver::tokio(
            ResolverConfig::default(),
            ResolverOpts::default(),
        )?;
        let start_ip = Instant::now();

        let addr = match url.authority() {
            Some(authority) => {
                let ip = resolver.lookup_ip(authority.host()).await?;
                println!("DNS resolution took {} ms", start_ip.elapsed().as_millis());
                format!(
                    "{}:{}",
                    ip.iter().next().unwrap(),
                    authority.port_u16().unwrap()
                )
            }
            _ => return Err(anyhow!("invalid host:port")),
        };

        println!("attempting to establish {} connections", connections);
        let start = tokio::time::Instant::now();
        let spawns = (0..connections).into_iter().map(|_| async {
            let stream = timeout_at(
                start + Duration::from_secs(2),
                tokio::net::TcpStream::connect(addr.clone()),
            )
            .await;

            let stream = match stream {
                Err(_) => {
                    return Err(anyhow!("connect timeout"));
                }
                Ok(Err(e)) => {
                    return Err(anyhow!(e));
                }
                Ok(Ok(stream)) => stream,
            };
            // stream.set_nodelay(true)?;

            let handshake_timeout = timeout_at(
                start + Duration::from_secs(4),
                hyper::client::conn::handshake(stream),
            )
            .await;

            let (send, conn) = match handshake_timeout {
                Err(_) => {
                    return Err(anyhow!("handshake timeout"));
                }
                Ok(Err(e)) => {
                    return Err(anyhow!(e));
                }
                Ok(Ok(handshake)) => handshake,
            };

            tokio::spawn(conn);

            Ok(send)
        });

        let sender_vec = join_all(spawns).await;

        let mut i = 0;

        for sender in sender_vec {
            if let Ok(sender) = sender {
                // let tracer = global::tracer("SendRequest");
                // let span = opentelemetry::trace::Tracer::start(&tracer, format!("id: {}", i));
                // let cx = Context::current_with_span(span);
                push.send(sender)?;
                i += 1;
            }
        }

        println!(
            "successfully established {} / {} connections in {} ms",
            i,
            connections,
            start.elapsed().as_millis()
        );

        Ok(ConnectionPool {
            senders,
            push,
            req_parts,
            timer,
            canceled,
        })
    }

    pub(crate) async fn perform_request(&self, body: Body) -> Result<Duration, anyhow::Error> {
        if self.canceled.load(Ordering::SeqCst) {
            return Err(anyhow!("application canceled"));
        }

        let start = Instant::now();

        let tracer = global::tracer("perform_request");
        let span = opentelemetry::trace::Tracer::span_builder(&tracer, "senders.recv_async")
            .with_kind(SpanKind::Client)
            .start(&tracer);
        if let Ok(mut sender) = self
            .senders
            .recv_async()
            .with_context(Context::current_with_span(span))
            .await
        {
            // let sender_num = sender.1;
            // let cx = Context::current_with_span(span);

            let delayed = start.elapsed().as_micros() > 1000;

            let mut req = Request::builder()
                .method(self.req_parts.method.as_str())
                .uri(self.req_parts.uri.path());
            req.headers_mut().unwrap().extend(
                self.req_parts
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            let req = req.body(body)?;

            // let mut span = opentelemetry::trace::Tracer::span_builder(&tracer, "sender.send_request").with_kind(SpanKind::Client).start(&tracer);
            // span.set_attribute(KeyValue::new("sender", sender_num));
            {
                sender
                    .send_request(req)
                    // .with_context(Context::current_with_span(span))
                    .await?;
            }
            let elapsed = start.elapsed();

            // let mut span = opentelemetry::trace::Tracer::span_builder(&tracer, "push.send_async").with_kind(SpanKind::Client).start(&tracer);
            // span.set_attribute(KeyValue::new("sender", sender_num));
            self.push
                .send_async(sender)
                // .with_context(Context::current_with_span(span))
                .await?;
            // get_active_span(|span| {
            //     span.set_attribute(KeyValue::new("sender", sender_num))
            // });

            if self.canceled.load(Ordering::SeqCst) {
                return Err(anyhow!("application canceled"));
            }

            // let mut span = opentelemetry::trace::Tracer::span_builder(&tracer, "timer.send_async").with_kind(SpanKind::Client).start(&tracer);
            // span.set_attribute(KeyValue::new("sender", sender_num));
            self.timer
                .send_async((
                    elapsed.as_secs() * 1_000_000
                        + <u32 as Into<u64>>::into(elapsed.subsec_micros()),
                    delayed,
                ))
                // .with_context(Context::current_with_span(span))
                .await?;

            return Ok(elapsed);
        }
        Err(anyhow!("unable to make request"))
    }
}

fn init_tracer(export: bool) -> Result<(), TraceError> {
    let span_processor = match export {
        true => {
            println!("CAUTION: Exporting OpenTelemetry spans can impact benchmarking performance under heavy loads");

            let exporter = opentelemetry_otlp::SpanExporterBuilder::Tonic(
                opentelemetry_otlp::new_exporter().tonic(),
            )
            .build_span_exporter()?;

            opentelemetry::sdk::trace::BatchSpanProcessor::builder(exporter, runtime::Tokio)
                .with_max_queue_size(10 << 15)
                .with_scheduled_delay(Duration::from_millis(100))
                .with_max_export_batch_size(1 << 14)
                .with_max_concurrent_exports(1 << 3)
                .build()
        }
        false => {
            let exporter = opentelemetry_sdk::testing::trace::NoopSpanExporter::new();

            opentelemetry::sdk::trace::BatchSpanProcessor::builder(exporter, runtime::Tokio)
                .with_max_queue_size(10 << 15)
                .with_scheduled_delay(Duration::from_millis(100))
                .with_max_export_batch_size(1 << 14)
                .with_max_concurrent_exports(1 << 3)
                .build()
        }
    };

    let provider = opentelemetry::sdk::trace::TracerProvider::builder()
        .with_span_processor(span_processor)
        .with_config(
            Config::default()
                .with_resource(Resource::new([KeyValue::new("service.name", "hbt-otlp")])),
        )
        .build();

    let _ = global::set_tracer_provider(provider);

    Ok(())
}

pub(crate) async fn request_runtime(args: Args, recorder: Recorder<u64>) {
    init_tracer(args.opentelemetry).expect("tracer should be set");

    let mut req = Request::builder()
        .header(ACCEPT, "*/*")
        .header(HOST, args.url.authority().unwrap().as_str())
        .method(args.method)
        .uri("/hi");

    req.headers_mut().unwrap().extend(
        args.headers
            .iter()
            .map(|h| (h.name.clone(), h.value.clone())),
    );
    let req = req.body(Body::empty()).unwrap();
    let (parts, _) = req.into_parts();
    let (s, r) = flume::unbounded::<(u64, bool)>();
    let canceled = Arc::new(AtomicBool::new(false));
    let canceled_clone = canceled.clone();

    tokio::spawn(async move {
        let canceled = canceled_clone;
        let mut signals = Signals::new(&[SIGINT]).unwrap();
        let mut interval = interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            match signals.pending().next() {
                Some(SIGINT) => {
                    canceled
                        .compare_exchange(false, true, Ordering::Release, Ordering::SeqCst)
                        .unwrap();
                    exit(1)
                }
                _ => (),
            }
        }
    });

    let connection_pool =
        match ConnectionPool::new(args.connections, s, args.url, parts, canceled).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("failed to establish the connection pool [{}]", e);
                exit(1);
            }
        };

    let start = Instant::now();
    let perform_requests = tokio::spawn(perform_requests(connection_pool, args.rps, args.duration));

    let record_request_timings = tokio::spawn(record_request_timings(
        r,
        recorder,
        args.duration + args.timeout,
    ));

    perform_requests.await.unwrap();

    println!(
        "finished making all requests in {:?} ms",
        start.elapsed().as_millis()
    );

    record_request_timings.await.unwrap();

    println!(
        "finished all requests in {:?} ms",
        start.elapsed().as_millis()
    );
    global::shutdown_tracer_provider();
}

async fn perform_requests(connection_pool: Arc<ConnectionPool>, rps: u64, duration: u64) {
    let start = tokio::time::Instant::now();
    let ct = connection_pool;

    let mut interval = tokio::time::interval(Duration::from_micros(1_000_000 / rps));

    for _ in 0..(rps * duration) {
        interval.tick().await;
        let ct = ct.clone();
        {
            let tracer = global::tracer("perform_request");
            let span = opentelemetry::trace::Tracer::span_builder(&tracer, "perform_request")
                .with_kind(SpanKind::Producer)
                .start(&tracer);
            let cx = Context::current_with_span(span);
            tokio::spawn(async move { ct.perform_request(Body::empty()).with_context(cx).await });
        }
    }

    println!(
        "finished making all requests in {:?} ms",
        start.elapsed().as_millis()
    );
}

async fn record_request_timings(
    timings: Receiver<(u64, bool)>,
    mut hisogram_recorder: Recorder<u64>,
    timeout: u64,
) {
    let start = Instant::now();

    let mut delayed = 0;

    loop {
        match timings.recv_async().await {
            Ok((n, b)) => {
                hisogram_recorder
                    .record(n)
                    .expect("recording value should never fail");
                if b {
                    delayed += 1;
                }
            }
            _ => {
                println!("{delayed} requests were delayed by over 1 ms");
                break;
            }
        };
        if start.elapsed().as_secs() > timeout {
            println!("{delayed} requests were delayed by over 1 ms");
            break;
        }
    }
}
