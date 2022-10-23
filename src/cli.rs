use std::{process::exit, str::FromStr};

use anyhow::anyhow;
use clap::Parser;
use http::{
    header::{HeaderName, HeaderValue},
    Method, Uri,
};

/// Simple program to make a lot of HTTP requests
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub(crate) struct Args {
    /// Number of requests per second per client
    #[clap(short, long, default_value_t = 10)]
    pub rps: u64,
    /// Duration of load generation, in seconds
    #[clap(short, long, default_value_t = 10)]
    pub duration: u64,
    /// Request timeout, in seconds
    #[clap(long, default_value_t = 30)]
    pub timeout: u64,
    /// Total number of concurrent connections
    #[clap(short, long, default_value_t = 1 << 6)]
    pub connections: u64,
    /// Total number of threads to use
    #[clap(short, long, default_value_t = 1)]
    pub threads: usize,
    /// TODO: Show terminal UI with live updating results table and latency graph
    #[clap(long, parse(from_flag))]
    pub tui: bool,
    /// Each request is a span sent to a default OpenTelemetry collector via grpc
    #[clap(long, parse(from_flag))]
    pub opentelemetry: bool,
    /// URL to make requests agains
    #[clap(value_parser)]
    pub url: Uri,
    /// Headers to include on requests
    #[clap(short = 'm', long = "method", default_value_t = Method::GET)]
    pub method: Method,
    /// Headers to include in all requests
    ///
    /// e.g. -H 'Authorization: Basic aGJ0OmhidA=='
    #[clap(short = 'H', long = "header")]
    pub headers: Vec<Header>,
    /// TODO: Request body to include in all requests
    ///
    /// e.g. -b '{"custom":"json","request":{"body":0}}'
    #[clap(short, long)]
    pub body: Option<String>,
    /// TODO: File containing the request body to include in all requests
    #[clap(short, long)]
    pub file: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Header {
    pub(crate) name: HeaderName,
    pub(crate) value: HeaderValue,
}

impl FromStr for Header {
    type Err = anyhow::Error;

    fn from_str(maybe_header: &str) -> Result<Self, Self::Err> {
        match maybe_header.split_once(':') {
            Some((name, value)) => {
                let name = HeaderName::from_str(name.trim())?;
                let value = HeaderValue::from_str(value.trim())?;
                Ok(Header{name, value})
            },
            None => Err(anyhow!("'{}' is not a valid header declaration. example valid declaration: -H 'Custom: Header'", maybe_header)),
        }
    }
}

pub(crate) fn parse_args() -> Args {
    let args = Args::parse();

    match args.url.host() {
        Some("") | None => {
            eprintln!("invalid target host {}", args.url.authority().unwrap());
            exit(1)
        }
        Some(_) => (),
    }

    match args.method {
        Method::GET
        | Method::POST
        | Method::PUT
        | Method::DELETE
        | Method::PATCH
        | Method::HEAD
        | Method::CONNECT
        | Method::OPTIONS
        | Method::TRACE => args,
        _ => {
            println!("invalid HTTP method {}. must be one of GET | POST | PUT | DELETE | PATCH | HEAD | CONNECT | OPTIONS | TRACE", args.method.as_str());
            exit(1)
        }
    }
}
