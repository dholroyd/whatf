extern crate time;
extern crate flate2;
extern crate chan;
extern crate regex;
extern crate urlparse;
#[macro_use] extern crate lazy_static;
extern crate hdrsample;
extern crate rusoto;
extern crate hyper;
#[macro_use]
extern crate nom;
#[macro_use]
extern crate clap;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate toml;
#[macro_use]
extern crate serde_derive;

mod parse_access_log;
mod process;
mod pathexpression;
mod rusoto_workarounds;
mod datasource;

use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use flate2::read::GzDecoder;
use parse_access_log::HttpdAccessLogParser;
use process::Consumer;
use std::time::Instant;
use std::thread;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use clap::{Arg, App};
use time::strptime;
use pathexpression::{PathExpression,PathMatchOptions};
use rusoto::s3;
use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::s3::S3Client;
use rusoto_workarounds::s3::S3ClientWorkarounds;
use rusoto::default_tls_client;
use hyper::client::Client;
use std::time::Duration;
use hyper::header::ContentType;
use hyper::mime;

fn process_file(name: &Path, consumer: &mut Consumer) -> Result<(), std::io::Error> {
    let parser = HttpdAccessLogParser::new();
    let f = File::open(name)?;
    match name.extension().map(|e| e.to_str() ) {
        Some(Some("gz")) => {
            let gunzip = GzDecoder::new(f)?;
            parser.process_lines(gunzip, consumer)
        },
        _ => parser.process_lines(f, consumer),
    }
}

fn is_gzip(key: &str, resp: &hyper::client::Response) -> bool {
    if let Some(&ContentType(hyper::mime::Mime(mime::TopLevel::Application, mime::SubLevel::Ext(ref ext), _))) = resp.headers.get() {
        ext=="gzip"
    } else if let Some(&ContentType(hyper::mime::Mime(mime::TopLevel::Ext(ref ext), mime::SubLevel::OctetStream, _))) = resp.headers.get() {
        ext=="binary" && key.ends_with(".gz")
    } else {
        false
    }
}

fn process_s3obj(client: &S3ClientWorkarounds<DefaultCredentialsProvider,Arc<Client>>, bucket: &str, obj: &s3::Object, consumer: &mut Consumer) -> Result<(), std::io::Error> {
    let parser = HttpdAccessLogParser::new();
    let mut req = s3::GetObjectRequest::default();
    req.bucket = bucket.to_string();
    req.key = obj.key.clone().unwrap();
    let resp = client
        .get_object(&req)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    if is_gzip(&req.key, &resp) {
        let gunzip = GzDecoder::new(resp)?;
        parser.process_lines(gunzip, consumer)
    } else {
        parser.process_lines(resp, consumer)
    }
}

enum Action {
    ProcessFile(PathBuf),
}

fn process_files(exp: PathExpression, options: PathMatchOptions) -> Result<(), std::io::Error> {
    let work_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let result_recv = {
        let (action_send, action_recv) = chan::async();
        let (result_send, result_recv) = chan::async();
        {
            let action_send = action_send.clone();
            let work_count = work_count.clone();
            thread::spawn (move || {
                let mut lim = 2000000;
                let mut matched = false;
                for entry in exp.list_local(options) {
                    match entry {
                        Ok(path) => {
                            matched = true;
                            action_send.send(Action::ProcessFile(path));
                            work_count.fetch_add(1, Ordering::AcqRel);
                        },
                        Err(e) => println!("{:?}", e),
                    }
                    lim -= 1;
                    if lim == 0 {
                        break;
                    }
                }
                if !matched {
                    println!("pattern did not match: {:?}", exp);
                }
            });
        }
        for _ in 0..6 {
            let action_recv = action_recv.clone();
            let result_send = result_send.clone();
            thread::spawn(move || {
                for action in action_recv {
                    match action {
                        Action::ProcessFile(path) => {
                            let mut consumer = Consumer::new();
                            let time = Instant::now();
                            process_file(&path, &mut consumer).unwrap();
                            let elapsed = time.elapsed();
                            let elapsed = elapsed.as_secs() * 1000 + elapsed.subsec_nanos() as u64 / 1000000;
                            println!("{} ({}ms)", path.display(), elapsed);
                            result_send.send(consumer);
                        },
                    }
                }
            });
        }
        result_recv
    };
    let mut reduced = Consumer::new();
    let mut completed: usize = 0;
    for result in result_recv {
        let remaining_work = work_count.fetch_sub(1, Ordering::AcqRel);
        reduced.merge(&result);
        completed += 1;
        println!("{} completed ({} known left)", completed, remaining_work);
    }
    {
        let mut f = File::create("by_status_timeslice.tsv")?;
        reduced.dump_by_status_timeslice(&mut f)?;
    }
    {
        let mut f = File::create("by_uritype_timeslice.tsv")?;
        reduced.dump_by_uritype_timeslice(&mut f)?;
    }
    {
        let mut f = File::create("servicetime_by_timeslice.tsv")?;
        reduced.dump_servicetimes_by_timeslice(&mut f)?;
    }
    Ok(())
}

fn s3client(region: Region) -> S3Client<DefaultCredentialsProvider,Client> {
    let provider = DefaultCredentialsProvider::new().unwrap();
    let mut http_client = default_tls_client().unwrap();
    http_client.set_read_timeout(Some(Duration::from_secs(5)));
    S3Client::new(http_client, provider, region)
}

fn s3client_workarounds(region: Region, http_client: Arc<Client>) -> S3ClientWorkarounds<DefaultCredentialsProvider,Arc<Client>> {
    let provider = DefaultCredentialsProvider::new().unwrap();
    S3ClientWorkarounds::new(http_client, provider, region)
}

fn process_s3(region: Region, bucket: &str, pathexp: PathExpression, options: PathMatchOptions) -> Result<(), std::io::Error> {
    let work_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let result_recv = {
        let (pathexp_send, pathexp_recv) = chan::async();
        {
            let pathexp_send = pathexp_send.clone();
            let bucket = bucket.to_string();
            let options = options.clone();
            thread::spawn (move || {
                let client = s3client(region);
                let mut lim = 2000000;
                let mut matched = false;
                for se in pathexp.specialise_first_element(client, &bucket, options.clone()) {
                    match se {
                        Ok(path) => {
                            matched = true;
                            pathexp_send.send(path);
                        },
                        Err(e) => println!("{:?}", e),
                    }
                    lim -= 1;
                    if lim == 0 {
                        break;
                    }
                }
                if !matched {
                    println!("pattern did not match: {:?}", pathexp);
                }
            });
        }
        let (s3obj_send, s3obj_recv) = chan::async();
        {
            // 2 threads for s3 listing
            // TODO: consider async rather than threading
            for _ in 0..2 {
                let pathexp_recv = pathexp_recv.clone();
                let s3obj_send = s3obj_send.clone();
                let bucket = bucket.to_string();
                let options = options.clone();
                let work_count = work_count.clone();
                thread::spawn(move || {
                    for pathexp in pathexp_recv {
                        let client = s3client(region);
                        for list_entry in pathexp.list_s3(client, &bucket, options.clone()) {
                            match list_entry {
                                Ok(obj) => {
                                    work_count.fetch_add(1, Ordering::AcqRel);
                                    s3obj_send.send(obj)
                                },
                                Err(e) => println!("Problem listing contents of S3: {:?}", e),
                            }
                        }
                    }
                });
            }
        }
        let (result_send, result_recv) = chan::async();
        let mut http = default_tls_client().unwrap();
        http.set_read_timeout(Some(Duration::from_secs(5)));
        let shared_http_client = Arc::new(http);
        for _ in 0..7 {
            let s3obj_recv = s3obj_recv.clone();
            let result_send = result_send.clone();
            let bucket = bucket.to_string();
            let http_client = shared_http_client.clone();
            thread::spawn(move || {
                let client = s3client_workarounds(region, http_client);
                for obj in s3obj_recv {
                    let mut consumer = Consumer::new();
                    let time = Instant::now();
                    process_s3obj(&client, &bucket, &obj, &mut consumer).unwrap();
                    let elapsed = time.elapsed();
                    let elapsed = elapsed.as_secs() * 1000 + elapsed.subsec_nanos() as u64 / 1000000;
                    println!("{} ({}ms)", obj.key.unwrap(), elapsed);
                    result_send.send(consumer);
                }
            });
        }
        result_recv
    };
    let mut reduced = Consumer::new();
    let mut completed: usize = 0;
    for result in result_recv {
        let remaining_work = work_count.fetch_sub(1, Ordering::AcqRel);
        reduced.merge(&result);
        completed += 1;
        println!("{} completed ({} known left)", completed, remaining_work);
    }
    {
        let mut f = File::create("by_status_timeslice.tsv")?;
        reduced.dump_by_status_timeslice(&mut f)?;
    }
    {
        let mut f = File::create("by_uritype_timeslice.tsv")?;
        reduced.dump_by_uritype_timeslice(&mut f)?;
    }
    {
        let mut f = File::create("servicetime_by_timeslice.tsv")?;
        reduced.dump_servicetimes_by_timeslice(&mut f)?;
    }
    Ok(())
}

fn parse_datetime(datetime: &str) -> Result<time::Tm, time::ParseError> {
    strptime(datetime, "%Y-%m-%d:%H:%M:%S")
}

fn range_to_opts(range: Option<&str>) -> Result<PathMatchOptions, time::ParseError> {
    let mut options = PathMatchOptions::new();
    if let Some(range) = range {
        let mut i = range.split("..");
        if let Some(datetime) = i.next() {
            parse_datetime(datetime)
                .and_then(|from| Ok(options.from(from)) )?;
        }
        if let Some(datetime) = i.next() {
            parse_datetime(datetime)
                .and_then(|to| Ok(options.to(to)) )?;
        }
    }
    Ok(options)
}

fn main() {
    let matches = App::new("whatf")
        .about("log log crunch crunch burp")
        .version(crate_version!())
        .arg(Arg::with_name("period")
             .long("period")
             .value_name("RANGE")
             .help("YYYY-MM-DD:hh:mm:ss..YYYY-MM-DD:hh:mm:ss"))
        .arg(Arg::with_name("source")
             .long("source")
             .value_name("SOURCE NAME")
             .help("name of a source from datasources.toml"))
        .get_matches();

    let _ = env_logger::init();

    let source_name = matches.value_of("source").expect("A --source argument must be supplied");
    let sources = datasource::get_datasources().unwrap();
    let source = sources.s3.iter().find(|s| s.name == source_name);
    let options = range_to_opts(matches.value_of("period")).expect("bad --range value");
    if let Some(s3source) = source {
        let expr = PathExpression::parse(&s3source.pathexp).unwrap();
        let time = Instant::now();
        let region = s3source.region.parse::<Region>();
        if let Err(_) = region {
            println!("Invalid AWS region: {:?}", s3source.region);
            return;
        }
        process_s3(region.unwrap(), &s3source.bucket, expr, options).unwrap();
        let elapsed = time.elapsed();
        let elapsed = elapsed.as_secs() * 1000 + elapsed.subsec_nanos() as u64 / 1000000;
        println!("Complete in {} ms", elapsed);
        return;
    }
    let source = sources.file.iter().find(|s| s.name == source_name);
    if let Some(filesource) = source {
        let expr = PathExpression::parse(&filesource.pathexp).unwrap();
        process_files(expr, options).unwrap();
        return;
    }


}
