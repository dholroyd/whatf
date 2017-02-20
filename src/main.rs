extern crate time;
extern crate flate2;
extern crate glob;
extern crate chan;

mod parse_access_log;

use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use flate2::read::GzDecoder;
use parse_access_log::HttpdAccessLogParser;
use parse_access_log::Consumer;
use glob::glob;
use std::time::Instant;
use std::thread;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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

enum Action {
    ProcessFile(PathBuf),
}

fn process(fileglob: &str) -> Result<(), std::io::Error> {
    let work_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let result_recv = {
        let (action_send, action_recv) = chan::async();
        let (result_send, result_recv) = chan::async();
        {
            let action_send = action_send.clone();
            let work_count = work_count.clone();
            let fileglob = fileglob.to_string();
            thread::spawn (move || {
                let mut lim = 200;
                let mut matched = false;
                for entry in glob(&fileglob).expect("Failed to read glob pattern") {
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
                    println!("pattern did not match: {}", fileglob);
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
    let mut f = try!(File::create("report.tsv"));
    reduced.dump(&mut f)?;
    Ok(())
}

fn main() {
    let mut args = std::env::args();
    args.next();
    match args.next() {
        Some(a) => {
            let time = Instant::now();
            process(&a).unwrap();
            let elapsed = time.elapsed();
            let elapsed = elapsed.as_secs() * 1000 + elapsed.subsec_nanos() as u64 / 1000000;
            println!("Complete in {} ms", elapsed);
        },
        None => println!("missing fileglob argument"),
    }
}

