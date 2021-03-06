use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use regex::RegexSet;
use parse_access_log::Record;
use std::io::Error;
use time::Timespec;
use hdrsample::Histogram;

#[derive(Debug,Clone,Copy,Hash,Eq,PartialEq)]
enum UriType {
    HdsBootstrap,
    HlsSegment,
    HdsSegment,
    HlsMediaManifest,
    HlsMasterManifest,
    HdsF4mManifest,
    DashInitialisationSegment,
    DashSegment,
    DashManifest,
    Admin,
    UnknownOther,
}

fn classify(uri: &str) -> UriType {
    lazy_static! {
        static ref RSET: RegexSet = RegexSet::new(&[
            r"\.bootstrap",
            r"/[^/]+.ts",
            r"-Seg1-Frag(\d+)",
            r"(?:audio=|video=)[^/]+\.m3u8",
            r"\.m3u8",
            r"\.f4m",
            r"\.dash",
            r"\.m4s",
            r"\.mpd",
            r"/test\.txt$|/Manifest?iss_client_manifest_version=22$|/archive-segment-length-seconds$|/state$|/statistics$|/servicePaths.txt$|/server-status$",
        ]).unwrap();
    }

    match RSET.matches(uri).into_iter().next() {
        Some(0) => UriType::HdsBootstrap,
        Some(1) => UriType::HlsSegment,
        Some(2) => UriType::HdsSegment,
        Some(3) => UriType::HlsMediaManifest,
        Some(4) => UriType::HlsMasterManifest,
        Some(5) => UriType::HdsF4mManifest,
        Some(6) => UriType::DashInitialisationSegment,
        Some(7) => UriType::DashSegment,
        Some(8) => UriType::DashManifest,
        Some(9) => UriType::Admin,
        Some(n) => panic!("Unexpected RegexSet index {}", n),
        None    => UriType::UnknownOther,
    }
}

#[derive(Debug,Clone,Hash,Eq,PartialEq)]
struct KeyStatusTimeslice {
    timeslice: i64,
    http_status: String,
}

#[derive(Debug,Clone,Hash,Eq,PartialEq)]
struct KeyUritypeTimeslice {
    timeslice: i64,
    uritype: UriType,
}

pub struct Consumer {
    servicetime_hist_by_timeslice: HashMap<i64,Histogram<u64>>,
    servicetime_hist: Histogram<u64>,
    by_status_timeslice: HashMap<KeyStatusTimeslice,u64>,
    timeslices: HashSet<i64>,
    statuses: HashSet<String>,
    by_uritype_timeslice: HashMap<KeyUritypeTimeslice,u64>,
    uritypes: HashSet<UriType>,
}

impl Consumer {
    pub fn new() -> Consumer {
        Consumer {
            servicetime_hist_by_timeslice: HashMap::new(),
            servicetime_hist: Histogram::new(1).unwrap(),
            by_status_timeslice: HashMap::new(),
            by_uritype_timeslice: HashMap::new(),
            timeslices: HashSet::new(),
            statuses: HashSet::new(),
            uritypes: HashSet::new(),
        }
    }
    pub fn handle(&mut self, r: Record) {
        let slice = timeslice(r.timestamp, 1200);
        self.timeslices.insert(slice);
        self.record_http_status(slice, r.response_status);
        self.record_uritype(slice, &r.request_uri);
        self.record_service_time(slice, r.response_time_micros);
    }
    fn record_http_status(&mut self, slice: i64, response_status: String) {
        let key_status_timeslice = KeyStatusTimeslice {
            timeslice: slice,
            http_status: response_status.clone(),
        };
        *self.by_status_timeslice.entry(key_status_timeslice).or_insert(0) += 1;
        self.statuses.insert(response_status);
    }
    fn record_uritype(&mut self, slice: i64, request_uri: &str) {
        let uritype = classify(request_uri);
        let key_uritype_timeslice = KeyUritypeTimeslice {
            timeslice: slice,
            uritype: uritype,
        };
        *self.by_uritype_timeslice.entry(key_uritype_timeslice).or_insert(0) += 1;
        self.uritypes.insert(uritype);
    }
    fn record_service_time(&mut self, slice: i64, response_time_micros: u64) {
        self.servicetime_hist_by_timeslice.entry(slice).or_insert_with(|| Histogram::new(1).unwrap()).record(response_time_micros);
        self.servicetime_hist.record(response_time_micros);
    }

    pub fn merge(&mut self, other: &Consumer) {
        for timeslice in other.timeslices.iter() {
            self.timeslices.insert(*timeslice);
            let other_times = other.servicetime_hist_by_timeslice.get(timeslice).unwrap();
            self.servicetime_hist_by_timeslice.entry(*timeslice).or_insert_with(|| Histogram::new(1).unwrap()).add(other_times);
        }
        for status in other.statuses.iter() {
            self.statuses.insert(status.clone());
        }
        for (k, v) in other.by_status_timeslice.iter() {
            *self.by_status_timeslice.entry(k.clone()).or_insert(0) += *v;
        }
        for uritype in other.uritypes.iter() {
            self.uritypes.insert(*uritype);
        }
        for (k, v) in other.by_uritype_timeslice.iter() {
            *self.by_uritype_timeslice.entry(k.clone()).or_insert(0) += *v;
        }
        self.servicetime_hist.add(&other.servicetime_hist);
    }

    pub fn dump_by_status_timeslice(&self, out: &mut Write) -> Result<(),Error>{
        let cols = self.statuses.iter().collect::<Vec<&String>>();
        write!(out, "timeslice")?;
        for c in &cols {
            write!(out, "\t{}", c)?;
        }
        writeln!(out, "")?;
        let mut timeslices = self.timeslices.iter().map(|ts| *ts ).collect::<Vec<i64>>();
        timeslices.sort();
        for ts in timeslices.iter() {
            write!(out, "{}\t", ts)?;
            for c in &cols {
                let key = KeyStatusTimeslice {
                    timeslice: *ts,
                    http_status: (*c).clone(),
                };
                let def = 0;
                let val = self.by_status_timeslice.get(&key).unwrap_or(&def);
                write!(out, "{}\t", val)?;
            }
            writeln!(out, "")?;
        }
        Ok(())
    }

    pub fn dump_by_uritype_timeslice(&self, out: &mut Write) -> Result<(),Error>{
        let cols = self.uritypes.iter().collect::<Vec<&UriType>>();
        write!(out, "timeslice")?;
        for c in &cols {
            write!(out, "\t{:?}", c)?;
        }
        writeln!(out, "")?;
        let mut timeslices = self.timeslices.iter().map(|ts| *ts ).collect::<Vec<i64>>();
        timeslices.sort();
        for ts in timeslices.iter() {
            write!(out, "{}\t", ts)?;
            for c in &cols {
                let key = KeyUritypeTimeslice {
                    timeslice: *ts,
                    uritype: (*c).clone(),
                };
                let def = 0;
                let val = self.by_uritype_timeslice.get(&key).unwrap_or(&def);
                write!(out, "{}\t", val)?;
            }
            writeln!(out, "")?;
        }
        Ok(())
    }

    pub fn dump_servicetimes_by_timeslice(&self, out: &mut Write) -> Result<(),Error>{
        let cols = self.servicetime_hist.iter_recorded().map(|v| v.value()).collect::<Vec<u64>>();
        write!(out, "timeslice")?;
        for c in &cols {
            write!(out, "\t{:?}", c)?;
        }
        writeln!(out, "")?;
        let mut timeslices = self.timeslices.iter().map(|ts| *ts ).collect::<Vec<i64>>();
        timeslices.sort();
        for ts in timeslices.iter() {
            write!(out, "{}\t", ts)?;
            let hist = self.servicetime_hist_by_timeslice.get(ts).unwrap();
            for c in &cols {
                write!(out, "{}\t", hist.count_at(*c).unwrap())?;
            }
            writeln!(out, "")?;
        }
        Ok(())
    }
}

fn timeslice(t: Timespec, seconds: i64) -> i64 {
    (t.sec / seconds) * seconds
}
