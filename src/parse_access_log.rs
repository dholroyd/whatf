use std::str::from_utf8;
use time::strptime;
use time::Timespec;
use std::io::Error;
use std::io::Read;
use std::io::ErrorKind;

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use regex::RegexSet;
use urlparse;

fn string_from_slice(slice: &[u8]) -> Result<&str,Error> {
    from_utf8(slice).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

pub struct HttpdAccessLogParser {
}
impl HttpdAccessLogParser {
    pub fn new() -> HttpdAccessLogParser {
        HttpdAccessLogParser { }
    }

    // LogFormat "%t %h %l %u \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Host}i\" %D \"%{X-Forwarded-For}i\" %{local}p %{cache-status}e %R" combined
    pub fn process_lines<T: Read>(&self, mut data: T, consumer: &mut Consumer) -> Result<(), Error> {
        let mut buf = Vec::new();
        data.read_to_end(&mut buf)?;
        let buf = buf;
        let mut lineno: u64 = 0;
        let mut idx = 0;
        // TODO: line endings!
        'lines: while idx < buf.len() {
            lineno += 1;
            if buf[idx] != b'[' {
                println!("timestamp not there at line start {:?}", &buf[idx..idx+10]);
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }
            idx += 1;
            let mut line_date:Option<Timespec> = None;
            let start = idx;
            while idx < buf.len() {
                if buf[idx] == b']' {
                    let dat = string_from_slice(&buf[start..idx])?;
                    let date = strptime(&dat, "%d/%b/%Y:%H:%M:%S %z");
                    if let Err(e) = date {
                        println!("date error line {}: {}", lineno, e);
                        println!("timestamp");
                        skip_to_eol(&buf, &mut idx);
                        continue 'lines;
                    }
                    line_date = Some(date.unwrap().to_timespec());
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            if let None = line_date {
                println!("timestamp not found in line");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }

            if !expect(&buf, &mut idx, b' ') {
                println!("after timestamp");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }

            let remote_host = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("remote_host"))?;
            let remote_logname = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("logname"))?;
            let remote_user = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("user"))?;
            let request_line = expect_field_qot(&buf, &mut idx).ok_or(invalid_data("reqest-line"))?;
            let mut itr = string_from_slice(request_line)?.split_whitespace();
            let request_method = itr.next().ok_or(invalid_data("method"))?;
            let request_uri = itr.next().ok_or(invalid_data("uri"))?;
            let request_proto = itr.next().ok_or(invalid_data("protocol"))?;
            if !expect(&buf, &mut idx, b' ') {
                println!("before status");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }
            let response_status = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("status"))?;
            let bytes = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("bytes"))?;
            let response_bytes: Option<usize> = match bytes {
                b"-" => None,
                _ => Some(string_from_slice(bytes)?.parse().map_err(|_| invalid_data(&format!("bytes: {:?}", bytes)))?),
            };
            let request_referer = expect_field_qot(&buf, &mut idx).ok_or(invalid_data("referer"))?;
            if !expect(&buf, &mut idx, b' ') {
                println!("before agent");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }
            let request_useragent = expect_field_qot(&buf, &mut idx).ok_or(invalid_data("agent"))?;
            if !expect(&buf, &mut idx, b' ') {
                println!("before request_host");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }
            let request_host = expect_field_qot(&buf, &mut idx).ok_or(invalid_data("request_host"))?;
            if !expect(&buf, &mut idx, b' ') {
                println!("before service-time");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }
            let micros = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("service-time"))?;
            let response_time_micros: u64 = string_from_slice(micros)?.parse().map_err(|_| invalid_data("service-time"))?;
            let request_forwarded_for = expect_field_qot(&buf, &mut idx).ok_or(invalid_data("forwarded-for"))?;
            if !expect(&buf, &mut idx, b' ') {
                println!("before port");
                skip_to_eol(&buf, &mut idx);
                continue 'lines;
            }
            let port = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("port"))?;
            let request_local_port: u32 = string_from_slice(port)?.parse().map_err(|_| invalid_data("port"))?;
            let response_cache_status = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("cache-status"))?;
            let request_handler = expect_field_ws(&buf, &mut idx).ok_or(invalid_data("handler"))?;

            consumer.handle(Record{
                timestamp: line_date.unwrap(),
                remote_host: string_from_slice(remote_host)?.to_string(),
                remote_logname: string_from_slice(remote_logname)?.to_string(),
                remote_user: string_from_slice(remote_user)?.to_string(),
                request_method: request_method.to_string(),
                request_uri: urlparse::unquote(request_uri).map_err(|_| invalid_data("failed to urldecode request uri"))?,
                request_proto: request_proto.to_string(),
                response_status: string_from_slice(response_status)?.to_string(),
                response_bytes: response_bytes,
                request_referer: string_from_slice(request_referer)?.to_string(),
                request_useragent: string_from_slice(request_useragent)?.to_string(),
                request_host: string_from_slice(request_host)?.to_string(),
                response_time_micros: response_time_micros,
                request_forwarded_for: string_from_slice(request_forwarded_for)?.to_string(),
                request_local_port: request_local_port,
                response_cache_status: string_from_slice(response_cache_status)?.to_string(),
                request_handler: string_from_slice(request_handler)?.to_string(),
            });
        }
        Ok(())
    }
}

fn expect(data: &[u8], idx: &mut usize, expected: u8) -> bool {
    let m = data[*idx] == expected;
    *idx += 1;
    m
}

fn skip_to_eol<'a>(data: &'a [u8], idx: &mut usize) {
    let start = *idx;
    for i in start..data.len() {
        if data[i] == b'\n' {
            *idx = i+1;
            return;
        }
    }
    *idx = data.len();
}
fn expect_field_ws<'a>(data: &'a [u8], idx: &mut usize) -> Option<(&'a [u8])> {
    let start = *idx;
    for i in start..data.len() {
        // TODO: line endings
        if data[i] == b' ' || data[i] == b'\n' {
            *idx = i+1;
            return Some(&data[start..i]);
        }
    }
    *idx = data.len();
    Some(&data[start..])
}

fn expect_field_qot<'a>(data: &'a [u8], mut idx: &mut usize) -> Option<(&'a [u8])> {
    if !expect(data, &mut idx, b'"') {
        return None;
    }
    let start = *idx + 1;
    let mut i = start;
    while i < data.len() {
        if data[i] == b'\\' {
            i += 1;
        } else if data[i] == b'"' {
            *idx = i+1;
            return Some(&data[start..i]);
        }
        i += 1;
    }
    None
}

#[derive(Debug)]
pub struct Record {
    pub timestamp: Timespec,
    pub remote_host: String,
    pub remote_logname: String,
    pub remote_user: String,
    pub request_method: String,
    pub request_uri: String,
    pub request_proto: String,
    pub response_status: String,
    pub response_bytes: Option<usize>,
    pub request_referer: String,
    pub request_useragent: String,
    pub request_host: String,
    pub response_time_micros: u64,
    pub request_forwarded_for: String,
    pub request_local_port: u32,
    pub response_cache_status: String,
    pub request_handler: String,
}

fn invalid_data(msg: &str) -> Error {
    Error::new(ErrorKind::InvalidData, msg.to_string())
}

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
    by_status_timeslice: HashMap<KeyStatusTimeslice,u64>,
    timeslices: HashSet<i64>,
    statuses: HashSet<String>,
    by_uritype_timeslice: HashMap<KeyUritypeTimeslice,u64>,
    uritypes: HashSet<UriType>,
}

impl Consumer {
    pub fn new() -> Consumer {
        Consumer {
            by_status_timeslice: HashMap::new(),
            by_uritype_timeslice: HashMap::new(),
            timeslices: HashSet::new(),
            statuses: HashSet::new(),
            uritypes: HashSet::new(),
        }
    }
    pub fn handle(&mut self, r: Record) {
        let slice = timeslice(r.timestamp, 300);
        let key_status_timeslice = KeyStatusTimeslice {
            timeslice: slice,
            http_status: r.response_status.clone(),
        };
        *self.by_status_timeslice.entry(key_status_timeslice).or_insert(0) += 1;
        self.timeslices.insert(slice);
        self.statuses.insert(r.response_status);

        let uritype = classify(&r.request_uri);
        let key_uritype_timeslice = KeyUritypeTimeslice {
            timeslice: slice,
            uritype: uritype,
        };
        *self.by_uritype_timeslice.entry(key_uritype_timeslice).or_insert(0) += 1;
        self.uritypes.insert(uritype);
    }

    pub fn merge(&mut self, other: &Consumer) {
        for timeslice in other.timeslices.iter() {
            self.timeslices.insert(*timeslice);
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
}

fn timeslice(t: Timespec, seconds: i64) -> i64 {
    (t.sec / seconds) * seconds
}

