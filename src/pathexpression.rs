use std::str::from_utf8;
use std::str::FromStr;
use std::path::PathBuf;
use std::path::Path;
use std::io;
use std::vec::IntoIter;
use nom::IResult;
use regex;
use time;
use std::collections::HashSet;
use std::env;

use rusoto_workarounds;
use rusoto_workarounds::s3::S3ClientWorkarounds;
use rusoto_workarounds::request::DispatchSignedRequestWorkaround;
use rusoto::s3;
use rusoto::ProvideAwsCredentials;


#[derive(Debug)]
pub struct GlobError {
    path: PathBuf,
    error: io::Error,
}

#[derive(Debug)]
pub struct ListLocal {
    pathexp: PathExpression,
    todo: Vec<(PathBuf, usize)>,
    scope: Option<PathBuf>,
    opts: PathMatchOptions,
    ctx: MatchContext,
}

// ---- local filesystem ----

fn fill_todo(todo: &mut Vec<(PathBuf, usize)>,
             elements: &[PathElement],
             idx: usize,
             path: &Path,
             ctx: &mut MatchContext) {

    // TODO: pass errors back to caller in todo list
    let element = &elements[idx];
    if element.has_placeholders() {
        match path.read_dir() {
            Ok(read) => {
                let entries = read
                    .map(|entry| entry.unwrap(/*TODO*/).file_name() )
                    .filter(|name| element.matches(ctx, name.to_str().unwrap(/*TODO*/)) )
                    .map(|name| (path.join(&name), idx) );
                todo.extend(entries);
            },
            Err(e) => {
                panic!("read_dir failed: {:?}", e);
            },
        }
    } else {
        if let PathElementPart::Literal(ref p) = element.parts[0] {
            let next = path.join(p);
            if next.exists() {
                if idx+1 == elements.len() {
                    todo.push( (next, !0 as usize) );
                } else {
                    fill_todo(todo, elements, idx+1, &next, ctx);
                }
            }
        } else {
            panic!("expected a single literal path element, but I have #{:?}", element.parts[0]);
        }
    }
}

impl Iterator for ListLocal {
    type Item = Result<PathBuf, GlobError>;

    fn next(&mut self) -> Option<Result<PathBuf, GlobError>> {
        if let Some(scope) = self.scope.take() {
            if self.pathexp.elements.len() > 0 {
                fill_todo(&mut self.todo,
                          &self.pathexp.elements,
                          0,
                          &scope,
                          &mut self.ctx);
            }
        }
        loop {
            match self.todo.pop() {
                None => return None,
                Some((path, idx)) => {
                    if idx == self.pathexp.elements.len()-1 {
                        return Some(Ok(path));
                    }
                    if path.is_dir() {
                        fill_todo(&mut self.todo,
                                  &self.pathexp.elements,
                                  idx+1,
                                  &path,
                                  &mut self.ctx);
                    }
                }
            }
        }
    }
}


// ---- S3 ----


/// No retries implemented by rusoto or hyper, so implement retrying here
fn list_objects<P,D>(client: &S3ClientWorkarounds<P,D>, req: &s3::ListObjectsRequest) -> Result<s3::ListObjectsOutput, rusoto_workarounds::s3::ListObjectsError>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    let mut tries = 3;
    loop {
        let resp = client.list_objects(&req);
        match &resp {
            &Err(ref e) => match e {
                &rusoto_workarounds::s3::ListObjectsError::HttpDispatch(_) => (),
                _ => tries = 0,
            },
            _ => tries = 0,
        }
        tries -= 1;
        if tries <= 0 {
            return resp;
        }
    }
}


pub struct ListS3<P,D>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    client: S3ClientWorkarounds<P,D>,
    bucket: String,
    pathexp: PathExpression,
    prefix: String,
    current_batch: Option<IntoIter<s3::Object>>,
    last_key: Option<String>,
    ended: bool,
    final_batch: bool,
}

impl <P,D> ListS3<P,D>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    fn new(client: S3ClientWorkarounds<P,D>, bucket: &str, pathexp: PathExpression) -> ListS3<P,D>
        where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
    {
        ListS3 {
            client: client,
            bucket: bucket.to_string(),
            prefix: pathexp.common_prefix(),
            pathexp: pathexp,
            current_batch: None,
            last_key: None,
            ended: false,
            final_batch: false,
        }
    }

    fn next_batch(&mut self) -> Result<IntoIter<s3::Object>, io::Error> {
        let mut req = s3::ListObjectsRequest::default();
        req.bucket = self.bucket.clone();
        req.prefix = Some(self.prefix.clone());
        if self.last_key.is_some() {
            req.marker = self.last_key.take();
        }
        match list_objects(&self.client, &req) {
            Ok(objects) => {
                match objects.contents {
                    Some(contents) => {
                        if let Some(true) = objects.is_truncated {
                            let last = contents.iter().last().unwrap();
                            if let Some(ref key) = last.key {
                                self.last_key = Some(key.clone());
                            } else {
                                self.last_key = None;
                            }
                        } else {
                            self.last_key = None;
                            self.final_batch = true;
                        }
                        Ok(contents.into_iter())
                    },
                    None => {
                        Err(io::Error::new(io::ErrorKind::Other, "S3 response contains no contents"))
                    },
                }
            },
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

impl <P,D> Iterator for ListS3<P,D>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    type Item = Result<s3::Object, GlobError>;

    fn next(&mut self) -> Option<Result<s3::Object, GlobError>> {
        loop {
            if self.ended {
                return None;
            }
            let mut end_of_batch = false;
            if let Some(ref mut batch) = self.current_batch {
                let next = batch.next();
                if let Some(o) = next {
                    let is_match = if let s3::Object{key: Some(ref key), ..} = o {
                        self.pathexp.is_match(key)
                    } else {
                        false
                    };
                    if is_match {
                        return Some(Ok(o));
                    }
                } else {
                    end_of_batch = true;
                }
            } else {
                match self.next_batch() {
                    Ok(batch) => {
                        self.current_batch = Some(batch);
                    },
                    Err(e) => {
                        self.ended = true;
                        return Some(Err(GlobError{path: PathBuf::from(&self.prefix), error: e}))
                    }
                }
            }
            if end_of_batch {
                self.current_batch = None;
                if self.final_batch {
                    self.ended = true;
                }
            }
        }
    }
}

pub struct SpecialiseS3<P,D>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    client: S3ClientWorkarounds<P,D>,
    bucket: String,
    pathexp: PathExpression,
    first_with_variable: PathElement,
    prefix: String,
    current_batch: Option<IntoIter<s3::CommonPrefix>>,
    last_key: Option<String>,
    ended: bool,
    final_batch: bool,
    ctx: MatchContext,
}

// TODO: given "../{%Y}-{%m}-{%d}/{instanceid}/..", how to do something sensible for specialisation
//       when the time range covers multiple days etc?

impl <P,D> SpecialiseS3<P,D>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    fn new(client: S3ClientWorkarounds<P,D>, bucket: &str, pathexp: PathExpression, ctx: MatchContext) -> SpecialiseS3<P,D>
        where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
    {
        SpecialiseS3 {
            client: client,
            bucket: bucket.to_string(),
            prefix: pathexp.common_prefix(),
            first_with_variable: pathexp.elements.iter().filter(|e| e.has_variable() ).next().unwrap(/*TODO: what if no placeholders?*/).clone(),
            pathexp: pathexp,
            current_batch: None,
            last_key: None,
            ended: false,
            final_batch: false,
            ctx: ctx,
        }
    }

    fn next_batch(&mut self) -> Result<IntoIter<s3::CommonPrefix>, io::Error> {
        let mut req = s3::ListObjectsRequest::default();
        req.bucket = self.bucket.clone();
        req.prefix = Some(self.prefix.clone());
        req.delimiter = Some("/".to_string());
        if self.last_key.is_some() {
            req.marker = self.last_key.take();
        }

        match list_objects(&self.client, &req) {
            Ok(prefixes) => {
                match prefixes.common_prefixes {
                    Some(contents) => {
                        if let Some(true) = prefixes.is_truncated {
                            if let Some(ref marker) = prefixes.next_marker {
                                self.last_key = Some(marker.clone());
                            } else {
                                self.last_key = None;
                            }
                        } else {
                            self.last_key = None;
                            self.final_batch = true;
                        }
                        Ok(contents.into_iter())
                    },
                    None => {
                        Err(io::Error::new(io::ErrorKind::Other, "S3 response contains no common prefixes"))
                    },
                }
            },
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

fn create_specialised(pathexp: &PathExpression, literal_val: &str) -> PathExpression {
    let mut newelements = Vec::new();
    let mut have_replaced_first = false;
    for elem in pathexp.elements.iter() {
        let mut newparts = Vec::new();
        for part in elem.parts.iter() {
            let newpart = match part {
                &PathElementPart::Literal(_) => part.clone(),
                &PathElementPart::TimePart{ .. } => part.clone(),
                &PathElementPart::Placeholder{ value: Some(_), .. } => part.clone(),
                &PathElementPart::Placeholder{ ref name, value: None } => {
                    if have_replaced_first {
                        part.clone()
                    } else {
                        have_replaced_first = true;
                        PathElementPart::Placeholder{ name: name.clone(), value: Some(literal_val.to_string()) }
                    }
                },
            };
            newparts.push(newpart);
        }
        newelements.push(PathElement::new(newparts));
    }
    PathExpression {
        leading_sep: pathexp.leading_sep,
        trailing_sep: pathexp.trailing_sep,
        elements: newelements,
        opts: pathexp.opts.clone(),
    }
}

impl <P,D> Iterator for SpecialiseS3<P,D>
    where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
{
    type Item = Result<PathExpression, GlobError>;

    fn next(&mut self) -> Option<Result<PathExpression, GlobError>> {
        loop {
            if self.ended {
                return None;
            }
            let mut end_of_batch = false;
            if let Some(ref mut batch) = self.current_batch {
                let next = batch.next();
                if let Some(o) = next {
                    if let s3::CommonPrefix{prefix: Some(ref prefix), ..} = o {
                        let last_element = Path::new(prefix).file_name().unwrap().to_str().unwrap();
                        if self.first_with_variable.matches(&mut self.ctx, last_element) {
                            return Some(Ok(create_specialised(&self.pathexp, last_element)));
                        }
                    }
                } else {
                    end_of_batch = true;
                }
            } else {
                match self.next_batch() {
                    Ok(batch) => {
                        self.current_batch = Some(batch);
                    },
                    Err(e) => {
                        self.ended = true;
                        return Some(Err(GlobError{path: PathBuf::from(&self.prefix), error: e}))
                    }
                }
            }
            if end_of_batch {
                self.current_batch = None;
                if self.final_batch {
                    self.ended = true;
                }
            }
        }
    }
}

// ---- generic interface ----


#[derive(Debug, Clone)]
pub enum PathElementPart {
    Literal (
        String
    ),
    Placeholder {
        name: String,
        value: Option<String>,
    },
    TimePart {
        fmt: String,
        value: Option<String>,
        last_in_expression: bool,
    },
}

impl PathElementPart {
    fn is_literal(&self) -> bool {
        if let &PathElementPart::Literal(_) = self {
            true
        } else {
            false
        }
    }

    fn is_variable(&self) -> bool {
        match self {
            &PathElementPart::Literal(_) => false,
            &PathElementPart::TimePart{ value: Some(_), ..} => false,
            &PathElementPart::TimePart{ value: None, ..} => true,
            &PathElementPart::Placeholder{ value: Some(_), ..} => false,
            &PathElementPart::Placeholder{..} => true,
        }
    }

    fn is_timepart(&self) -> bool {
        if let &PathElementPart::TimePart{..} = self {
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathElement {
    parts: Vec<PathElementPart>,
    placeholders: bool,
    variable: bool,
    timeparts: bool,
    re: regex::Regex,
}

impl PathElement {
    fn new(parts: Vec<PathElementPart>) -> PathElement {
        let placeholders = parts.iter().find(|p| !p.is_literal() ).map(|_| true).unwrap_or(false);
        let variable = parts.iter().find(|p| p.is_variable() ).map(|_| true).unwrap_or(false);
        let timeparts = parts.iter().find(|p| p.is_timepart() ).map(|_| true).unwrap_or(false);
        let re_str = PathElement::to_regex_string(&parts);
        let re = regex::Regex::new(&re_str).unwrap();
        PathElement { parts: parts, placeholders: placeholders, variable: variable, timeparts: timeparts, re: re }
    }

    fn has_placeholders(&self) -> bool {
        self.placeholders
    }

    fn has_variable(&self) -> bool {
        self.variable
    }

    fn has_timeparts(&self) -> bool {
        self.timeparts
    }

    fn to_regex_string(parts: &[PathElementPart]) -> String {
        let mut acc = String::new();
        acc.push_str("^");
        for part in parts.iter() {
            match part {
                &PathElementPart::Literal(ref s) => {
                    acc.push_str(&regex::escape(s));
                },
                &PathElementPart::Placeholder{..} => {
                    acc.push_str("(.*)");
                },
                &PathElementPart::TimePart{ref value, ..} => {
                    match *value {
                        Some(ref v) => {
                            acc.push_str(&v);
                        },
                        None => {
                            // TODO: allow non-numeric patterns when more format characters are supported
                            acc.push_str("(\\d+)");
                        },
                    }
                },
            }
        };
        acc.push_str("$");
        acc
    }

    fn common_prefix(&self, prefix: &mut String) -> bool {
        for part in self.parts.iter() {
            match part {
                &PathElementPart::Literal(ref s) => prefix.push_str(s),
                &PathElementPart::Placeholder{ref value, ..} => {
                    match *value {
                        Some(ref v) => prefix.push_str(&v),
                        None => return false,
                    }
                },
                &PathElementPart::TimePart{ref value, ..} => {
                    // TODO: two time values could share a common prefix even if they are not equal
                    // overall e.g. the common '1' prefix on the hours '11' and '12'
                    match *value {
                        Some(ref v) => prefix.push_str(&v),
                        None => return false,
                    }
                },
            }
        }
        true
    }

    fn matches(&self, ctx: &mut MatchContext, name: &str) -> bool {
        if self.has_timeparts() {
            let captures = self.re.captures(name);
            if captures.is_none() {
                return false;
            }
            let captures = captures.unwrap();
            let mut capture_strings = captures.iter().skip(1).map(|c| c.unwrap().as_str() );
            for part in self.parts.iter() {
                if let &PathElementPart::TimePart{ ref fmt, ref value, ref last_in_expression, .. } = part {
                    let c = fmt.chars().next().unwrap(/*TODO*/);
                    match value {
                        &Some(ref v) => ctx.set_time_part(c, i32::from_str(v).unwrap(/*TODO*/)),  // TODO: we don't need to set these for every match!  just specify these values once on context init
                        &None => {
                            if let Some(cap) = capture_strings.next() {
                                // TODO: strptime() instread of from_str()?
                                if let Ok(num) = i32::from_str(cap) {
                                    ctx.set_time_part(c, num);
                                } else {
                                    // not numeric when use of a time-format required to be, so match failed
                                    return false;
                                }
                            } else {
                                // TODO: not sure I've thought this case through well enough yet -
                                //       under what circumstances could we run out of captures?
                                return false;
                            }
                        },
                    }
                    // TODO: only checking that the provided time-elements fall within the provided
                    // limits when we've got all of them misses opportunities to fail earlier when
                    // the time-elements seen near the start of the expression are clearly bogus.
                    // We can't perform that smarter checking simply by comparing time::Tm
                    // instances though -- we'd need to track what Tm elements have been supplied,
                    // and only compare vs. the limits taking into account the precision with which
                    // we know the time at this point into the path (time-elements not being in
                    // decreasing order of magnitude within the expression probably complicates
                    // that?)
                    if *last_in_expression {
                        if let Some(from) = ctx.from {
                            if from > ctx.match_time {
                                return false
                            }
                        }
                        if let Some(to) = ctx.to {
                            if to < ctx.match_time {
                                return false
                            }
                        }
                    }
                }
            }
            self.re.is_match(name)
        } else {
            self.re.is_match(name)
        }
    }
}

#[derive(Debug)]
struct MatchContext {
    match_time: time::Tm,
    from: Option<time::Tm>,
    to: Option<time::Tm>,
}

impl MatchContext {
    fn new (opts: &PathMatchOptions) -> MatchContext {
        let time = time::Tm {
            tm_sec: 0,
            tm_min: 0,
            tm_hour: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
            tm_wday: 0,
            tm_yday: 0,
            tm_isdst: 0,
            tm_utcoff: 0,
            tm_nsec: 0,
        };
        MatchContext {
            match_time: time,
            from: opts.from.clone(),
            to: opts.to.clone(),
        }
    }

    pub fn set_time_part(&mut self, fmt: char, part: i32) {
        match fmt {
            'S' => self.match_time.tm_sec = part,
            'M' => self.match_time.tm_min = part,
            'H' => self.match_time.tm_hour = part,
            'd' => self.match_time.tm_mday = part,
            'm' => self.match_time.tm_mon = part-1,
            'Y' => self.match_time.tm_year = part-1900,
            _ => panic!("unsupported time format char {:?}", fmt),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathExpression {
    leading_sep: bool,
    trailing_sep: bool,
    elements: Vec<PathElement>,
    opts: Option<PathMatchOptions>
}

fn is_maybe_constent_element(opts: &PathMatchOptions, fmt: char) -> bool {
    let format = format!("%{}", fmt);
    let left = time::strftime(&format, &opts.from.unwrap());
    let right = time::strftime(&format, &opts.to.unwrap());
    left == right
}

fn constant_time_elements(opts: &PathMatchOptions) -> HashSet<char> {
    let mut set = HashSet::new();
    for f in vec!('Y', 'm', 'd', 'H', 'M', 'S') {
        if is_maybe_constent_element(opts, f) {
            set.insert(f);
        } else {
            break;
        }
    }
    set
}

impl PathExpression {
    pub fn parse(exp: &str) -> Result<PathExpression, &'static str> {
        let mut exp = exp;
        let lead = if exp.starts_with("/") {
            exp = &exp[1..];
            true
        } else {
            false
        };
        let trail = if exp.ends_with("/") {
            exp = &exp[..exp.len()-1];
            true
        } else {
            false
        };
        named!(placeholder_timepart( &[u8] ) -> PathElementPart,
            do_parse!(
                tag!("%") >>
                a: is_not!("}") >>
                (
                    PathElementPart::TimePart {
                        fmt: from_utf8(a).unwrap().to_string(),
                        last_in_expression: false,
                        value: None,
                    }
                )
            )
        );
        named!(placeholder_name( &[u8] ) -> PathElementPart,
            do_parse!(
                a: is_not!("}") >>
                ( PathElementPart::Placeholder { name: from_utf8(a).unwrap().to_string(), value: None } )
            )
        );
        named!(placeholder( &[u8] ) -> PathElementPart,
            delimited!(char!('{'), alt!(placeholder_timepart | placeholder_name), char!('}'))
        );
        named!(element_part( &[u8] ) -> PathElementPart,
            do_parse!(
                a: is_not!("/{") >>
                ( PathElementPart::Literal ( from_utf8(a).unwrap().to_string() ) )
            )
        );
        named!(element( &[u8] ) -> PathElement,
            do_parse!(
                a: many1!( alt!( element_part | placeholder )) >>
                ( PathElement::new(a) )
            )
        );
        named!(separator <&[u8]>, is_a!("/"));
        named!(elements( &[u8] ) -> Vec<PathElement>, separated_list!(separator, element));
        named!(path( &[u8] ) -> PathExpression,
            do_parse!(
                elems: opt!(elements) >>
                (
                    PathExpression {
                        leading_sep: false,
                        trailing_sep: false,  // TODO: work out how to avoid fixing this up later
                        elements: elems.unwrap_or_else(|| Vec::new()),
                        opts: None,
                    }
                )
            )
        );

        let r = path(exp.as_bytes());

        match r {
            IResult::Done(_, mut output) => {
                output.leading_sep = lead;
                output.trailing_sep = trail;
                let count = output.count_time_parts();
                let mut index = 0;
                for e in output.elements.iter_mut() {
                    for p in e.parts.iter_mut() {
                        if let &mut PathElementPart::TimePart{ref mut last_in_expression, ..} = p {
                            if index == count -1 {
                                *last_in_expression = true;
                            }
                            index += 1;
                        }
                    }
                }
                Ok(output)
            },
            IResult::Incomplete(_) => Err("premature end of input"),
            IResult::Error(_) => Err("error parsing path expression"),
        }
    }

    fn count_time_parts(&self) -> usize {
        self.elements.iter().map(|e| {
            e.parts.iter().fold(0, |acc, p| {
                if let &PathElementPart::TimePart{..} = p {
                    acc + 1
                } else {
                    acc
                }
            })
        }).sum()
    }

    pub fn with(&self, opts: PathMatchOptions) -> PathExpression {
        PathExpression {
            leading_sep: self.leading_sep,
            trailing_sep: self.trailing_sep,
            elements: self.specialise_elements(&opts),
            opts: Some(opts),
        }
    }

    fn specialise_elements(&self, opts: &PathMatchOptions) -> Vec<PathElement> {
        if opts.from.is_none() || opts.to.is_none() {
            return self.elements.clone();
        }
        let const_elements = constant_time_elements(&opts);
        let mut newelements = Vec::new();
        for elem in self.elements.iter() {
            let mut newparts = Vec::new();
            for part in elem.parts.iter() {
                let newpart = match part {
                    &PathElementPart::Literal(ref s) => PathElementPart::Literal(s.clone()),
                    &PathElementPart::TimePart{ref fmt, ref value, ref last_in_expression} => {
                        let c = fmt.chars().next().unwrap(/*TODO*/);
                        if const_elements.contains(&c) {
                            PathElementPart::TimePart{ fmt: fmt.clone(), value: Some(time::strftime(&format!("%{}", c), &opts.from.unwrap()).unwrap()), last_in_expression: *last_in_expression }
                        } else {
                            PathElementPart::TimePart{ fmt: fmt.clone(), value: value.clone(), last_in_expression: *last_in_expression }
                        }
                    },
                    &PathElementPart::Placeholder{ ref name, ref value } => {
                        PathElementPart::Placeholder{ name: name.clone(), value: value.clone() }
                    },
                };
                newparts.push(newpart);
            }
            newelements.push(PathElement::new(newparts));
        }
        newelements
    }

    pub fn common_prefix(&self) -> String {
        let mut prefix = String::default();
        if self.leading_sep {
            prefix.push_str("/");
        }
        let mut i = self.elements.iter().peekable();
        while let Some(t) = i.next() {
            if !t.common_prefix(&mut prefix) {
                break;
            }
            if i.peek().is_some() {
                prefix.push_str("/");
            }
        }
        if self.trailing_sep {
            prefix.push_str("/");
        }
        prefix
    }

    // TODO: return Result and bail-out early for up front problems,
    pub fn list_local(&self, opts: PathMatchOptions) -> ListLocal {
        let specialised = self.with(opts.clone());
        let ctx = MatchContext::new(&opts);
        let scope = if self.leading_sep {
            PathBuf::from("/")
        } else {
            env::current_dir().unwrap()
        };
        ListLocal { pathexp: specialised, todo: Vec::new(), scope: Some(scope), opts: opts, ctx: ctx }
    }

    // TODO: return Result and bail-out early for up front problems,
    pub fn list_s3<P, D>(&self, client: S3ClientWorkarounds<P, D>, bucket: &str, opts: PathMatchOptions) -> ListS3<P,D>
        where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
    {
        let specialised = self.with(opts.clone());
        ListS3::new(client, bucket, specialised)
    }

    // TODO: return Result and bail-out early for up front problems,
    pub fn specialise_first_element<P, D>(&self, client: S3ClientWorkarounds<P, D>, bucket: &str, opts: PathMatchOptions) -> SpecialiseS3<P,D>
        where P: ProvideAwsCredentials, D: DispatchSignedRequestWorkaround
    {
        let specialised = self.with(opts.clone());
        let ctx = MatchContext::new(&opts);
        SpecialiseS3::new(client, bucket, specialised, ctx)
    }

    pub fn is_match(&self, name: &str) -> bool {
        let opts = if let Some(ref o) = self.opts {
            o.clone()
        } else {
            PathMatchOptions::new()
        };
        let mut ctx = MatchContext::new(&opts);
        self.do_match(name, 0, &mut ctx)
    }

    fn do_match(&self, name: &str, idx: usize, ctx: &mut MatchContext) -> bool {
        if self.leading_sep && !name.starts_with("/") {
            return false;
        }
        let mut i = name.splitn(2, '/');
        let head = i.next().unwrap();
        if self.elements.len()==0 && head == "" {
            return true;
        }
        if self.elements[idx].matches(ctx, head) {
            if idx == self.elements.len()-1 {
                match i.next() {
                    Some("") => true,
                    None => true,
                    Some(_) => false
                }
            } else {
                if let Some(mut tail) = i.next() {
                    if "" == tail {
                        // we ran out of path elements but there are still unpatched components of the
                        // pattern
                        false
                    } else {
                        // compress multiple '/' character down to one (that we already handled
                        // with splitn() higher up)
                        while tail.starts_with("/") {
                            tail = &tail[1..]
                        }
                        self.do_match(tail, idx + 1, ctx)
                    }
                } else {
                    // we ran out of path elements but there are still unpatched components of the
                    // pattern
                    false
                }
            }
        } else {
            false
        }
    }
}



#[derive(Debug,Clone)]
pub struct PathMatchOptions {
    from: Option<time::Tm>,
    to: Option<time::Tm>,
}

impl PathMatchOptions {
    pub fn new() -> PathMatchOptions {
        PathMatchOptions {
            from: None,
            to: None,
        }
    }

    pub fn from(&mut self, from: time::Tm) -> &mut PathMatchOptions {
        if let Some(to) = self.to {
            assert!(from < to);
        }
        self.from = Some(from);
        self
    }

    pub fn to(&mut self, to: time::Tm) -> &mut PathMatchOptions {
        if let Some(from) = self.from {
            assert!(from < to);
        }
        self.to = Some(to);
        self
    }
}


#[cfg(test)]
mod tests {
    extern crate hyper;

    use super::*;
    use std::path::Path;
    use time::strptime;
    use regex::Regex;
    use std::str::FromStr;
    use std::time::Duration;

    use rusoto::{DefaultCredentialsProvider, Region};
    use rusoto_workarounds::s3::S3ClientWorkarounds;
    use self::hyper::client::Client;

    #[test]
    fn contant() {
        let e = PathExpression::parse("a").unwrap();
        assert_eq!("a", e.common_prefix());
        assert!(e.is_match("a"));
        assert!(!e.is_match("b"));
        assert!(!e.is_match(""));
        assert!(!e.is_match("aa"));
    }

    #[test]
    fn separator() {
        let e = PathExpression::parse("/").unwrap();
        assert_eq!("/", e.common_prefix());
        assert!(e.is_match("/"));

        let e = PathExpression::parse("a/").unwrap();
        assert_eq!("a/", e.common_prefix());
        assert!(e.is_match("a/"));
        assert!(!e.is_match("a/b"));

        let e = PathExpression::parse("a/b").unwrap();
        assert_eq!("a/b", e.common_prefix());
        assert!(e.is_match("a/b"));
        assert!(e.is_match("a///b"));
        assert!(!e.is_match("a"));

        let e = PathExpression::parse("a///b").unwrap();
        assert_eq!("a/b", e.common_prefix());
        assert!(e.is_match("a/b"));
    }

    #[test]
    fn placeholder() {
        let e = PathExpression::parse("a/{b}/c").unwrap();
        assert_eq!("a/", e.common_prefix());
        assert!(e.is_match("a/bbb/c"));
    }

    #[test]
    fn time_common_hour() {
        let e = PathExpression::parse("a/{%H}:{%M}/c").unwrap();
        let mut options = PathMatchOptions::new();
        options
            .from(strptime("2017-02-03 11:20:34", "%Y-%m-%d %H:%M:%S").unwrap())
            .to(strptime("2017-02-03 11:44:34", "%Y-%m-%d %H:%M:%S").unwrap());
        let expr = e.with(options);
        assert_eq!("a/11:", expr.common_prefix());
        assert!(e.is_match("a/11:30/c"));
    }

    #[test]
    fn time_different_minute() {
        // NB 'seconds' comes before 'minutes' (which means the time doesn't contribute to the
        // common prefix)
        let e = PathExpression::parse("a/{%S}:{%M}/c").unwrap();
        let mut options = PathMatchOptions::new();
        options
            .from(strptime("2017-02-03 11:20:34", "%Y-%m-%d %H:%M:%S").unwrap())
            .to(strptime("2017-02-03 11:44:34", "%Y-%m-%d %H:%M:%S").unwrap());
        let expr = e.with(options);
        assert_eq!("a/", expr.common_prefix());
        assert!(e.is_match("a/30:11/c"));
    }
}
