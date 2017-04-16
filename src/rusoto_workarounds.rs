
pub mod request {

use hyper::Method;
use hyper::client::{Client, FutureResponse, Request};
use hyper::header::UserAgent;
use hyper_tls::HttpsConnector;
use log::LogLevel;
use rusoto::SignedRequest;
use std::env;
use std::io::Read;

// code lifted from Rusoto and then hacked in order to,
//  - work around not being able to support binary S3 response bodies (since fixed in main branch?)
//  - needing to buffer the entire response body (this code allows the app to stream the response
//    body)

lazy_static! {
    static ref DEFAULT_USER_AGENT: Vec<Vec<u8>> = vec![format!("whatf/{} {}",
            env!("CARGO_PKG_VERSION"), env::consts::OS).as_bytes().to_vec()];
}

pub trait DispatchSignedRequestWorkaround {
    fn dispatch(&self, request: &SignedRequest) -> Result<FutureResponse, String>;
}

impl DispatchSignedRequestWorkaround for Client<HttpsConnector> {
    fn dispatch(&self, request: &SignedRequest) -> Result<FutureResponse, String> {
        let hyper_method = match request.method().as_ref() {
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "GET" => Method::Get,
            "HEAD" => Method::Head,
            v => return Err(format!("Unsupported HTTP verb {}", v))
        };

        let mut final_uri = format!("https://{}{}", request.hostname(), request.canonical_path());
        if !request.canonical_query_string().is_empty() {
            final_uri = final_uri + &format!("?{}", request.canonical_query_string());
        }
        let mut req = Request::new(hyper_method, final_uri.parse().unwrap());

        {
            let mut hyper_headers = req.headers_mut();
            for h in request.headers().iter() {
                hyper_headers.set_raw(h.0.to_owned(), h.1.to_owned());
            }

            // Add a default user-agent header if one is not already present.
            if !hyper_headers.has::<UserAgent>() {
                hyper_headers.set_raw("user-agent".to_owned(), DEFAULT_USER_AGENT.clone());
            }
        }

        if log_enabled!(LogLevel::Debug) {
            let payload = request.payload().map(|mut payload_bytes| {
                let mut payload_string = String::new();

                payload_bytes.read_to_string(&mut payload_string)
                    .map(|_| payload_string)
                    .unwrap_or_else(|_| String::from("<non-UTF-8 data>"))
            });

            debug!("Full request: \n method: {}\n final_uri: {}\n payload: {}\nHeaders:\n", req.method(), final_uri, payload.unwrap_or("".to_owned()));
            for h in req.headers().iter() {
                debug!("{}:{}", h.name(), h.value_string());
            }
        }

        if let Some(payload_contents) = request.payload() {
            req.set_body(payload_contents.to_vec());
        }
        Ok(self.request(req))
    }
}


}  // mod request


pub mod s3 {

use futures::stream::Stream;
use futures::future;
use futures::{Async, Future, Poll};
use hyper::client::{Client, FutureResponse, Response};
use hyper::status::StatusCode;
use hyper_tls::HttpsConnector;
use rusoto::{CredentialsError, HttpDispatchError, ProvideAwsCredentials, Region, SignedRequest};
use rusoto::s3::{CommonPrefix, CommonPrefixList, GetObjectRequest, ListObjectsOutput, ListObjectsRequest, Object, ObjectList, Owner};
use rusoto_workarounds::request::DispatchSignedRequestWorkaround;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::iter::Peekable;
use std::num::ParseIntError;
use std::str;
use std;
use tokio_core::reactor::{Core, Handle};
use xml::reader::{EventReader, Events, XmlEvent};
use xml;

/// Workarounds for problems in rusoto::s3::S3Client
pub struct S3ClientWorkarounds<P, D> where P: ProvideAwsCredentials,
           D: DispatchSignedRequestWorkaround {
    credentials_provider: P,
    region: Region,
    dispatcher: D,
}

pub type Params = BTreeMap<String, Option<String>>;
pub trait ServiceParams {
    fn put(&mut self, key: &str, val: &str);
    fn put_key(&mut self, key: &str);
}
impl ServiceParams for Params {
    fn put(&mut self, key: &str, val: &str) {
        self.insert(key.to_owned(), Some(val.to_owned()));
    }

    fn put_key(&mut self, key: &str) {
		self.insert(key.to_owned(), None);
	}
}

fn wait_for_body(core: &mut Core, resp: Response) -> Result<Vec<u8>, ()> {
    core.run(resp.body().map_err(|_| ()).fold(vec![], |mut acc, chunk| {
        acc.extend_from_slice(&chunk);
        Ok(acc)
    }))
}



fn http_client(handle: &Handle) -> Result<Client<HttpsConnector>, std::io::Error> {
    Ok(Client::configure()
        .connector(HttpsConnector::new(4, handle))
        .build(handle))
}

impl <P, D> S3ClientWorkarounds<P, D> where P: ProvideAwsCredentials,
    D: DispatchSignedRequestWorkaround
{
    pub fn new(request_dispatcher: D, credentials_provider: P,
               region: Region) -> Self {
        S3ClientWorkarounds{credentials_provider: credentials_provider,
                 region: region,
                 dispatcher: request_dispatcher,}
    }


    pub fn get_object(&self, input: &GetObjectRequest)
     -> Result<FutureGetObjectOutput, GetObjectError> {

        let mut params = Params::new();

        let mut request_uri = "/{Bucket}/{Key}".to_string();


        request_uri =
            request_uri.replace("{Bucket}", &input.bucket.to_string());
        request_uri = request_uri.replace("{Key}", &input.key.to_string());

        let mut request =
            SignedRequest::new("GET", "s3", self.region, &request_uri);


        if let Some(ref if_match) = input.if_match {
            request.add_header("If-Match", &if_match.to_string());
        }

        if let Some(ref if_modified_since) = input.if_modified_since {
            request.add_header("If-Modified-Since",
                               &if_modified_since.to_string());
        }

        if let Some(ref if_none_match) = input.if_none_match {
            request.add_header("If-None-Match", &if_none_match.to_string());
        }

        if let Some(ref if_unmodified_since) = input.if_unmodified_since {
            request.add_header("If-Unmodified-Since",
                               &if_unmodified_since.to_string());
        }

        if let Some(ref range) = input.range {
            request.add_header("Range", &range.to_string());
        }

        if let Some(ref request_payer) = input.request_payer {
            request.add_header("x-amz-request-payer",
                               &request_payer.to_string());
        }

        if let Some(ref sse_customer_algorithm) = input.sse_customer_algorithm
               {
            request.add_header("x-amz-server-side-encryption-customer-algorithm",
                               &sse_customer_algorithm.to_string());
        }

        if let Some(ref sse_customer_key) = input.sse_customer_key {
            request.add_header("x-amz-server-side-encryption-customer-key",
                               &sse_customer_key.to_string());
        }

        if let Some(ref sse_customer_key_md5) = input.sse_customer_key_md5 {
            request.add_header("x-amz-server-side-encryption-customer-key-MD5",
                               &sse_customer_key_md5.to_string());
        }

        if let Some(ref part_number) = input.part_number {
            params.put("partNumber", &part_number.to_string());
        }

        if let Some(ref response_cache_control) = input.response_cache_control
               {
            params.put("response-cache-control",
                       &response_cache_control.to_string());
        }

        if let Some(ref response_content_disposition) =
               input.response_content_disposition {
            params.put("response-content-disposition",
                       &response_content_disposition.to_string());
        }

        if let Some(ref response_content_encoding) =
               input.response_content_encoding {
            params.put("response-content-encoding",
                       &response_content_encoding.to_string());
        }

        if let Some(ref response_content_language) =
               input.response_content_language {
            params.put("response-content-language",
                       &response_content_language.to_string());
        }

        if let Some(ref response_content_type) = input.response_content_type {
            params.put("response-content-type",
                       &response_content_type.to_string());
        }

        if let Some(ref response_expires) = input.response_expires {
            params.put("response-expires", &response_expires.to_string());
        }

        if let Some(ref version_id) = input.version_id {
            params.put("versionId", &version_id.to_string());
        }

        request.set_params(params);

        request.sign(&self.credentials_provider.credentials()?);
        // TODO: hacked the error variant to be Unknown rather than HttpDispatch since rusoto::HttpDispatchError has private ctor
        let future = self.dispatcher.dispatch(&request).map_err(|err| GetObjectError::Unknown(err) )?;
        Ok(FutureGetObjectOutput::new(future))
    }

    pub fn list_objects(&self, input: &ListObjectsRequest)
     -> Result<ListObjectsOutput, ListObjectsError> {

        let mut params = Params::new();

        let mut request_uri = "/{Bucket}".to_string();


        request_uri =
            request_uri.replace("{Bucket}", &input.bucket.to_string());

        let mut request =
            SignedRequest::new("GET", "s3", self.region, &request_uri);


        if let Some(ref request_payer) = input.request_payer {
            request.add_header("x-amz-request-payer",
                               &request_payer.to_string());
        }

        if let Some(ref delimiter) = input.delimiter {
            params.put("delimiter", &delimiter.to_string());
        }

        if let Some(ref encoding_type) = input.encoding_type {
            params.put("encoding-type", &encoding_type.to_string());
        }

        if let Some(ref marker) = input.marker {
            params.put("marker", &marker.to_string());
        }

        if let Some(ref max_keys) = input.max_keys {
            params.put("max-keys", &max_keys.to_string());
        }

        if let Some(ref prefix) = input.prefix {
            params.put("prefix", &prefix.to_string());
        }

        request.set_params(params);

        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let http_client = http_client(&handle).unwrap();


        request.sign(&self.credentials_provider.credentials()?);
        let future = http_client.dispatch(&request).map_err(|err| ListObjectsError::Unknown(err) )?;
        let response = core.run(future).map_err(|err| ListObjectsError::Unknown(err.description().to_string()) )?;

        match response.status() {
            StatusCode::Ok | StatusCode::NoContent |
            StatusCode::PartialContent => {
                let body = wait_for_body(&mut core, response).map_err(|_| ListObjectsError::Unknown("failed to read response body".to_string()))?;
                let reader = EventReader::new(&body[..]);
                let mut stack = XmlResponse::new(reader.into_iter().peekable());
                let _start_document = stack.next();
                let actual_tag_name = peek_at_name(&mut stack).map_err(|err| ListObjectsError::Unknown(err.0) )?;
                let result =
                    ListObjectsOutputDeserializer :: deserialize (
                         &actual_tag_name , &mut stack)?;

                Ok(result)
            }
            _ => {
                let body = wait_for_body(&mut core, response).map_err(|_| ListObjectsError::Unknown("failed to read error response body".to_string()))?;
                // FIXME: use response charset!
                let body_string = str::from_utf8(&body[..]).map_err(|_| ListObjectsError::Unknown("failed to convert error response body to string".to_string() ))?;
                Err(ListObjectsError::from_body(body_string))
            },
        }
    }

}
fn deserialize<T: Peek + Next, R: str::FromStr>(tag_name: &str, stack: &mut T) -> Result<R, XmlParseError>
    where R: Debug
{
    start_element(tag_name, stack)?;
    let obj: R = characters(stack)?.parse().ok().unwrap();
    end_element(tag_name, stack)?;
    Ok(obj)
}

fn deserialize_object_list<T: Peek + Next>(tag_name: &str, stack: &mut T)
 -> Result<ObjectList, XmlParseError> {

    let mut obj = vec!();

    loop  {

        let consume_next_tag =
            match stack.peek() {
                Some(&Ok(XmlEvent::StartElement { ref name, .. })) =>
                name.local_name == tag_name,
                _ => false,
            };

        if consume_next_tag {
            obj.push(try!(deserialize_object (
                          tag_name , stack )));
        } else { break  }

    }
    Ok(obj)

}

fn deserialize_object<T: Peek + Next>(tag_name: &str, stack: &mut T)
 -> Result<Object, XmlParseError> {
    try!(start_element ( tag_name , stack ));

    let mut obj = Object::default();

    loop  {
        let next_event =
            match stack.peek() {
                Some(&Ok(XmlEvent::EndElement { .. })) =>
                DeserializerNext::Close,
                 // TODO verify that we received the expected tag?
                Some(&Ok(XmlEvent::StartElement { ref name, .. })) =>
                DeserializerNext::Element(name.local_name.to_owned()),
                _ => DeserializerNext::Skip,
            };

        match next_event {
            DeserializerNext::Element(name) => {
                match &name[..] {
                    "ETag" => {
                        obj.e_tag =
                            Some(try!(deserialize("ETag", stack)));
                    }
                    "Key" => {
                        obj.key =
                            Some(try!(deserialize("Key", stack)));
                    }
                    "LastModified" => {
                        obj.last_modified =
                            Some(try!(deserialize("LastModified", stack)));
                    }
                    "Owner" => {
                        obj.owner =
                            Some(try!(deserialize_owner("Owner", stack)));
                    }
                    "Size" => {
                        obj.size =
                            Some(try!(deserialize("Size", stack)));
                    }
                    "StorageClass" => {
                        obj.storage_class =
                            Some(try!(deserialize("StorageClass", stack)));
                    }
                    _ => skip_tree(stack),
                }
            }
            DeserializerNext::Close => break ,
            DeserializerNext::Skip => { stack.next(); }
        }
    }

    try!(end_element ( tag_name , stack ));

    Ok(obj)

}

pub struct FutureGetObjectOutput {
    response: Box<Future<Item=Response, Error=GetObjectError>>,
}

impl FutureGetObjectOutput {
    fn new(future_resp: FutureResponse) -> FutureGetObjectOutput {
        let response = future_resp.map_err(|e| GetObjectError::Unknown(e.description().to_string()) ).and_then(|resp| {
            match resp.status() {
                StatusCode::Ok | StatusCode::NoContent |
                StatusCode::PartialContent => {
                    future::Either::A(future::ok(resp))
                }
                _ => {
                    let future_body = resp.body()
                        .map_err(|_| GetObjectError::Unknown("failure reading response body".to_string()))
                        .map(|chunk| chunk.to_vec(/*TODO: avoid this copy*/) )
                        .concat();
                    future::Either::B(future_body.and_then(|body| {
                        // FIXME: use response charset!
                        let body_string = str::from_utf8(&body[..])
                            .map_err(|_| GetObjectError::Unknown("failed to convert error response body to string".to_string() ));
                        let err = if let Ok(text) = body_string {
                            GetObjectError::from_body(text)
                        } else {
                            GetObjectError::Unknown("failed to read response body".to_string())
                        };
                        future::err(err)
                    }))
                },
            }

        });

        FutureGetObjectOutput {
            response: Box::new(response),
        }
    }
}

impl Future for FutureGetObjectOutput {
    type Item = Response;
    type Error = GetObjectError;

    fn poll(&mut self) -> Poll<Response, GetObjectError> {
        self.response.poll()
    }
}

#[derive(PartialEq, Debug)]
pub enum GetObjectError {

    ///The specified key does not exist.
    NoSuchKey(String),

    /// An error occurred dispatching the HTTP request
    HttpDispatch(HttpDispatchError),

    /// An error was encountered with AWS credentials.
    Credentials(CredentialsError),

    /// A validation error occurred.  Details from AWS are provided.
    Validation(String),

    /// An unknown error occurred.  The raw HTTP response is provided.
    Unknown(String),
}
impl GetObjectError {
    pub fn from_body(body: &str) -> GetObjectError {
        let mut reader = EventReader::new(body.as_bytes());
        let mut stack = XmlResponse::new(reader.into_iter().peekable());
        let _start_document = stack.next();
        let _response_envelope = stack.next();
        match XmlErrorDeserializer::deserialize("Error", &mut stack) {
            Ok(parsed_error) => {
                match &parsed_error.code[..] {
                    "NoSuchKey" =>
                    GetObjectError::NoSuchKey(String::from(parsed_error.message)),
                    _ => GetObjectError::Unknown(String::from(body)),
                }
            }
            Err(_) => GetObjectError::Unknown(body.to_string()),
        }
    }
}
impl From<XmlParseError> for GetObjectError {
    fn from(err: XmlParseError) -> GetObjectError {
        let XmlParseError(message) = err;
        GetObjectError::Unknown(message.to_string())
    }
}
impl From<CredentialsError> for GetObjectError {
    fn from(err: CredentialsError) -> GetObjectError {
        GetObjectError::Credentials(err)
    }
}
impl From<HttpDispatchError> for GetObjectError {
    fn from(err: HttpDispatchError) -> GetObjectError {
        GetObjectError::HttpDispatch(err)
    }
}
impl fmt::Display for GetObjectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f , "{}" , self . description (  ))
    }
}
impl Error for GetObjectError {
    fn description(&self) -> &str {
        match *self {
            GetObjectError::NoSuchKey(ref cause) => cause,
            GetObjectError::Validation(ref cause) => cause,
            GetObjectError::Credentials(ref err) => err.description(),
            GetObjectError::HttpDispatch(ref dispatch_error) =>
            dispatch_error.description(),
            GetObjectError::Unknown(ref cause) => cause,
        }
    }
}

fn deserialize_owner<T: Peek + Next>(tag_name: &str, stack: &mut T)
 -> Result<Owner, XmlParseError> {
    try!(start_element ( tag_name , stack ));

    let mut obj = Owner::default();

    loop  {
        let next_event =
            match stack.peek() {
                Some(&Ok(XmlEvent::EndElement { .. })) =>
                DeserializerNext::Close,
                 // TODO verify that we received the expected tag?
                Some(&Ok(XmlEvent::StartElement { ref name, .. })) =>
                DeserializerNext::Element(name.local_name.to_owned()),
                _ => DeserializerNext::Skip,
            };

        match next_event {
            DeserializerNext::Element(name) => {
                match &name[..] {
                    "DisplayName" => {
                        obj.display_name =
                            Some(deserialize("DisplayName", stack)?);
                    }
                    "ID" => {
                        obj.id =
                            Some(deserialize("ID", stack)?);
                    }
                    _ => skip_tree(stack),
                }
            }
            DeserializerNext::Close => break ,
            DeserializerNext::Skip => { stack.next(); }
        }
    }

    try!(end_element ( tag_name , stack ));

    Ok(obj)

}

fn deserialize_common_prefix<T: Peek + Next>(tag_name: &str, stack: &mut T)
 -> Result<CommonPrefix, XmlParseError> {
    try!(start_element ( tag_name , stack ));

    let mut obj = CommonPrefix::default();

    loop  {
        let next_event =
            match stack.peek() {
                Some(&Ok(XmlEvent::EndElement { .. })) =>
                DeserializerNext::Close,
                 // TODO verify that we received the expected tag?
                Some(&Ok(XmlEvent::StartElement { ref name, .. })) =>
                DeserializerNext::Element(name.local_name.to_owned()),
                _ => DeserializerNext::Skip,
            };

        match next_event {
            DeserializerNext::Element(name) => {
                match &name[..] {
                    "Prefix" => {
                        obj.prefix =
                            Some(deserialize("Prefix", stack)?);
                    }
                    _ => skip_tree(stack),
                }
            }
            DeserializerNext::Close => break ,
            DeserializerNext::Skip => { stack.next(); }
        }
    }

    try!(end_element ( tag_name , stack ));

    Ok(obj)

}

fn deserialize_common_prefix_list<T: Peek + Next>(tag_name: &str, stack: &mut T)
 -> Result<CommonPrefixList, XmlParseError> {

    let mut obj = vec!();

    loop  {

        let consume_next_tag =
            match stack.peek() {
                Some(&Ok(XmlEvent::StartElement { ref name, .. })) =>
                name.local_name == tag_name,
                _ => false,
            };

        if consume_next_tag {
            obj.push(deserialize_common_prefix(tag_name, stack)?);
        } else { break  }

    }
    Ok(obj)

}

enum DeserializerNext { Close, Skip, Element(String), }


struct ListObjectsOutputDeserializer;
impl ListObjectsOutputDeserializer {
    #[allow(unused_variables)]
    fn deserialize<T: Peek + Next>(tag_name: &str, stack: &mut T)
     -> Result<ListObjectsOutput, XmlParseError> {
        try!(start_element ( tag_name , stack ));

        let mut obj = ListObjectsOutput::default();

        loop  {
            let next_event =
                match stack.peek() {
                    Some(&Ok(XmlEvent::EndElement { .. })) =>
                    DeserializerNext::Close,
                     // TODO verify that we received the expected tag?
                    Some(&Ok(XmlEvent::StartElement { ref name, .. })) =>
                    DeserializerNext::Element(name.local_name.to_owned()),
                    _ => DeserializerNext::Skip,
                };

            match next_event {
                DeserializerNext::Element(name) => {
                    match &name[..] {
                        "CommonPrefixes" => {
                            obj.common_prefixes =
                                Some(deserialize_common_prefix_list("CommonPrefixes", stack)?);
                        }
                        "Contents" => {
                            obj.contents =
                                Some(try!(deserialize_object_list("Contents", stack)));
                        }
                        "Delimiter" => {
                            obj.delimiter =
                                Some(try!(deserialize
                                          ( "Delimiter" , stack )));
                        }
                        "EncodingType" => {
                            obj.encoding_type =
                                Some(try!(deserialize("EncodingType", stack)));
                        }
                        "IsTruncated" => {
                            obj.is_truncated =
                                Some(deserialize::<T,bool>("IsTruncated", stack)?);
                        }
                        "Marker" => {
                            obj.marker =
                                Some(try!(deserialize (
                                          "Marker" , stack )));
                        }
                        "MaxKeys" => {
                            obj.max_keys =
                                Some(deserialize::<T,i32>("MaxKeys", stack)?);
                        }
                        "Name" => {
                            obj.name =
                                Some(try!(
                                          deserialize ( "Name" , stack )));
                        }
                        "NextMarker" => {
                            obj.next_marker =
                                Some(try!(
                                          deserialize ( "NextMarker" , stack
                                          )));
                        }
                        "Prefix" => {
                            obj.prefix =
                                Some(try!(deserialize (
                                          "Prefix" , stack )));
                        }
                        _ => skip_tree(stack),
                    }
                }
                DeserializerNext::Close => break ,
                DeserializerNext::Skip => { stack.next(); }
            }
        }

        try!(end_element ( tag_name , stack ));

        Ok(obj)

    }
}


/// Errors returned by ListObjects
#[derive(PartialEq, Debug)]
pub enum ListObjectsError {

    ///The specified bucket does not exist.
    NoSuchBucket(String),

    /// An error occurred dispatching the HTTP request
    HttpDispatch(HttpDispatchError),

    /// An error was encountered with AWS credentials.
    Credentials(CredentialsError),

    /// A validation error occurred.  Details from AWS are provided.
    Validation(String),

    /// An unknown error occurred.  The raw HTTP response is provided.
    Unknown(String),
}

impl ListObjectsError {
    pub fn from_body(body: &str) -> ListObjectsError {
        let reader = EventReader::new(body.as_bytes());
        let mut stack = XmlResponse::new(reader.into_iter().peekable());
        let _start_document = stack.next();
        let _response_envelope = stack.next();
        match XmlErrorDeserializer::deserialize("Error", &mut stack) {
            Ok(parsed_error) => {
                match &parsed_error.code[..] {
                    "NoSuchBucket" =>
                    ListObjectsError::NoSuchBucket(String::from(parsed_error.message)),
                    _ => ListObjectsError::Unknown(String::from(body)),
                }
            }
            Err(_) => ListObjectsError::Unknown(body.to_string()),
        }
    }
}

impl From<XmlParseError> for ListObjectsError {
    fn from(err: XmlParseError) -> ListObjectsError {
        let XmlParseError(message) = err;
        ListObjectsError::Unknown(message.to_string())
    }
}
impl From<CredentialsError> for ListObjectsError {
    fn from(err: CredentialsError) -> ListObjectsError {
        ListObjectsError::Credentials(err)
    }
}
impl From<ParseIntError> for XmlParseError {
    fn from(_e: ParseIntError) -> XmlParseError {
        XmlParseError::new("ParseIntError")
    }
}
impl fmt::Display for ListObjectsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f , "{}" , self . description (  ))
    }
}
impl Error for ListObjectsError {
    fn description(&self) -> &str {
        match *self {
            ListObjectsError::NoSuchBucket(ref cause) => cause,
            ListObjectsError::Validation(ref cause) => cause,
            ListObjectsError::Credentials(ref err) => err.description(),
            ListObjectsError::HttpDispatch(ref dispatch_error) =>
            dispatch_error.description(),
            ListObjectsError::Unknown(ref cause) => cause,
        }
    }
}

// ---- lifted from rusoto's xmlutils ----

#[derive(Debug)]
pub struct XmlParseError(pub String);

impl XmlParseError {
    pub fn new(msg: &str) -> XmlParseError {
        XmlParseError(msg.to_string())
    }
}

/// Peek at next items in the XML stack
pub trait Peek {
    fn peek(&mut self) -> Option<&Result<XmlEvent, xml::reader::Error>>;
}

/// Move to the next part of the XML stack
pub trait Next {
    fn next(&mut self) -> Option<Result<XmlEvent, xml::reader::Error>>;
}


pub struct XmlResponse<'b> {
    xml_stack: Peekable<Events<&'b [u8]>>, // refactor to use XmlStack type?
}

impl<'b> XmlResponse<'b> {
    pub fn new(stack: Peekable<Events<&'b [u8]>>) -> XmlResponse {
        XmlResponse { xml_stack: stack }
    }
}

impl<'b> Peek for XmlResponse<'b> {
    fn peek(&mut self) -> Option<&Result<XmlEvent, xml::reader::Error>> {
        while let Some(&Ok(XmlEvent::Whitespace(_))) = self.xml_stack.peek() {
            self.xml_stack.next();
        }
        self.xml_stack.peek()
    }
}

impl<'b> Next for XmlResponse<'b> {
    fn next(&mut self) -> Option<Result<XmlEvent, xml::reader::Error>> {
        let mut maybe_event;
        loop {
            maybe_event = self.xml_stack.next();
            match maybe_event {
                Some(Ok(XmlEvent::Whitespace(_))) => {}
                _ => break,
            }
        }
        maybe_event
    }
}

/// get the name of the current element in the stack.  throw a parse error if it's not a `StartElement`
pub fn peek_at_name<T: Peek + Next>(stack: &mut T) -> Result<String, XmlParseError> {
    let current = stack.peek();
    if let Some(&Ok(XmlEvent::StartElement { ref name, .. })) = current {
        Ok(name.local_name.to_string())
    } else {
        Ok("".to_string())
    }
}

/// return a string field with the right name or throw a parse error
pub fn string_field<T: Peek + Next>(name: &str, stack: &mut T) -> Result<String, XmlParseError> {
    try!(start_element(name, stack));
    let value = try!(characters(stack));
    try!(end_element(name, stack));
    Ok(value)
}

/// return some XML Characters
pub fn characters<T: Peek + Next>(stack: &mut T) -> Result<String, XmlParseError> {
    {
        // Lexical lifetime
        // Check to see if the next element is an end tag.
        // If it is, return an empty string.
        let current = stack.peek();
        if let Some(&Ok(XmlEvent::EndElement { .. })) = current {
            return Ok("".to_string());
        }
    }
    if let Some(Ok(XmlEvent::Characters(data))) = stack.next() {
        Ok(data.to_string())
    } else {
        Err(XmlParseError::new("Expected characters"))
    }
}

/// consume a `StartElement` with a specific name or throw an `XmlParseError`
pub fn start_element<T: Peek + Next>(element_name: &str,
                                     stack: &mut T)
                                     -> Result<HashMap<String, String>, XmlParseError> {
    let next = stack.next();

    if let Some(Ok(XmlEvent::StartElement { name, attributes, .. })) = next {
        if name.local_name == element_name {
            let mut attr_map = HashMap::new();
            for attr in attributes {
                attr_map.insert(attr.name.local_name, attr.value);
            }
            Ok(attr_map)
        } else {
            Err(XmlParseError::new(&format!("START Expected {} got {}",
                                            element_name,
                                            name.local_name)))
        }
    } else {
        Err(XmlParseError::new(&format!("Expected StartElement {} got {:#?}", element_name, next)))
    }
}

/// consume an `EndElement` with a specific name or throw an `XmlParseError`
pub fn end_element<T: Peek + Next>(element_name: &str, stack: &mut T) -> Result<(), XmlParseError> {
    let next = stack.next();
    if let Some(Ok(XmlEvent::EndElement { name, .. })) = next {
        if name.local_name == element_name {
            Ok(())
        } else {
            Err(XmlParseError::new(&format!("END Expected {} got {}",
                                            element_name,
                                            name.local_name)))
        }
    } else {
        Err(XmlParseError::new(&format!("Expected EndElement {} got {:?}", element_name, next)))
    }
}

/// skip a tag and all its children
pub fn skip_tree<T: Peek + Next>(stack: &mut T) {

    let mut deep: usize = 0;

    loop {
        match stack.next() {
            None => break,
            Some(Ok(XmlEvent::StartElement { .. })) => deep += 1,
            Some(Ok(XmlEvent::EndElement { .. })) => {
                if deep > 1 {
                    deep -= 1;
                } else {
                    break;
                }
            }
            _ => (),
        }
    }

}

// --- xmlerror.rs

#[derive(Default, Debug)]
pub struct XmlError {
    pub error_type: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
}

pub struct XmlErrorDeserializer;
impl XmlErrorDeserializer {
    pub fn deserialize<T: Peek + Next>(tag_name: &str,
                                       stack: &mut T)
                                       -> Result<XmlError, XmlParseError> {
        try!(start_element(tag_name, stack));

        let mut obj = XmlError::default();

        loop {
            match &try!(peek_at_name(stack))[..] {
                "Type" => {
                    obj.error_type = try!(string_field("Type", stack));
                    continue;
                }
                "Code" => {
                    obj.code = try!(string_field("Code", stack));
                    continue;
                }
                "Message" => {
                    obj.message = try!(string_field("Message", stack));
                    continue;
                }
                "Detail" => {
                    try!(start_element("Detail", stack));
                    if let Ok(characters) = characters(stack) {
                        obj.detail = Some(characters.to_string());
                        try!(end_element("Detail", stack));
                    }
                    continue;
                }
                _ => break,
            }
        }

        try!(end_element(tag_name, stack));

        Ok(obj)
    }
}

}  // mod s3
