
mod request {

use std::env;
use hyper::Client;
use hyper::method::Method;
use hyper::header::{Headers, UserAgent};
use rusoto::SignedRequest;
use log::LogLevel::Debug;
use std::io::Read;
use hyper::client::Response;
use hyper::header::Connection;
use std::error::Error;
use std::sync::Arc;

// code lifted from Rusoto and then hacked in order to,
//  - work around not being able to support binary S3 response bodies (since fixed in main branch?)
//  - needing to buffer the entire response body (this code allows the app to stream the response
//    body)

lazy_static! {
    static ref DEFAULT_USER_AGENT: Vec<Vec<u8>> = vec![format!("whatf/{} {}",
            env!("CARGO_PKG_VERSION"), env::consts::OS).as_bytes().to_vec()];
}

pub trait DispatchSignedRequestWorkaround {
    fn dispatch(&self, request: &SignedRequest) -> Result<Response, String>;
}

impl DispatchSignedRequestWorkaround for Arc<Client> {
    fn dispatch(&self, request: &SignedRequest) -> Result<Response, String> {
        let hyper_method = match request.method().as_ref() {
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "GET" => Method::Get,
            "HEAD" => Method::Head,
            v => return Err(format!("Unsupported HTTP verb {}", v))
        };

        // translate the headers map to a format Hyper likes
        let mut hyper_headers = Headers::new();
        for h in request.headers().iter() {
            hyper_headers.set_raw(h.0.to_owned(), h.1.to_owned());
        }

        // Add a default user-agent header if one is not already present.
        if !hyper_headers.has::<UserAgent>() {
            hyper_headers.set_raw("user-agent".to_owned(), DEFAULT_USER_AGENT.clone());
        }

        let mut final_uri = format!("https://{}{}", request.hostname(), request.canonical_path());
        if !request.canonical_query_string().is_empty() {
            final_uri = final_uri + &format!("?{}", request.canonical_query_string());
        }

        if log_enabled!(Debug) {
            let payload = request.payload().map(|mut payload_bytes| {
                let mut payload_string = String::new();

                payload_bytes.read_to_string(&mut payload_string)
                    .map(|_| payload_string)
                    .unwrap_or_else(|_| String::from("<non-UTF-8 data>"))
            });

            debug!("Full request: \n method: {}\n final_uri: {}\n payload: {}\nHeaders:\n", hyper_method, final_uri, payload.unwrap_or("".to_owned()));
            for h in hyper_headers.iter() {
                debug!("{}:{}", h.name(), h.value_string());
            }
        }
        let req = self.request(hyper_method, &final_uri)
            .headers(hyper_headers)
            .header(Connection::keep_alive());
        let hyper_response = match request.payload() {
            None => req.body("").send().map_err(|err| err.description().to_string() )?,
            Some(payload_contents) => req.body(payload_contents).send().map_err(|err| err.description().to_string() )?,
        };

        Ok(hyper_response)
    }
}


}  // mod request


pub mod s3 {

use rusoto_workarounds::request::DispatchSignedRequestWorkaround;
use rusoto::ProvideAwsCredentials;
use rusoto::Region;
use rusoto::s3::GetObjectRequest;
use rusoto::s3::GetObjectError;
use rusoto::SignedRequest;
use hyper::status::StatusCode;
use hyper::client::Response;
use std::io::Read;
use std::collections::BTreeMap;

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
     -> Result<Response, GetObjectError> {

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
        let mut response = self.dispatcher.dispatch(&request).map_err(|err| GetObjectError::Unknown(err) )?;

        match response.status {
            StatusCode::Ok | StatusCode::NoContent |
            StatusCode::PartialContent => {
                Ok(response)
            }
            _ => {
                let mut body = String::new();
                if let Err(_) = response.read_to_string(&mut body) {
                    body.push_str("An S3 error occured, and the S3 response body could not be converted into a String");
                }
                Err(GetObjectError::from_body(&body))
            },
        }
    }
}



}  // mod s3
