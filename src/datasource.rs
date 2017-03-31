use toml;
use std::io;
use std::fs::File;
use std::io::Read;

#[derive(Deserialize,Debug)]
pub struct Datasources {
    pub s3: Vec<S3Source>,
    pub file: Vec<FileSource>,
}

#[derive(Deserialize,Debug)]
pub struct S3Source {
    pub name: String,
    pub region: String,
    pub bucket: String,
    pub pathexp: String,
}

#[derive(Deserialize,Debug)]
pub struct FileSource {
    pub name: String,
    pub pathexp: String,
}

pub fn get_datasources() -> Result<Datasources, io::Error> {
    let mut f = File::open("datasources.toml")?;
    let mut text = String::new();
    f.read_to_string(&mut text);
    let sources: Datasources = toml::from_str(&text).unwrap();
    Ok(sources)
}
