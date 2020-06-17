use clap::{App, Arg};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use url::Url;

mod bano;
mod cosmogony;
mod download;
mod driver;
mod error;
mod ntfs;
mod osm;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let matches = App::new("Create Elasticsearch Index")
        .version("0.1")
        .author("Matthieu Paindavoine")
        .arg(
            Arg::with_name("index_type")
                .short("i")
                .value_name("STRING")
                .help("input type (admins, streets, addresses)"),
        )
        .arg(
            Arg::with_name("data_source")
                .short("d")
                .value_name("STRING")
                .help("data source (osm, bano, openaddress)"),
        )
        .arg(
            Arg::with_name("region")
                .short("r")
                .value_name("STRING")
                .help("region"),
        )
        .get_matches();

    let index_type = matches
        .value_of("index_type")
        .ok_or(error::Error::MiscError {
            details: String::from("Missing Index Type"),
        })?;
    let data_source = matches
        .value_of("data_source")
        .ok_or(error::Error::MiscError {
            details: String::from("Missing Data Source"),
        })?;
    let region = matches.value_of("region").ok_or(error::Error::MiscError {
        details: String::from("Missing Region"),
    })?;
    let (tx, mut rx) = mpsc::channel(100);
    let mut driver = driver::Driver::new(index_type, data_source, region, tx);
    tokio::spawn(async move {
        driver.drive().await;
    });
    while let Some(state) = rx.recv().await {
        println!("{:?}", state)
    }

    Ok(())
}
