use async_zmq::StreamExt;
use clap::{App, Arg};
use snafu::ResultExt;
// use std::collections::VecDeque;
// use std::path::PathBuf;
// use std::time::{Duration, Instant};
// use tokio::sync::mpsc;
// use url::Url;

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
    let mut driver = driver::Driver::new(index_type, data_source, region, 5555)?;
    let mut zmq = async_zmq::subscribe("tcp://127.0.0.1:5555")
        .context(error::ZMQSocketError {
            details: String::from("Could not subscribe on tcp://127.0.0.1:5555"),
        })?
        .connect()
        .context(error::ZMQError {
            details: String::from("Could not connect subscribe"),
        })?;
    zmq.set_subscribe("state")
        .context(error::ZMQSubscribeError {
            details: format!("Could not subscribe to '{}' topic", "state"),
        })?;
    tokio::spawn(async move {
        driver.drive().await;
    });
    while let Some(msg) = zmq.next().await {
        // Received message is a type of Result<MessageBuf>
        let msg = msg.context(error::ZMQRecvError {
            details: String::from("ZMQ Reception Error"),
        })?;

        msg.iter()
            .skip(1) // skip the topic
            .for_each(|m| println!("Received: {}", m.as_str().unwrap()));
    }
    Ok(())
}
