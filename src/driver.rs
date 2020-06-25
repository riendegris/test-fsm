use async_zmq::{Message, MultipartIter, SendError, SinkExt};
use serde::Serialize;
use snafu::ResultExt;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
//use std::time::{Duration, SystemTime};
use url::Url;

use super::bano;
use super::cosmogony;
use super::error;
use super::ntfs;
use super::osm;

// From https://gist.github.com/anonymous/ee3e4df093c136ced7b394dc7ffb78e1

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum State {
    NotAvailable,
    DownloadingInProgress {
        started_at: SystemTime,
    },
    DownloadingError {
        details: String,
    },
    Downloaded {
        file_path: PathBuf,
        duration: Duration,
    },
    ProcessingInProgress {
        file_path: PathBuf,
        started_at: SystemTime,
    },
    ProcessingError {
        details: String,
    },
    Processed {
        file_path: PathBuf,
        duration: Duration,
    },
    IndexingInProgress {
        file_path: PathBuf,
        started_at: SystemTime,
    },
    IndexingError {
        details: String,
    },
    Indexed {
        duration: Duration,
    },
    ValidationInProgress,
    ValidationError {
        details: String,
    },
    Available,
    Failure(String),
}

#[derive(Debug, Clone, Serialize)]
enum Event {
    Download,
    DownloadingError(String),
    DownloadingComplete(PathBuf, Duration),
    Process(PathBuf),
    ProcessingError(String),
    ProcessingComplete(PathBuf, Duration),
    Index(PathBuf),
    IndexingError(String),
    IndexingComplete(Duration),
    Validate,
    ValidationError(String),
    ValidationComplete,
    Reset,
}

// pub struct Driver<I: Iterator<Item = T> + Unpin, T: Into<Message>> {
pub struct Driver<'a> {
    state: State,
    working_dir: PathBuf,
    mimirs_dir: PathBuf,
    cosmogony_dir: PathBuf,
    events: VecDeque<Event>,
    es: Url,
    index_type: String,
    data_source: String,
    region: String,
    // publish: async_zmq::publish::Publish<std::vec::IntoIter<&'a String>, &'a String>,
    publish: async_zmq::publish::Publish<std::vec::IntoIter<&'a str>, &'a str>,
}

// impl<I, T> Driver<I, T>
// where
//     I: Iterator<Item = T> + Unpin,
//     T: Into<Message>,
impl<'a> Driver<'a> {
    pub fn new<S: Into<String>>(
        index_type: S,
        data_source: S,
        region: S,
        port: u32,
    ) -> Result<Self, error::Error> {
        let zmq_endpoint = format!("tcp://127.0.0.1:{}", port);
        let mut zmq = async_zmq::publish(&zmq_endpoint)
            .context(error::ZMQSocketError {
                details: format!("Could not publish on endpoint '{}'", zmq_endpoint),
            })?
            .bind()
            .context(error::ZMQError {
                details: String::from("Could not bind socket for publication"),
            })?;
        Ok(Driver {
            state: State::NotAvailable,
            working_dir: PathBuf::from("./work"),
            mimirs_dir: PathBuf::from("/home/matt/lab/rust/kisio/mimirsbrunn"),
            cosmogony_dir: PathBuf::from("/home/matt/lab/rust/kisio/cosmogony"),
            events: VecDeque::new(),
            es: Url::parse("http://localhost:9200").unwrap(),
            index_type: index_type.into(),
            data_source: data_source.into(),
            region: region.into(),
            publish: zmq,
        })
    }
    async fn next(&mut self, event: Event) {
        match (&self.state, event) {
            (State::NotAvailable, Event::Download) => {
                self.state = State::DownloadingInProgress {
                    started_at: SystemTime::now(),
                };
            }
            (State::DownloadingInProgress { .. }, Event::DownloadingError(ref d)) => {
                self.state = State::DownloadingError {
                    details: String::from(d.as_str()),
                };
            }
            (State::DownloadingInProgress { .. }, Event::DownloadingComplete(ref p, ref d)) => {
                self.state = State::Downloaded {
                    file_path: p.clone(),
                    duration: d.clone(),
                }
            }
            (State::DownloadingError { .. }, Event::Reset) => {
                self.state = State::NotAvailable;
            }
            (State::Downloaded { .. }, Event::Process(ref p)) => {
                self.state = State::ProcessingInProgress {
                    file_path: p.clone(),
                    started_at: SystemTime::now(),
                };
            }
            (State::ProcessingInProgress { .. }, Event::ProcessingError(d)) => {
                self.state = State::ProcessingError { details: d }
            }
            (State::ProcessingError { .. }, Event::Reset) => {
                self.state = State::NotAvailable;
            }
            (State::ProcessingInProgress { .. }, Event::ProcessingComplete(ref p, ref d)) => {
                self.state = State::Processed {
                    file_path: p.clone(),
                    duration: d.clone(),
                };
            }
            (State::Processed { .. }, Event::Index(ref p)) => {
                self.state = State::IndexingInProgress {
                    file_path: p.clone(),
                    started_at: SystemTime::now(),
                };
            }
            (State::Downloaded { .. }, Event::Index(ref p)) => {
                self.state = State::IndexingInProgress {
                    file_path: p.clone(),
                    started_at: SystemTime::now(),
                };
            }
            (State::IndexingInProgress { .. }, Event::IndexingError(d)) => {
                self.state = State::IndexingError { details: d }
            }
            (State::IndexingError { .. }, Event::Reset) => {
                self.state = State::NotAvailable;
            }
            (State::IndexingInProgress { .. }, Event::IndexingComplete(ref d)) => {
                self.state = State::Indexed {
                    duration: d.clone(),
                };
            }
            (State::Indexed { .. }, Event::Validate) => {
                self.state = State::ValidationInProgress;
            }
            (State::ValidationInProgress, Event::ValidationError(d)) => {
                self.state = State::ValidationError { details: d }
            }
            (State::ValidationError { .. }, Event::Reset) => {
                self.state = State::NotAvailable;
            }
            (State::ValidationInProgress, Event::ValidationComplete) => {
                self.state = State::Available;
            }
            (s, e) => {
                self.state = State::Failure(
                    format!("Wrong state, event combination: {:#?} {:#?}", s, e).to_string(),
                )
            }
        }
    }

    pub async fn run(&mut self) {
        match &self.state {
            State::NotAvailable => {
                // println!("Not Available");
                // println!("Sending Download Event");
            }
            State::DownloadingInProgress { started_at } => {
                // println!(
                //     "Downloading {} / {} / {}",
                //     self.index_type, self.data_source, self.region
                // );
                match self.data_source.as_ref() {
                    "cosmogony" => {
                        match osm::download_osm_region(self.working_dir.clone(), &self.region) {
                            Ok(file_path) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events
                                    .push_back(Event::DownloadingComplete(file_path, duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::DownloadingError(format!(
                                    "Could not download: {}",
                                    err
                                )));
                            }
                        }
                    }
                    "bano" => {
                        match bano::download_bano_region(self.working_dir.clone(), &self.region) {
                            Ok(file_path) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events
                                    .push_back(Event::DownloadingComplete(file_path, duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::DownloadingError(format!(
                                    "Could not download: {}",
                                    err
                                )));
                            }
                        }
                    }
                    "osm" => match osm::download_osm_region(self.working_dir.clone(), &self.region)
                    {
                        Ok(file_path) => {
                            let duration = started_at.elapsed().unwrap();
                            self.events
                                .push_back(Event::DownloadingComplete(file_path, duration));
                        }
                        Err(err) => {
                            self.events.push_back(Event::DownloadingError(format!(
                                "Could not download: {}",
                                err
                            )));
                        }
                    },
                    "ntfs" => {
                        match ntfs::download_ntfs_region(self.working_dir.clone(), &self.region) {
                            Ok(file_path) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events
                                    .push_back(Event::DownloadingComplete(file_path, duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::DownloadingError(format!(
                                    "Could not download: {}",
                                    err
                                )));
                            }
                        }
                    }
                    _ => {
                        self.events.push_back(Event::DownloadingError(format!(
                            "Dont know how to download {}",
                            &self.data_source
                        )));
                    }
                }
            }
            State::DownloadingError { details } => {
                // println!("Downloading Error: {}", details);
            }
            State::Downloaded {
                file_path,
                duration,
            } => {
                // println!(
                //     "Downloaded {} in {}s",
                //     file_path.display(),
                //     duration.as_secs()
                // );
                // We're done downloading, now we need an extra processing step for cosmogony
                match self.data_source.as_ref() {
                    "cosmogony" => {
                        self.events.push_back(Event::Process(file_path.clone()));
                    }
                    _ => {
                        self.events.push_back(Event::Index(file_path.clone()));
                    }
                }
            }
            State::ProcessingInProgress {
                file_path,
                started_at,
            } => {
                // println!(
                //     "Processing {} / {} / {} using {}",
                //     self.index_type,
                //     self.data_source,
                //     self.region,
                //     file_path.display()
                // );
                match self.data_source.as_ref() {
                    "cosmogony" => {
                        match cosmogony::generate_cosmogony(
                            self.cosmogony_dir.clone(),
                            self.working_dir.clone(),
                            file_path.clone(),
                            &self.region,
                        ) {
                            Ok(path) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events
                                    .push_back(Event::ProcessingComplete(path, duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::ProcessingError(format!(
                                    "Could not process: {}",
                                    err
                                )));
                            }
                        }
                    }
                    _ => {
                        self.events.push_back(Event::ProcessingError(format!(
                            "Dont know how to process {}",
                            &self.data_source
                        )));
                    }
                }
            }
            State::ProcessingError { details } => {
                // println!("Processing Error: {}", details);
            }
            State::Processed {
                file_path,
                duration,
            } => {
                // println!(
                //     "Processed {} {} in {}s",
                //     self.data_source,
                //     self.region,
                //     duration.as_secs()
                // );
                self.events.push_back(Event::Index(file_path.clone()));
            }
            State::IndexingInProgress {
                file_path,
                started_at,
            } => {
                // println!(
                //     "Indexing {} / {} / {} using {}",
                //     self.index_type,
                //     self.data_source,
                //     self.region,
                //     file_path.display()
                // );
                match self.data_source.as_ref() {
                    "bano" => {
                        match bano::index_bano_region(
                            self.mimirs_dir.clone(),
                            self.es.clone(),
                            file_path.clone(),
                        ) {
                            Ok(()) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events.push_back(Event::IndexingComplete(duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::IndexingError(format!(
                                    "Could not index BANO: {}",
                                    err
                                )));
                            }
                        }
                    }
                    "osm" => {
                        // We need to analyze the index_type to see how we are going to import
                        // osm: do we need to import admins, streets, ...?
                        // FIXME: Here, for simplicity, we hard code index_poi = false
                        let index = match self.index_type.as_ref() {
                            "admins" => Some((true, false, false)),
                            "streets" => Some((false, true, false)),
                            _ => None,
                        };

                        if index.is_none() {
                            self.events.push_back(Event::IndexingError(format!(
                                "Could not index {} using OSM",
                                self.index_type
                            )));
                        } else {
                            let index = index.unwrap();
                            match osm::index_osm_region(
                                self.mimirs_dir.clone(),
                                self.es.clone(),
                                file_path.clone(),
                                index.0,
                                index.1,
                                index.2,
                                8, // 8 = default city level
                            ) {
                                Ok(()) => {
                                    let duration = started_at.elapsed().unwrap();
                                    self.events.push_back(Event::IndexingComplete(duration));
                                }
                                Err(err) => {
                                    self.events.push_back(Event::IndexingError(format!(
                                        "Could not index OSM: {}",
                                        err
                                    )));
                                }
                            }
                        }
                    }
                    "cosmogony" => {
                        match cosmogony::index_cosmogony_region(
                            self.mimirs_dir.clone(),
                            self.es.clone(),
                            file_path.clone(),
                        ) {
                            Ok(()) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events.push_back(Event::IndexingComplete(duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::IndexingError(format!(
                                    "Could not index cosmogony: {}",
                                    err
                                )));
                            }
                        }
                    }
                    "ntfs" => {
                        match ntfs::index_ntfs_region(
                            self.mimirs_dir.clone(),
                            self.es.clone(),
                            file_path.clone(),
                        ) {
                            Ok(()) => {
                                let duration = started_at.elapsed().unwrap();
                                self.events.push_back(Event::IndexingComplete(duration));
                            }
                            Err(err) => {
                                self.events.push_back(Event::IndexingError(format!(
                                    "Could not index NTFS: {}",
                                    err
                                )));
                            }
                        }
                    }
                    _ => {
                        self.events.push_back(Event::IndexingError(format!(
                            "Dont know how to index {}",
                            &self.data_source
                        )));
                    }
                }
            }
            State::IndexingError { details } => {
                // println!("Indexing Error: {}", details);
            }
            State::Indexed { duration } => {
                self.events.push_back(Event::Validate);
            }
            State::ValidationInProgress => {
                std::thread::sleep(std::time::Duration::from_secs(1));
                self.events.push_back(Event::ValidationComplete);
            }
            State::ValidationError { details } => {}
            State::Available => {}
            State::Failure(_) => {}
        }
    }
}

pub async fn drive<'a>(mut driver: Driver<'a>) {
    driver.events.push_back(Event::Download);
    while let Some(event) = driver.events.pop_front() {
        driver.next(event).await;
        let j = serde_json::to_string(&driver.state).unwrap();
        let msg = vec!["foo", j.as_str()];
        // let msg = vec!["foo"];
        let res: MultipartIter<_, _> = msg.into();
        driver.publish.send(res).await.unwrap();
        if let State::Failure(string) = &driver.state {
            println!("{}", string);
            break;
        } else {
            driver.run().await;
        }
    }
}
