use clap::{App, Arg};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use url::Url;

mod bano;
mod cosmogony;
mod download;
mod error;
mod ntfs;
mod osm;

// From https://gist.github.com/anonymous/ee3e4df093c136ced7b394dc7ffb78e1

#[derive(Debug, PartialEq)]
enum State {
    NotAvailable,
    DownloadingInProgress {
        started_at: Instant,
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
        started_at: Instant,
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
        started_at: Instant,
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

#[derive(Debug, Clone)]
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

struct Driver {
    state: State,
    working_dir: PathBuf,
    mimirs_dir: PathBuf,
    cosmogony_dir: PathBuf,
    events: VecDeque<Event>,
    es: Url,
    index_type: String,
    data_source: String,
    region: String,
}

impl Driver {
    fn new<S: Into<String>>(index_type: S, data_source: S, region: S) -> Self {
        Driver {
            state: State::NotAvailable,
            working_dir: PathBuf::from("./work"),
            mimirs_dir: PathBuf::from("/home/matt/lab/rust/kisio/mimirsbrunn"),
            cosmogony_dir: PathBuf::from("/home/matt/lab/rust/kisio/cosmogony"),
            events: VecDeque::new(),
            es: Url::parse("http://localhost:9200").unwrap(),
            index_type: index_type.into(),
            data_source: data_source.into(),
            region: region.into(),
        }
    }
    fn next(&mut self, event: Event) {
        match (&self.state, event) {
            (State::NotAvailable, Event::Download) => {
                self.state = State::DownloadingInProgress {
                    started_at: Instant::now(),
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
                    started_at: Instant::now(),
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
                    started_at: Instant::now(),
                };
            }
            (State::Downloaded { .. }, Event::Index(ref p)) => {
                self.state = State::IndexingInProgress {
                    file_path: p.clone(),
                    started_at: Instant::now(),
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

    fn run(&mut self) {
        match &self.state {
            State::NotAvailable => {
                println!("Not Available");
                println!("Sending Download Event");
            }
            State::DownloadingInProgress { started_at } => {
                println!(
                    "Downloading {} / {} / {}",
                    self.index_type, self.data_source, self.region
                );
                match self.data_source.as_ref() {
                    "cosmogony" => {
                        match osm::download_osm_region(self.working_dir.clone(), &self.region) {
                            Ok(file_path) => {
                                let duration = started_at.elapsed();
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
                                let duration = started_at.elapsed();
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
                            let duration = started_at.elapsed();
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
                    _ => {
                        self.events.push_back(Event::DownloadingError(format!(
                            "Dont know how to download {}",
                            &self.data_source
                        )));
                    }
                }
            }
            State::DownloadingError { details } => {
                println!("Downloading Error: {}", details);
            }
            State::Downloaded {
                file_path,
                duration,
            } => {
                println!(
                    "Downloaded {} in {}s",
                    file_path.display(),
                    duration.as_secs()
                );
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
                println!(
                    "Processing {} / {} / {} using {}",
                    self.index_type,
                    self.data_source,
                    self.region,
                    file_path.display()
                );
                match self.data_source.as_ref() {
                    "cosmogony" => {
                        match cosmogony::generate_cosmogony(
                            self.cosmogony_dir.clone(),
                            self.working_dir.clone(),
                            file_path.clone(),
                            &self.region,
                        ) {
                            Ok(path) => {
                                let duration = started_at.elapsed();
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
                println!("Processing Error: {}", details);
            }
            State::Processed {
                file_path,
                duration,
            } => {
                println!(
                    "Processed {} {} in {}s",
                    self.data_source,
                    self.region,
                    duration.as_secs()
                );
                self.events.push_back(Event::Index(file_path.clone()));
            }
            State::IndexingInProgress {
                file_path,
                started_at,
            } => {
                println!(
                    "Indexing {} / {} / {} using {}",
                    self.index_type,
                    self.data_source,
                    self.region,
                    file_path.display()
                );
                match self.data_source.as_ref() {
                    "bano" => {
                        match bano::index_bano_region(
                            self.mimirs_dir.clone(),
                            self.es.clone(),
                            file_path.clone(),
                        ) {
                            Ok(()) => {
                                let duration = started_at.elapsed();
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
                                    let duration = started_at.elapsed();
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
                                let duration = started_at.elapsed();
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
                    _ => {
                        self.events.push_back(Event::IndexingError(format!(
                            "Dont know how to index {}",
                            &self.data_source
                        )));
                    }
                }
            }
            State::IndexingError { details } => {
                println!("Indexing Error: {}", details);
            }
            State::Indexed { duration } => {
                println!(
                    "Indexed {} {} in {}s",
                    self.data_source,
                    self.region,
                    duration.as_secs()
                );
                self.events.push_back(Event::Validate);
            }
            State::ValidationInProgress => {
                println!("Validating");
                std::thread::sleep(std::time::Duration::from_secs(1));
                println!("Validation complete");
                self.events.push_back(Event::ValidationComplete);
            }
            State::ValidationError { details } => {
                println!("Validation Error: {}", details);
            }
            State::Available => {
                println!("Available");
            }
            State::Failure(_) => {}
        }
    }
    fn drive(&mut self) {
        self.events.push_back(Event::Download);
        while let Some(event) = self.events.pop_front() {
            self.next(event);
            if let State::Failure(string) = &self.state {
                println!("{}", string);
                break;
            } else {
                self.run()
            }
        }
    }
}

fn main() -> Result<(), error::Error> {
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
    let mut driver = Driver::new(index_type, data_source, region);
    driver.drive();
    Ok(())
}
