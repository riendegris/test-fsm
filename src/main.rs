use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Instant;

// From https://gist.github.com/anonymous/ee3e4df093c136ced7b394dc7ffb78e1

#[derive(Debug, PartialEq)]
enum State {
    NotAvailable,
    DownloadingInProgress {
        index_type: String,
        data_source: String,
        region: String,
        started_at: Instant,
    },
    DownloadingError {
        details: String,
    },
    Downloaded,
    IndexingInProgress,
    IndexingError {
        details: String,
    },
    Indexed,
    ValidationInProgress,
    ValidationError {
        details: String,
    },
    Available,
    Failure(String),
}

#[derive(Debug, Clone)]
enum Event {
    Download(String, String, String),
    DownloadingError(String),
    DownloadingComplete,
    Index,
    IndexingError(String),
    IndexingComplete,
    Validate,
    ValidationError(String),
    ValidationComplete,
    Reset,
}

struct Driver {
    state: State,
    working_dir: PathBuf,
    events: VecDeque<Event>,
}

impl Driver {
    fn new() -> Self {
        Driver {
            state: State::NotAvailable,
            working_dir: PathBuf::from("./work"),
            events: VecDeque::new(),
        }
    }
    fn next(&mut self, event: Event) {
        match (&self.state, event) {
            (State::NotAvailable, Event::Download(i, d, r)) => {
                self.state = State::DownloadingInProgress {
                    index_type: i,
                    data_source: d,
                    region: r,
                    started_at: Instant::now(),
                };
            }
            (
                State::DownloadingInProgress {
                    index_type,
                    data_source,
                    region,
                    started_at,
                },
                Event::DownloadingError(ref d),
            ) => {
                self.state = State::DownloadingError {
                    details: String::from(d.as_str()),
                };
            }
            (
                State::DownloadingInProgress {
                    index_type,
                    data_source,
                    region,
                    started_at,
                },
                Event::DownloadingComplete,
            ) => {
                self.state = State::Downloaded;
            }
            (State::DownloadingError { details }, Event::Reset) => {
                self.state = State::NotAvailable;
            }
            (State::Downloaded, Event::Index) => {
                self.state = State::IndexingInProgress;
            }
            (State::IndexingInProgress, Event::IndexingError(d)) => {
                self.state = State::IndexingError { details: d }
            }
            (State::IndexingError { details }, Event::Reset) => {
                self.state = State::NotAvailable;
            }
            (State::IndexingInProgress, Event::IndexingComplete) => {
                self.state = State::Indexed;
            }
            (State::Indexed, Event::Validate) => {
                self.state = State::ValidationInProgress;
            }
            (State::ValidationInProgress, Event::ValidationError(d)) => {
                self.state = State::ValidationError { details: d }
            }
            (State::ValidationError { details }, Event::Reset) => {
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
            State::DownloadingInProgress {
                index_type,
                data_source,
                region,
                started_at,
            } => {
                println!("Downloading from {}", data_source);
                std::thread::sleep(std::time::Duration::from_secs(1));
                println!("Downloading complete");
                self.events.push_back(Event::DownloadingComplete);
            }
            State::DownloadingError { details } => {
                println!("Downloading Error: {}", details);
            }
            State::Downloaded => {
                println!("Downloaded");
                self.events.push_back(Event::Index);
            }
            State::IndexingInProgress => {
                println!("Indexing");
                std::thread::sleep(std::time::Duration::from_secs(3));
                println!("Indexing complete");
                self.events
                    .push_back(Event::IndexingError(String::from("Oops")));
            }
            State::IndexingError { details } => {
                println!("Indexing Error: {}", details);
            }
            State::Indexed => {
                println!("Indexed");
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
        self.events.push_back(Event::Download(
            String::from("admins"),
            String::from("cosmogony"),
            String::from("france"),
        ));
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

fn main() {
    let mut driver = Driver::new();
    driver.drive();
}
