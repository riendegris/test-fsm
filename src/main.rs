use std::collections::VecDeque;
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

impl State {
    fn next(self, event: Event) -> State {
        match (self, event) {
            (State::NotAvailable, Event::Download(i, d, r)) => State::DownloadingInProgress {
                index_type: i,
                data_source: d,
                region: r,
                started_at: Instant::now(),
            },
            (
                State::DownloadingInProgress {
                    index_type,
                    data_source,
                    region,
                    started_at,
                },
                Event::DownloadingError(d),
            ) => State::DownloadingError { details: d },
            (
                State::DownloadingInProgress {
                    index_type,
                    data_source,
                    region,
                    started_at,
                },
                Event::DownloadingComplete,
            ) => State::Downloaded,
            (State::DownloadingError { details }, Event::Reset) => State::NotAvailable,
            (State::Downloaded, Event::Index) => State::IndexingInProgress,
            (State::IndexingInProgress, Event::IndexingError(d)) => {
                State::IndexingError { details: d }
            }
            (State::IndexingError { details }, Event::Reset) => State::NotAvailable,
            (State::IndexingInProgress, Event::IndexingComplete) => State::Indexed,
            (State::Indexed, Event::Validate) => State::ValidationInProgress,
            (State::ValidationInProgress, Event::ValidationError(d)) => {
                State::ValidationError { details: d }
            }
            (State::ValidationError { details }, Event::Reset) => State::NotAvailable,
            (State::ValidationInProgress, Event::ValidationComplete) => State::Available,
            (s, e) => State::Failure(
                format!("Wrong state, event combination: {:#?} {:#?}", s, e).to_string(),
            ),
        }
    }

    fn run(&self, events: &mut VecDeque<Event>) {
        match self {
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
                events.push_back(Event::DownloadingComplete);
            }
            State::DownloadingError { details } => {
                println!("Downloading Error: {}", details);
            }
            State::Downloaded => {
                println!("Downloaded");
                events.push_back(Event::Index);
            }
            State::IndexingInProgress => {
                println!("Indexing");
                std::thread::sleep(std::time::Duration::from_secs(3));
                println!("Indexing complete");
                events.push_back(Event::IndexingComplete);
            }
            State::IndexingError { details } => {
                println!("Indexing Error: {}", details);
            }
            State::Indexed => {
                println!("Indexed");
                events.push_back(Event::Validate);
            }
            State::ValidationInProgress => {
                println!("Validating");
                std::thread::sleep(std::time::Duration::from_secs(1));
                println!("Validation complete");
                events.push_back(Event::ValidationComplete);
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
}

fn main() {
    let mut state = State::NotAvailable;

    let mut events = VecDeque::new();

    events.push_back(Event::Download(
        String::from("admins"),
        String::from("cosmogony"),
        String::from("france"),
    ));

    while let Some(event) = events.pop_front() {
        // println!("Received event: {:?}", event);
        // println!("Current state: {:?}", state);
        state = state.next(event);
        // println!("New state: {:?}", state);
        if let State::Failure(string) = state {
            println!("{}", string);
            break;
        } else {
            state.run(&mut events)
        }
    }
}
