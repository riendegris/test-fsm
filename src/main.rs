use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{delay_for, Duration, Instant};

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
    DownloadingError,
    Downloaded,
    Failure(String),
}

#[derive(Debug, Clone)]
enum Event {
    Download(String, String, String),
    DownloadingError,
    DownloadingComplete,
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
                Event::DownloadingError,
            ) => State::DownloadingError,
            (
                State::DownloadingInProgress {
                    index_type,
                    data_source,
                    region,
                    started_at,
                },
                Event::DownloadingComplete,
            ) => State::Downloaded,
            (State::DownloadingError, Event::Reset) => State::NotAvailable,
            (s, e) => State::Failure(
                format!("Wrong state, event combination: {:#?} {:#?}", s, e).to_string(),
            ),
        }
    }

    async fn run(&self, mut tx: mpsc::Sender<Event>) {
        match *self {
            State::NotAvailable => {
                println!("Not Available");
                println!("Sending Download Event");
                tx.send(Event::Download(
                    String::from("admins"),
                    String::from("cosmogony"),
                    String::from("france"),
                ));
            }
            State::DownloadingInProgress {
                index_type,
                data_source,
                region,
                started_at,
            } => {
                println!("Downloading from {}", data_source);
                delay_for(Duration::from_secs(1)).await;
                println!("Downloading complete");
                if let Err(err) = tx.send(Event::DownloadingComplete).await {
                    println!("You should have used try_send!");
                }
            }
            State::DownloadingError => {
                println!("Downloading Error");
            }
            State::Downloaded => {
                println!("Downloaded");
            }
            State::Failure(_) => {}
        }
    }
}

async fn execute_fsm(state: Arc<Mutex<State>>, tx: mpsc::Sender<Event>) {
    // In this function, we check if we're in a failstate.
    // If we are, we return, which drops the tx, and will stop the rx loop in turn.
    // If it's not a failstate, we execute the current state.
    loop {
        match state.lock().unwrap() {
            State::Failure(string) => {
                println!("Failure: {}", string);
                return;
            }
            State::DownloadingError => {
                println!("Download Error");
                return;
            }
            state => state.run(tx.clone()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    let mut state = Arc::new(Mutex::new(State::NotAvailable));

    let (mut tx, mut rx) = mpsc::channel(10);

    tokio::spawn(async move { execute_fsm(state.clone(), tx.clone()).await });

    while let Some(e) = rx.recv().await {
        println!("Received event: {:?}", e);
        println!("Current state: {:?}", state);
        state = state.lock().unwrap().next(e);
        println!("New state: {:?}", state);
    }
    Ok(())
}
