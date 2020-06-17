use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("IOError {}: {}", details, source))]
    #[snafu(visibility(pub))]
    IOError {
        details: String,
        source: std::io::Error,
    },
    #[snafu(display("ReqwestError {}: {}", details, source))]
    #[snafu(visibility(pub))]
    ReqwestError {
        details: String,
        source: reqwest::Error,
    },
    #[snafu(display("Miscellaneous Error {}", details))]
    #[snafu(visibility(pub))]
    MiscError { details: String },

    #[snafu(display("URL Error {}: {}", details, source))]
    #[snafu(visibility(pub))]
    URLError {
        details: String,
        source: url::ParseError,
    },

    #[snafu(display("Serde JSON Error {}: {}", details, source))]
    #[snafu(visibility(pub))]
    SerdeJSONError {
        details: String,
        source: serde_json::error::Error,
    },
}
