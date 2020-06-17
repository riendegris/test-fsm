use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct NTFSDownload {
    format: String,
    filename: String,
    width: u32,
    id: String,
    height: u32,
    thumbnail: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct NTFSFields {
    license_link: String,
    update_date: String,
    description: String,
    licence: String,
    format: String,
    validity_end_date: String,
    validity_start_date: String,
    download: NTFSDownload,
    id: String,
    size: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct NTFSDataset {
    datasetid: String,
    recordid: String,
    fields: NTFSFields,
    record_timestamp: String,
}
