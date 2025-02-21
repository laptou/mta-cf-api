// src/lib.rs

mod utils;

use anyhow::{anyhow, Context};
use csv::StringRecord;
use itertools::Itertools;
use js_sys::{Array, Function, JsString, Object, Reflect, Uint8Array};
use serde::Deserialize;
use std::{io::Cursor, str};
use wasm_bindgen::prelude::*;

// --- Structures for each file type ---

#[derive(Debug, Deserialize)]
struct CalendarRow {
    service_id: String,
    monday: u8,
    tuesday: u8,
    wednesday: u8,
    thursday: u8,
    friday: u8,
    saturday: u8,
    sunday: u8,
    start_date: String,
    end_date: String,
}

#[derive(Debug, Deserialize)]
struct CalendarDatesRow {
    service_id: String,
    date: String,
    exception_type: u8,
}

#[derive(Debug, Deserialize)]
struct RoutesRow {
    route_id: String,
    agency_id: String,
    route_short_name: String,
    route_long_name: String,
    route_type: u8,
    route_desc: Option<String>,
    route_url: Option<String>,
    route_color: Option<String>,
    route_text_color: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StopTimesRow {
    trip_id: String,
    stop_id: String,
    arrival_time: String,
    departure_time: String,
    stop_sequence: u32,
}

#[derive(Debug, Deserialize)]
struct StopsRow {
    stop_id: String,
    stop_name: String,
    stop_lat: f64,
    stop_lon: f64,
    location_type: Option<u8>,
    parent_station: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TransfersRow {
    from_stop_id: String,
    to_stop_id: String,
    transfer_type: u8,
    min_transfer_time: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TripsRow {
    route_id: String,
    trip_id: String,
    service_id: String,
    trip_headsign: String,
    direction_id: String,
    shape_id: String,
}

// --- Helper functions to process times etc. ---

fn parse_gtfs_time(time: &str) -> anyhow::Result<(u32, u32, u32)> {
    if time.len() < 8 {
        anyhow::bail!("Invalid time format: {}", time);
    }
    let hours = time[0..2].parse::<u32>()?;
    let minutes = time[3..5].parse::<u32>()?;
    let seconds = time[6..8].parse::<u32>()?;
    Ok((hours % 24, minutes, seconds))
}

/// Helper: log a message using JS's console.log.
fn log(s: &str) {
    web_sys::console::log_1(&JsValue::from_str(s));
}

// --- Processing helpers for each CSV file type ---

fn process_calendar_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("processing {} calendar rows", chunk.len()));

    let sql = storage.sql();

    let query = "
INSERT OR REPLACE INTO calendar 
(service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    for record in chunk {
        let row: CalendarRow = record
            .deserialize(Some(headers))
            .context("deserialize CalendarRow")?;

        // Build the query and parameters.

        let params = Array::new();
        params.push(&JsValue::from_str(&row.service_id));
        params.push(&JsValue::from_f64(row.monday as f64));
        params.push(&JsValue::from_f64(row.tuesday as f64));
        params.push(&JsValue::from_f64(row.wednesday as f64));
        params.push(&JsValue::from_f64(row.thursday as f64));
        params.push(&JsValue::from_f64(row.friday as f64));
        params.push(&JsValue::from_f64(row.saturday as f64));
        params.push(&JsValue::from_f64(row.sunday as f64));
        params.push(&JsValue::from_str(&row.start_date));
        params.push(&JsValue::from_str(&row.end_date));

        sql.exec(query, &params)
            .map_err(|err| anyhow!("sql_exec for calendar: {err:?}"))?
    }

    Ok(())
}

fn process_calendar_dates_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("processing {} calendar_dates rows", chunk.len()));

    let sql = storage.sql();
    let query = "
INSERT OR REPLACE INTO calendar_dates 
(service_id, date, exception_type) VALUES (?, ?, ?)";

    for record in chunk {
        let row: CalendarDatesRow = record
            .deserialize(Some(headers))
            .context("deserialize CalendarDatesRow")?;

        let params = Array::new();
        params.push(&JsValue::from_str(&row.service_id));
        params.push(&JsValue::from_str(&row.date));
        params.push(&JsValue::from_f64(row.exception_type as f64));

        sql.exec(query, &params)
            .map_err(|err| anyhow!("sql_exec for calendar_dates: {err:?}"))?;
    }

    Ok(())
}

fn process_routes_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("processing {} routes rows", chunk.len()));

    let sql = storage.sql();

    let query = "
INSERT OR REPLACE INTO routes 
(route_id, agency_id, route_short_name, route_long_name, route_type, 
route_desc, route_url, route_color, route_text_color)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    for record in chunk {
        let row: RoutesRow = record
            .deserialize(Some(headers))
            .context("deserialize RoutesRow")?;

        let params = Array::new();
        params.push(&JsValue::from_str(&row.route_id));
        params.push(&JsValue::from_str(&row.agency_id));
        params.push(&JsValue::from_str(&row.route_short_name));
        params.push(&JsValue::from_str(&row.route_long_name));
        params.push(&JsValue::from_f64(row.route_type as f64));
        params.push(&JsValue::from_str(row.route_desc.as_deref().unwrap_or("")));
        params.push(&JsValue::from_str(row.route_url.as_deref().unwrap_or("")));
        params.push(&JsValue::from_str(row.route_color.as_deref().unwrap_or("")));
        params.push(&JsValue::from_str(
            row.route_text_color.as_deref().unwrap_or(""),
        ));

        sql.exec(query, &params)
            .map_err(|err| anyhow!("sql_exec for routes: {err:?}"))?;
    }
    Ok(())
}

fn process_stop_times_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("processing {} stop_times rows", chunk.len()));

    let sql = storage.sql();

    storage.transaction_sync(&mut || {
        let query = "
        INSERT OR REPLACE INTO stop_times
        (trip_id, stop_id, arrival_hours, arrival_minutes, arrival_seconds, 
        departure_hours, departure_minutes, departure_seconds, stop_sequence) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        let query = &std::iter::repeat_n(query, 1).join(";");

        let params = Array::new_with_length(9);

        let trip_id_idx = headers.iter().position(|h| h == "trip_id").unwrap();
        let stop_id_idx = headers.iter().position(|h| h == "stop_id").unwrap();
        let arrival_time_idx = headers.iter().position(|h| h == "arrival_time").unwrap();
        let departure_time_idx = headers.iter().position(|h| h == "departure_time").unwrap();
        let stop_sequence_idx = headers.iter().position(|h| h == "stop_sequence").unwrap();

        for record in chunk {
            let row = record.iter().collect_vec();

            let trip_id = row[trip_id_idx];
            let stop_id = row[stop_id_idx];
            let arrival_time = row[arrival_time_idx];
            let departure_time = row[departure_time_idx];
            let stop_sequence = u32::from_str_radix(row[stop_sequence_idx], 10).unwrap();

            let (a_h, a_m, a_s) = parse_gtfs_time(arrival_time)
                .context("parse arrival_time")
                .map_err(|err| JsValue::from_str(&format!("{err:?}")))?;
            let (d_h, d_m, d_s) = parse_gtfs_time(departure_time)
                .context("parse departure_time")
                .map_err(|err| JsValue::from_str(&format!("{err:?}")))?;

            params.set(0, JsValue::from_str(&trip_id));
            params.set(1, JsValue::from_str(&stop_id));
            params.set(2, JsValue::from_f64(a_h as f64));
            params.set(3, JsValue::from_f64(a_m as f64));
            params.set(4, JsValue::from_f64(a_s as f64));
            params.set(5, JsValue::from_f64(d_h as f64));
            params.set(6, JsValue::from_f64(d_m as f64));
            params.set(7, JsValue::from_f64(d_s as f64));
            params.set(8, JsValue::from_f64(stop_sequence as f64));

            sql.exec(query, &params)?;
        }

        Ok(())
    });

    // // having an index enabled while inserting 2.2 million rows is too slow
    // sql_exec
    //     .apply(
    //         &JsValue::NULL,
    //         &Array::from_iter([JsValue::from_str(
    //             "DROP INDEX IF EXISTS idx_stop_times_stop_id",
    //         )]),
    //     )
    //     .map_err(|err| anyhow!("sql_exec for stop_times drop index: {err:?}"))?;

    // sql_exec
    //     .apply(
    //         &JsValue::NULL,
    //         &Array::from_iter([JsValue::from_str(
    //             "CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id)",
    //         )]),
    //     )
    //     .map_err(|err| anyhow!("sql_exec for stop_times create index: {err:?}"))?;

    Ok(())
}

fn process_stops_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("Processing {} stops rows", chunk.len()));
    let sql = storage.sql();

    let query = 
        "
INSERT OR REPLACE INTO stops
(stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station)
VALUES (?, ?, ?, ?, ?, ?)";

    for record in chunk {
        let row: StopsRow = record
            .deserialize(Some(headers))
            .context("deserialize StopsRow")?;

        let params = Array::new();
        
        params.push(&JsValue::from_str(&row.stop_id));
        params.push(&JsValue::from_str(&row.stop_name));
        params.push(&JsValue::from_f64(row.stop_lat));
        params.push(&JsValue::from_f64(row.stop_lon));
        params.push(&match row.location_type {
            Some(location_ty) => JsValue::from_f64(location_ty as f64),
            None => JsValue::null(),
        });
        // parent_station might be null; pass empty string if not present.
        params.push(&JsValue::from_str(
            row.parent_station.as_deref().unwrap_or(""),
        ));

        sql.exec(query, &params)
            .map_err(|err| anyhow!("sql_exec for stops: {err:?}"))?;
    }
    Ok(())
}

fn process_transfers_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("Processing {} transfers rows", chunk.len()));

    let sql = storage.sql();

    let query = "\
INSERT OR REPLACE INTO transfers \
(from_stop_id, to_stop_id, transfer_type, min_transfer_time) \
VALUES (?, ?, ?, ?)";

    for record in chunk {
        let row: TransfersRow = record
            .deserialize(Some(headers))
            .context("deserialize TransfersRow")?;

        let params = Array::new();
        
        params.push(&JsValue::from_str(&row.from_stop_id));
        params.push(&JsValue::from_str(&row.to_stop_id));
        params.push(&JsValue::from_f64(row.transfer_type as f64));

        // If min_transfer_time is None, we pass null.
        match row.min_transfer_time {
            Some(t) => params.push(&JsValue::from_f64(t as f64)),
            None => params.push(&JsValue::NULL),
        };

        sql.exec(query, &params)
            .map_err(|err| anyhow!("sql_exec for transfers: {err:?}"))?;
    }
    Ok(())
}

fn process_trips_rows(
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    log(&format!("processing {} trips rows", chunk.len()));

    let sql = storage.sql();

    let query =
        "\
INSERT OR REPLACE INTO trips \
(route_id, trip_id, service_id, trip_headsign, direction_id, shape_id) \
VALUES (?, ?, ?, ?, ?, ?)";

    for record in chunk {
        let row: TripsRow = record
            .deserialize(Some(headers))
            .context("deserialize TripsRow")?;

        let params = Array::new();
        
        params.push(&JsValue::from_str(&row.route_id));
        params.push(&JsValue::from_str(&row.trip_id));
        params.push(&JsValue::from_str(&row.service_id));
        params.push(&JsValue::from_str(&row.trip_headsign));
        params.push(&JsValue::from_str(&row.direction_id));
        params.push(&JsValue::from_str(&row.shape_id));

        sql.exec(query, &params)
            .map_err(|err| anyhow!("sql_exec for trips: {err:?}"))?;
    }
    Ok(())
}

// --- Main entry point ---

#[wasm_bindgen]
extern "C" {
    pub type DurableObjectStorage;

    #[wasm_bindgen(method, getter)]
    pub fn sql(this: &DurableObjectStorage) -> DurableObjectSqlStorage;

    #[wasm_bindgen(method)]
    pub fn transaction_sync(this: &DurableObjectStorage, cb: &mut dyn FnMut() -> Result<(), JsValue>);

    pub type DurableObjectSqlStorage;

    #[wasm_bindgen(method, catch, variadic)]
    pub fn exec(
        this: &DurableObjectSqlStorage,
        query: &str,
        bindings: &Array,
    ) -> Result<(), JsValue>;
}

#[wasm_bindgen]
pub fn unpack_csv_archive(
    buf: Box<[u8]>,
    row_chunk_len: usize,
    storage: DurableObjectStorage,
) -> Result<(), JsValue> {
    unpack_csv_archive_inner(&buf, row_chunk_len, &storage)
        .map_err(|err| JsValue::from_str(&format!("{err:?}")))
}

fn unpack_csv_archive_inner(
    buf: &[u8],
    row_chunk_len: usize,
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(buf)).context("could not create zip archive")?;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .with_context(|| format!("could not open file at index {i}"))?;

        let file_name = file.name().to_owned();
        if file_name == "shapes.txt" {
            println!("skipping shapes.txt");
        }

        log(&format!("extracting file {}", file_name));

        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(&mut file);

        let headers = rdr.headers()?.clone();

        log(&format!(
            "headers for {}: {}",
            file_name,
            headers.iter().join(", ")
        ));

        let mut total = 0;

        let mut chunk: Vec<StringRecord> = Vec::with_capacity(row_chunk_len);
        for result in rdr.records() {
            let record = result.context("csv record parse error")?;
            chunk.push(record);
            total += 1;

            if chunk.len() >= row_chunk_len {
                process_chunk(&file_name, &headers, &chunk, storage)?;

                log(&format!(
                    "processed {} rows ({total} total) from {}",
                    chunk.len(),
                    file_name
                ));

                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            process_chunk(&file_name, &headers, &chunk, storage)?;

            log(&format!(
                "processed {} rows ({total} total) from {} (final chunk)",
                chunk.len(),
                file_name
            ));
        }
    }
    Ok(())
}

/// Dispatch the chunk to the appropriate processor based on file name.
fn process_chunk(
    file_name: &str,
    headers: &StringRecord,
    chunk: &[StringRecord],
    storage: &DurableObjectStorage,
) -> anyhow::Result<()> {
    match file_name {
        "calendar.txt" => process_calendar_rows(headers, chunk, storage),
        "calendar_dates.txt" => process_calendar_dates_rows(headers, chunk, storage),
        "routes.txt" => process_routes_rows(headers, chunk, storage),
        "stop_times.txt" => process_stop_times_rows(headers, chunk, storage),
        "stops.txt" => process_stops_rows(headers, chunk, storage),
        "transfers.txt" => process_transfers_rows(headers, chunk, storage),
        "trips.txt" => process_trips_rows(headers, chunk, storage),
        other => {
            log(&format!("Skipping unknown file: {}", other));
            Ok(())
        }
    }
}
