mod utils;

use std::{io::Cursor, str::FromStr};

use anyhow::Context;
use itertools::Itertools;
use js_sys::{Function, JsString, Uint8Array};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn unpack_csv_archive(
    data: Uint8Array,
    row_chunk_len: usize,
    callback: Function,
) -> Result<(), wasm_bindgen::JsError> {
    let mut buf = Vec::with_capacity(data.byte_length() as usize);
    data.copy_to_uninit(buf.spare_capacity_mut());
    unsafe { buf.set_len(data.byte_length() as usize) };

    unpack_csv_archive_inner(
        &buf,
        row_chunk_len,
        |file_name, row_chunk_headers, row_chunk| {
            callback
                .call3(
                    &callback,
                    &JsValue::from_str(file_name),
                    row_chunk_headers,
                    row_chunk,
                )
                .map_err(|err| anyhow::anyhow!("callback failed: {:?}", err))?;

            Ok(())
        },
    )
    .map_err(|err| wasm_bindgen::JsError::new(&err.to_string()))?;

    Ok(())
}

pub fn unpack_csv_archive_inner<
    F: FnMut(&str, &js_sys::Array, &js_sys::Array) -> anyhow::Result<()>,
>(
    buf: &[u8],
    row_chunk_len: usize,
    mut callback: F,
) -> anyhow::Result<()> {
    let mut ar = zip::ZipArchive::new(Cursor::new(buf)).context("could not create zip archive")?;

    for i in 0..ar.len() {
        let file = ar
            .by_index(i)
            .with_context(|| format!("could not open file at index {i}"))?;

        let file_name = file.name().to_owned();

        let mut reader = csv::ReaderBuilder::new().from_reader(file);
        let headers = reader.headers()?;
        let headers_arr: js_sys::Array = headers.iter().map(JsString::from_str).try_collect()?;

        let mut chunk = vec![];

        for record in reader.into_records() {
            let record = record?;

            let record_arr: js_sys::Array = record.iter().map(JsString::from_str).try_collect()?;

            chunk.push(record_arr);

            if chunk.len() >= row_chunk_len {
                callback(&file_name, &headers_arr, &js_sys::Array::from_iter(chunk))?;
                chunk = vec![];
            }
        }

        if chunk.len() > 0 {
            callback(&file_name, &headers_arr, &js_sys::Array::from_iter(chunk))?;
        }
    }

    Ok(())
}
