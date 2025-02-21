# 21 feb

- removing the index on the stop times actually yields a huge speed up for insertions locally! got about 16600ms 
- using transactions w/ no index seems to have no effect or a negative effect? getting around 17400 ms
- using transactions w/ index is not any faster than before, getting around 22500 ms

- it turns out that i forgot to rebuild the rust project when i tried to add transactions the first time, so idk what i was measuring
- i apparently can't use SQL BEGIN statements. let's try using the alternative they suggest

```
Error: sql_exec for stop_times drop index: JsValue(Error: To execute a transaction, please use the state.storage.transaction() or state.storage.transactionSync() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT statements. The JavaScript API is safer because it will automatically roll back on exceptions, and because it interacts correctly with Durable Objects' automatic atomic write coalescing.
Error: To execute a transaction, please use the state.storage.transaction() or state.storage.transactionSync() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT statements. The JavaScript API is safer because it will automatically roll back on exceptions, and because it interacts correctly with Durable Objects' automatic atomic write coalescing.
    at file:///home/ibiyemi/projects/scratch/mta-cf-api/.wrangler/tmp/dev-68zz23/index.js:5503:22
    at handleError (file:///home/ibiyemi/projects/scratch/mta-cf-api/.wrangler/tmp/dev-68zz23/index.js:5355:15)
    at __wbg_apply_36be6a55257c99bf (file:///home/ibiyemi/projects/scratch/mta-cf-api/.wrangler/tmp/dev-68zz23/index.js:5502:10)
    at wasm://wasm/0012539a:wasm-function[281]:0x2ea7e
    at wasm://wasm/0012539a:wasm-function[18]:0x819c
    at wasm://wasm/0012539a:wasm-function[315]:0x36d32
    at unpack_csv_archive (file:///home/ibiyemi/projects/scratch/mta-cf-api/.wrangler/tmp/dev-68zz23/index.js:5495:20)
    at MtaStateObject.loadGtfsStatic (file:///home/ibiyemi/projects/scratch/mta-cf-api/.wrangler/tmp/dev-68zz23/index.js:5754:7)
```
