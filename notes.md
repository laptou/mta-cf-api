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

- holy shit. transactions was the unlock i needed. insert + response time went down to 4211ms. that's with no index, let's try adding one. 
- it's still insanely fast. just recorded 3910ms.

- IT WORKS IN THE CLOUD!!

so to recap, i build a static gtfs timetable importer. it was too slow, so
cloudflare was killing the durable object while it was trying to fill up the
database. i assumed that the bottleneck was in zipping & unzipping, so i tried a
different zip library, and then i tried performing the unzipping in rust. this
yielded some modest performance gains, but it still wasn't fast enough, so i
profiled it. profiling revealed that most of the time was actually spent in the
sqlite.exec call and not in decompression, so i tried disabling the index, which
yielded modest performance gains, and i tried using transactions, which yielded
huge performance gains.

now it loads the gtfs tables very fast, and my API is working in the cloud.

- i tried reverting to a JS implementation that uses transactions. it's unusably
  slow. so at least i didn't write all this rust for no reason.
- woah. it's way slower if i use transactionSync instead of transaction. i
  wonder why. i assumed that the difference was related to whether or not you
  needed to execute async code in the callback, so i switched it to
  transactionSync. i also used transactionSync in the slow js implementation.
