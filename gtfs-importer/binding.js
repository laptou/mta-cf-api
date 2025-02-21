// cloudflare workers don't work with the default bindings, so we need this shim
import wasm from "./pkg/gtfs_importer_bg.wasm";
import * as imports from "./pkg/gtfs_importer_bg.js";
import { __wbg_set_wasm } from "./pkg/gtfs_importer_bg.js";

const instance = new WebAssembly.Instance(wasm, {
  "./gtfs_importer_bg.js": imports,
});

__wbg_set_wasm(instance.exports);

instance.exports.__wbindgen_start();


export {
  unpack_csv_archive as unpackCsvArchive
} from "./pkg/gtfs_importer_bg.js";
