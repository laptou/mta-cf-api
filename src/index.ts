import { DurableObject } from "cloudflare:workers";
import { Temporal } from "temporal-polyfill";
import { unpack_csv_archive } from "../gtfs-static/pkg/gtfs_static_patched";

function parseGtfsDate(date: string): Temporal.PlainDate {
	const year = Number.parseInt(date.slice(0, 4), 10);
	const month = Number.parseInt(date.slice(4, 6), 10);
	const day = Number.parseInt(date.slice(6, 8), 10);
	return new Temporal.PlainDate(year, month, day);
}

const MTA_SUPPLEMENTED_GTFS_STATIC_URL =
	"https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";

export class MtaStateObject extends DurableObject {
	sql: SqlStorage;

	constructor(state: DurableObjectState, env: Env) {
		super(state, env);

		// Get the SQL storage interface.
		this.sql = state.storage.sql;

		state.blockConcurrencyWhile(async () => {
			await this.initializeDatabase();
			await this.loadGtfsStatic();
		});
	}

	private async initializeDatabase() {
		// The SQL API supports multiple statements separated by semicolons.
		this.sql.exec(`
      CREATE TABLE IF NOT EXISTS calendar (
        service_id TEXT PRIMARY KEY,
        monday INTEGER,
        tuesday INTEGER,
        wednesday INTEGER, 
        thursday INTEGER,
        friday INTEGER,
        saturday INTEGER,
        sunday INTEGER,
        start_date TEXT,
        end_date TEXT
      );

      CREATE TABLE IF NOT EXISTS calendar_dates (
        service_id TEXT,
        date TEXT,
        exception_type INTEGER,
        PRIMARY KEY (service_id, date)
      );

      CREATE TABLE IF NOT EXISTS routes (
        route_id TEXT PRIMARY KEY,
        agency_id TEXT,
        route_short_name TEXT,
        route_long_name TEXT,
        route_type INTEGER,
        route_desc TEXT,
        route_url TEXT,
        route_color TEXT,
        route_text_color TEXT
      );

      CREATE TABLE IF NOT EXISTS stop_times (
        trip_id TEXT,
        stop_id TEXT,
        arrival_hours INTEGER,
        arrival_minutes INTEGER,
        arrival_seconds INTEGER,
				arrival_total_seconds INTEGER,
        departure_hours INTEGER,
        departure_minutes INTEGER,
        departure_seconds INTEGER,
				departure_total_seconds INTEGER,
        stop_sequence INTEGER,
        PRIMARY KEY (trip_id, stop_id)
      );

      CREATE TABLE IF NOT EXISTS stops (
        stop_id TEXT PRIMARY KEY,
        stop_name TEXT,
        stop_lat REAL,
        stop_lon REAL,
        location_type INTEGER,
        parent_station TEXT
      );

      CREATE TABLE IF NOT EXISTS transfers (
        from_stop_id TEXT,
        to_stop_id TEXT,
        transfer_type INTEGER,
        min_transfer_time INTEGER,
        PRIMARY KEY (from_stop_id, to_stop_id)
      );

      CREATE TABLE IF NOT EXISTS trips (
        route_id TEXT,
        trip_id TEXT PRIMARY KEY,
        service_id TEXT,
        trip_headsign TEXT,
        direction_id TEXT,
        shape_id TEXT
      );

			CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id);
      CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips(route_id);
      CREATE INDEX IF NOT EXISTS idx_stops_parent_station ON stops(parent_station);

      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
      );
    `);
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		console.log("handling response", url.pathname);

		let response: Response;
		switch (url.pathname) {
			case "/lines":
				response = await this.handleGetAllLines();
				break;
			case "/stations":
				response = await this.handleGetStationsForLine(url.searchParams);
				break;
			case "/station":
				response = await this.handleGetStation(url.searchParams);
				break;
			case "/arrivals":
				response = await this.handleGetUpcomingArrivals(url.searchParams);
				break;
			default:
				response = new Response("not found", { status: 404 });
		}

		return response;
	}

	// --- Handler Implementations ---

	/**
	 * Loads data from the GTFS zip file into SQL.
	 */
	private async shouldUpdateGtfs(): Promise<boolean> {
		const cursor = this.sql.exec<{ value: string }>(
			"SELECT value FROM metadata WHERE key = 'last_gtfs_update'",
		);
		const rows = cursor.toArray();
		if (rows.length === 0) return true;

		const lastUpdate = Temporal.Instant.from(rows[0].value);
		const now = Temporal.Now.instant();
		const diff = now.since(lastUpdate);
		console.log("last gtfs update", lastUpdate.toString(), diff.toString());
		return Temporal.Duration.compare(diff, { hours: 1 }) > 0;
	}

	async loadGtfsStatic() {
		if (!(await this.shouldUpdateGtfs())) {
			console.log("not updating static gtfs");
			return;
		}

		console.log("updating static gtfs");

		try {
			const gtfsResponse = await fetch(MTA_SUPPLEMENTED_GTFS_STATIC_URL);
			const buf = await gtfsResponse.bytes();

			unpack_csv_archive(buf, 50_000, this.ctx.storage);

			console.log("gtfs static timetable update completed");

			// Update the last update timestamp
			const now = Temporal.Now.instant();
			this.sql.exec(
				"INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
				"last_gtfs_update",
				now.toString(),
			);
		} catch (err) {
			console.error(err);
			throw err instanceof Error ? err : new Error(String(err));
		}
	}

	/**
	 * Returns all lines (routes).
	 */
	async handleGetAllLines(): Promise<Response> {
		try {
			// Use a generic type in case you need type hints.
			type RouteRow = {
				route_id: string;
				agency_id: string;
				route_short_name: string;
				route_long_name: string;
				route_type: number;
				route_desc: string | null;
				route_url: string | null;
				route_color: string | null;
				route_text_color: string | null;
			};

			const cursor = this.sql.exec<RouteRow>("SELECT * FROM routes");
			const routes = cursor.toArray();
			const camelCaseRoutes = routes.map((route) => ({
				routeId: route.route_id,
				agencyId: route.agency_id,
				routeShortName: route.route_short_name,
				routeLongName: route.route_long_name,
				routeType: route.route_type,
				routeDesc: route.route_desc,
				routeUrl: route.route_url,
				routeColor: route.route_color,
				routeTextColor: route.route_text_color,
			}));
			return new Response(JSON.stringify(camelCaseRoutes), {
				headers: { "Content-Type": "application/json" },
			});
		} catch (e) {
			const error = e as Error;
			return new Response(`Error: ${error.message}`, { status: 500 });
		}
	}

	/**
	 * Returns stations for a given line id.
	 * Uses the trips table to find trips for the route and then all distinct stops.
	 */
	async handleGetStationsForLine(params: URLSearchParams): Promise<Response> {
		const lineId = params.get("lineId");
		if (!lineId) {
			return new Response("missing lineId parameter", { status: 400 });
		}

		try {
			const stopsCursor = this.sql.exec(
				`
				SELECT DISTINCT s.*
				FROM stops s
				WHERE s.stop_id IN (
					SELECT st.stop_id
					FROM stop_times st
					JOIN trips t ON t.trip_id = st.trip_id
					WHERE t.route_id = ?
				)
				OR s.stop_id IN (
					SELECT DISTINCT s2.parent_station
					FROM stops s2
					JOIN stop_times st ON st.stop_id = s2.stop_id
					JOIN trips t ON t.trip_id = st.trip_id
					WHERE t.route_id = ?
						AND s2.parent_station IS NOT NULL
				)
				`,
				lineId,
				lineId,
			);

			const stopsResult = stopsCursor.toArray();
			const camelCaseStopsResult = stopsResult.map((stop) => ({
				stopId: stop.stop_id,
				stopCode: stop.stop_code,
				stopName: stop.stop_name,
				stopDesc: stop.stop_desc,
				stopLat: stop.stop_lat,
				stopLon: stop.stop_lon,
				zoneId: stop.zone_id,
				stopUrl: stop.stop_url,
				locationType: stop.location_type,
				parentStation: stop.parent_station,
				stopTimezone: stop.stop_timezone,
				wheelchairBoarding: stop.wheelchair_boarding,
				levelId: stop.level_id,
				platformCode: stop.platform_code,
			}));

			return new Response(JSON.stringify(camelCaseStopsResult), {
				headers: { "Content-Type": "application/json" },
			});
		} catch (e) {
			console.error(e);
			return new Response(`Error: ${e}`, { status: 500 });
		}
	}

	/**
	 * Returns the station details for a given stationId.
	 */
	async handleGetStation(params: URLSearchParams): Promise<Response> {
		const stationId = params.get("stationId");
		if (!stationId) {
			return new Response("Missing stationId parameter", { status: 400 });
		}
		try {
			const cursor = this.sql.exec(
				"SELECT * FROM stops WHERE stop_id = ?",
				stationId,
			);

			const rows = cursor.toArray();
			if (rows.length === 0) {
				return new Response("station not found", { status: 404 });
			}

			const stop = rows[0];
			const camelCaseStop = {
				stopId: stop.stop_id,
				stopCode: stop.stop_code,
				stopName: stop.stop_name,
				stopDesc: stop.stop_desc,
				stopLat: stop.stop_lat,
				stopLon: stop.stop_lon,
				zoneId: stop.zone_id,
				stopUrl: stop.stop_url,
				locationType: stop.location_type,
				parentStation: stop.parent_station,
				stopTimezone: stop.stop_timezone,
				wheelchairBoarding: stop.wheelchair_boarding,
				levelId: stop.level_id,
				platformCode: stop.platform_code,
			};
			return new Response(JSON.stringify(camelCaseStop), {
				headers: { "Content-Type": "application/json" },
			});
		} catch (e) {
			return new Response(`Error: ${e}`, { status: 500 });
		}
	}

	/**
	 * Returns upcoming arrivals for a given station.
	 * Requires stationId, an optional direction (north or south),
	 * and an optional limit (default to 10).
	 */
	async handleGetUpcomingArrivals(params: URLSearchParams): Promise<Response> {
		const stationId = params.get("stationId");
		const lineId = params.get("lineId");

		if (!stationId) {
			return new Response("missing stationId parameter", { status: 400 });
		}

		const limit = Math.min(
			params.get("limit") ? Number(params.get("limit")) : 10,
			15,
		);

		try {
			// Get the stop details for the given stationId.
			const stopCursor = this.sql.exec(
				"SELECT * FROM stops WHERE stop_id = $1 OR parent_station = $1",
				stationId,
			);
			const stops = stopCursor.toArray();

			if (stops.length === 0) {
				return new Response("station not found", { status: 404 });
			}

			// Use the provided stationId as the stop_id.
			// Get all upcoming stop_times for that stop joined with trips.
			type ArrivalRow = {
				trip_id: string;
				stop_id: string;
				stop_name: string;
				arrival_hours: number;
				arrival_minutes: number;
				arrival_seconds: number;
				departure_hours: number;
				departure_minutes: number;
				departure_seconds: number;
				stop_sequence: number;
				route_id: string;
				service_id: string;
			};

			const now = Temporal.Now.plainTimeISO('America/New_York');
			const nowTotalSeconds = now
				.since(Temporal.PlainTime.from("00:00:00"))
				.total("second") | 0;
			// const nowTotalSeconds = 49_000;

			console.log({ now, nowTotalSeconds });

			const arrivalCursor = lineId
				? this.sql.exec<ArrivalRow>(
						`SELECT st.*, t.route_id, t.service_id, s.stop_name
						FROM stop_times st
						JOIN trips t ON t.trip_id = st.trip_id
						JOIN stops s ON s.stop_id = st.stop_id
						WHERE (s.stop_id == $1 or s.parent_station == $1) AND (t.route_id == $2) AND (st.arrival_total_seconds >= $3)
						ORDER BY st.arrival_total_seconds
						LIMIT $4`,
						stationId,
						lineId,
						nowTotalSeconds,
						// double the limit to account for services that aren't active today
						limit * 2,
					)
				: this.sql.exec<ArrivalRow>(
						`SELECT st.*, t.route_id, t.service_id, s.stop_name
         FROM stop_times st
         JOIN trips t ON t.trip_id = st.trip_id
         JOIN stops s ON s.stop_id = st.stop_id
         WHERE (s.stop_id == $1 OR s.parent_station == $1) AND (st.arrival_total_seconds >= $2)
         ORDER BY st.arrival_total_seconds
				 LIMIT $3`,
						stationId,
						nowTotalSeconds,
						// double the limit to account for services that aren't active today
						limit * 2,
					);

			const rows = arrivalCursor.toArray();
			const upcoming: {
				line: string;
				tripId: string;
				stopId: string;
				stopName: string;
				arrivalTime: Temporal.PlainTime;
				departureTime: Temporal.PlainTime;
				stopSequence: number;
			}[] = [];

			// For each stop_time row, check if:
			// • the arrival time is later than now, and
			// • the service is active today.
			for (const row of rows) {
				console.log(row);
				const arrivalTime = Temporal.PlainTime.from({
					hour: row.arrival_hours,
					minute: row.arrival_minutes,
					second: row.arrival_seconds,
				});

				// Check if the service for this row is active today.
				const isActive = await this.isServiceActiveToday(row.service_id);
				if (!isActive) continue;

				const departureTime = Temporal.PlainTime.from({
					hour: row.departure_hours,
					minute: row.departure_minutes,
					second: row.departure_seconds,
				});

				upcoming.push({
					line: row.route_id,
					tripId: row.trip_id,
					stopId: row.stop_id,
					stopName: row.stop_name,
					arrivalTime,
					departureTime,
					stopSequence: row.stop_sequence,
				});

				if (upcoming.length >= limit) break;
			}

			// Return the collected upcoming arrivals.
			return new Response(JSON.stringify(upcoming), {
				headers: { "Content-Type": "application/json" },
			});
		} catch (e) {
			return new Response(`Error: ${e}`, { status: 500 });
		}
	}

	/**
	 * Checks if a given service (by service_id) is active today.
	 * Converts stored strings (YYYYMMDD) to Temporal.PlainDate for comparisons.
	 */
	async isServiceActiveToday(serviceId: string): Promise<boolean> {
		const today = Temporal.Now.plainDateISO();
		type CalendarRow = {
			service_id: string;
			monday: number;
			tuesday: number;
			wednesday: number;
			thursday: number;
			friday: number;
			saturday: number;
			sunday: number;
			start_date: string;
			end_date: string;
		};

		const cursor = this.sql.exec<CalendarRow>(
			"SELECT * FROM calendar WHERE service_id = ?",
			[serviceId],
		);
		const rows = cursor.toArray();
		if (rows.length === 0) return false;
		const service = rows[0];
		// Convert start_date and end_date strings to Temporal.PlainDate.
		const startDate = parseGtfsDate(service.start_date);
		const endDate = parseGtfsDate(service.end_date);
		if (
			Temporal.PlainDate.compare(today, startDate) < 0 ||
			Temporal.PlainDate.compare(today, endDate) > 0
		) {
			return false;
		}

		// Using ISO weekday numbering: Monday=1, Sunday=7.
		const day = today.dayOfWeek;
		switch (day) {
			case 1:
				return service.monday === 1;
			case 2:
				return service.tuesday === 1;
			case 3:
				return service.wednesday === 1;
			case 4:
				return service.thursday === 1;
			case 5:
				return service.friday === 1;
			case 6:
				return service.saturday === 1;
			case 7:
				return service.sunday === 1;
			default:
				return false;
		}
	}
}

export default {
	async fetch(request: Request, env, ctx: ExecutionContext): Promise<Response> {
		const id = env.MTA_STATE_OBJECT.idFromName("mta");
		const stub = env.MTA_STATE_OBJECT.get(id);
		return stub.fetch(request);
	},
} satisfies ExportedHandler<Env>;
