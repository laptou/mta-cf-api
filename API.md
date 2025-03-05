# MTA Transit API Documentation

This document details the API endpoints exposed by the MTA Transit Worker. The API provides real-time and static transit data for the New York City subway system.

## Base URL

The base URL for all endpoints will depend on your Cloudflare Workers deployment.

## Response Format

All responses are in JSON format and use camelCase property naming.

## Endpoints

### Get Route Information

Get details about a specific route, optionally including all stations and upcoming arrivals.

```typescript
GET /route?routeId={routeId}&expanded={boolean}
```

#### Parameters

- `routeId` (required): The ID of the route (e.g., "A", "1", "L")
- `expanded` (optional): If "true", includes stations and upcoming arrivals. Defaults to false.

#### Response Types

```typescript
// Basic route response (expanded=false)
type RouteResponse = {
  routeId: string;
  agencyId: string;
  routeShortName: string;
  routeLongName: string;
  routeType: number;
  routeDesc: string | null;
  routeUrl: string | null;
  routeColor: string | null;
  routeTextColor: string | null;
}

// Expanded route response (expanded=true)
type ExpandedRouteResponse = {
  route: RouteResponse;
  stations: Array<{
    stopId: string;
    stopCode: string | null;
    stopName: string;
    stopDesc: string | null;
    stopLat: number;
    stopLon: number;
    zoneId: string | null;
    stopUrl: string | null;
    locationType: number | null;
    parentStation: string | null;
    stopTimezone: string | null;
    wheelchairBoarding: number | null;
    levelId: string | null;
    platformCode: string | null;
    upcomingArrivals: Array<{
      route: string;
      tripId: string;
      stopId: string;
      stopName: string;
      arrivalTime: string; // ISO time string
      departureTime: string; // ISO time string
      isRealtimeUpdated: boolean;
      stopSequence: number;
    }>;
  }>;
}
```

### Get All Routes

Get a list of all available routes.

```typescript
GET /routes
```

#### Response Type

```typescript
type RoutesResponse = Array<{
  routeId: string;
  agencyId: string;
  routeShortName: string;
  routeLongName: string;
  routeType: number;
  routeDesc: string | null;
  routeUrl: string | null;
  routeColor: string | null;
  routeTextColor: string | null;
}>
```

### Get Stations for Route

Get all stations served by a specific route.

```typescript
GET /stations?routeId={routeId}
```

#### Parameters

- `routeId` (required): The ID of the route (e.g., "A", "1", "L")

#### Response Type

```typescript
type StationsResponse = Array<{
  stopId: string;
  stopCode: string | null;
  stopName: string;
  stopDesc: string | null;
  stopLat: number;
  stopLon: number;
  zoneId: string | null;
  stopUrl: string | null;
  locationType: number | null;
  parentStation: string | null;
  stopTimezone: string | null;
  wheelchairBoarding: number | null;
  levelId: string | null;
  platformCode: string | null;
}>
```

### Get Station Details

Get details for one or more stations.

```typescript
GET /station?stationId={stationId}
```

#### Parameters

- `stationId` (required): Single station ID or comma-separated list of station IDs

#### Response Type

```typescript
type StationResponse = Array<{
  stopId: string;
  stopCode: string | null;
  stopName: string;
  stopDesc: string | null;
  stopLat: number;
  stopLon: number;
  zoneId: string | null;
  stopUrl: string | null;
  locationType: number | null;
  parentStation: string | null;
  stopTimezone: string | null;
  wheelchairBoarding: number | null;
  levelId: string | null;
  platformCode: string | null;
}>
```

### Get Upcoming Arrivals

Get upcoming arrivals for a specific station.

```typescript
GET /arrivals?stationId={stationId}&routeId={routeId}&limit={limit}
```

#### Parameters

- `stationId` (required): The ID of the station
- `routeId` (optional): Filter arrivals by route ID
- `limit` (optional): Maximum number of arrivals to return (default: 10, max: 15)

#### Response Type

```typescript
type ArrivalsResponse = Array<{
  route: string;
  tripId: string;
  stopId: string;
  stopName: string;
  arrivalTime: string; // ISO time string
  departureTime: string; // ISO time string
  isRealtimeUpdated: boolean;
  stopSequence: number;
}>
```

## Error Responses

All endpoints may return the following error responses:

- 400 Bad Request: Missing or invalid parameters
- 404 Not Found: Requested resource not found
- 500 Internal Server Error: Server-side error

Error responses will have a plain text body with an error message.

## Real-time Updates

The API automatically updates arrival times using real-time data from the MTA's GTFS-RT feeds. The `isRealtimeUpdated` field in arrival responses indicates whether the times are from real-time data (true) or scheduled data (false).

Real-time updates are processed for the following route groups:
- ACE
- BDFM
- G
- JZ
- NQRW
- L
- 1234567
- SIR

Updates occur every 2 minutes to ensure current arrival predictions.

## Notes

1. All times are in America/New_York timezone
2. Station and route IDs follow the MTA's GTFS specification
3. Location coordinates (stopLat, stopLon) are in WGS84 format
4. Route types follow the GTFS specification for transit types
5. Parent stations may contain multiple child stops (platforms/entrances) 
