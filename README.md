# Argus

An all-source common operating picture for Canberra, Australia — live aircraft,
weather, rain radar, and emergency-service incidents on one map, with a history
recorder so you can scrub back through time. Think Palantir, but cheap: every
data source is free.

Named for [Argus Panoptes](https://en.wikipedia.org/wiki/Argus_Panoptes), the
hundred-eyed watchman of Greek myth.

![version](https://img.shields.io/badge/version-0.01-blue)
![status](https://img.shields.io/badge/status-beta-orange)

## What it shows

- **Aircraft** within 60 NM of Canberra via ADS-B ([airplanes.live](https://airplanes.live)),
  coloured by altitude, military airframes flagged red. 12 s refresh.
- **Current weather** for Canberra ([Open-Meteo](https://open-meteo.com)).
- **Rain radar** overlay ([RainViewer](https://www.rainviewer.com)).
- **Live emergency incidents** — fires, ambulance callouts, rescues — from the
  [ACT Emergency Services Agency](https://esa.act.gov.au) dispatch feed
  (CC-BY 4.0, updated every 60 s).
- **Traffic flow** ([TomTom](https://developer.tomtom.com), optional) —
  green/amber/red road congestion. Needs a free API key in `.env`
  (`TOMTOM_API_KEY=...`); the layer is off by default and simply stays
  unavailable without one.
- **Fire hotspots** ([NASA FIRMS](https://firms.modaps.eosdis.nasa.gov),
  optional) — VIIRS satellite thermal detections from the last 48 h, sized
  by fire radiative power. Needs a free map key in `.env`
  (`FIRMS_MAP_KEY=...`).
- **NSW fires** — [NSW RFS](https://www.rfs.nsw.gov.au) major incidents
  around the ACT border region, coloured by official alert level (the
  "Fires Near Me" feed).
- **Power outages** — [Evoenergy](https://www.evoenergy.com.au/Outages) (ACT)
  and [Essential Energy](https://www.essentialenergy.com.au) (surrounding NSW)
  current outages plus works scheduled in the next 7 days: outage-footprint
  polygons and pins coloured by severity (unplanned / active planned /
  scheduled).
- **Live transit** — Canberra light rail vehicle positions (15 s refresh),
  decoded from Transport Canberra's GTFS-realtime feed with a minimal
  built-in protobuf parser — no dependencies. Live buses hook in once
  MyWayPlus API credentials are added to `.env` (`TC_VP_URL` +
  `TC_AUTH_BASIC`); keys come from the Transport Canberra developer portal.
- **BOM warnings** — current [Bureau of Meteorology](https://www.bom.gov.au)
  warnings for the Canberra area (fire weather, severe weather, total fire
  bans, flood, sheep graziers…), shown as a colour-coded sidebar banner
  ranked by severity. No key needed.
- **Local news** — RiotACT and Canberra Times headlines, merged and
  time-sorted in the sidebar.
- **Address search** — type an address to geocode it (via
  [Nominatim](https://nominatim.openstreetmap.org), biased to the Canberra
  region) and fly the map to it with a marker.
- **Basemaps** — dark (CARTO), street (OSM), or satellite (Esri imagery).

## Running it

```sh
python3 serve.py
```

Then open <http://localhost:8899/poc.html>. No dependencies beyond Python 3
and a browser. A `docker-compose.yml` is included for running it on a
homelab box (put API keys in `.env` next to the app files).

(`poc.html` also works opened directly as a file, but the ESA incident layer
needs `serve.py` — the upstream feed sends no CORS headers, so the little
server relays it.)

## Roadmap

- **Phase 2** — proper backend (FastAPI container), live buses via the
  MyWayPlus GTFS-realtime API (needs an approved developer key).
- **Phase 3** — alert rules (loitering aircraft, incidents near a watchpoint,
  storm cells inbound), per-decision saved views. *(Time slider done in v0.01:
  the incident layers are recorded to a local store and can be replayed.)*
- **Someday** — a local RTL-SDR receiver as a first-party ADS-B sensor, and a
  Meshtastic mesh layer for own-network tracking.

## Data attribution

Aircraft data from airplanes.live community receivers. Weather by Open-Meteo
(CC-BY 4.0). Radar tiles by RainViewer. Incident data © ACT Emergency Services
Agency (CC-BY 4.0) and © State of New South Wales (NSW Rural Fire Service).
Outage data © Evoenergy and © Essential Energy, relayed from their public
outage maps. Transit vehicle positions sourced from Transport Canberra.
Weather warnings © Bureau of Meteorology. Geocoding © OpenStreetMap
contributors via Nominatim. Base map © OpenStreetMap contributors, © CARTO.
Air quality data © Australian Capital Territory (CC-BY 4.0, via
data.act.gov.au). Road closure data © Transport Canberra and City Services —
Roads ACT (CC-BY-SA 4.0). Earthquake data © Commonwealth of Australia
(Geoscience Australia, CC-BY 4.0).

Incident data can affect life and property decisions — treat this as a hobby
visualisation, not an emergency information service. Use official sources
(esa.act.gov.au, BOM) for anything that matters.
