# canberra-cop

A common operating picture for Canberra, Australia — live aircraft, weather,
rain radar, and emergency-service incidents on one map. Think Palantir, but
cheap: every data source is free.

![status](https://img.shields.io/badge/status-proof--of--concept-orange)

## What it shows

- **Aircraft** within 60 NM of Canberra via ADS-B ([airplanes.live](https://airplanes.live)),
  coloured by altitude, military airframes flagged red. 12 s refresh.
- **Current weather** for Canberra ([Open-Meteo](https://open-meteo.com)).
- **Rain radar** overlay ([RainViewer](https://www.rainviewer.com)).
- **Live emergency incidents** — fires, ambulance callouts, rescues — from the
  [ACT Emergency Services Agency](https://esa.act.gov.au) dispatch feed
  (CC-BY 4.0, updated every 60 s). 

## Running it

```sh
python3 serve.py
```

Then open <http://localhost:8899/poc.html>. No dependencies beyond Python 3
and a browser.

(`poc.html` also works opened directly as a file, but the ESA incident layer
needs `serve.py` — the upstream feed sends no CORS headers, so the little
server relays it.)

## Roadmap

- **Phase 2** — proper backend (FastAPI container), TomTom traffic flow,
  Transport Canberra GTFS-realtime (live buses/light rail), NASA FIRMS fire
  hotspots.
- **Phase 3** — alert rules (loitering aircraft, incidents near a watchpoint,
  storm cells inbound), per-decision saved views, time slider.
- **Someday** — a local RTL-SDR receiver as a first-party ADS-B sensor.

## Data attribution

Aircraft data from airplanes.live community receivers. Weather by Open-Meteo
(CC-BY 4.0). Radar tiles by RainViewer. Incident data © ACT Emergency Services
Agency (CC-BY 4.0). Base map © OpenStreetMap contributors, © CARTO.

Incident data can affect life and property decisions — treat this as a hobby
visualisation, not an emergency information service. Use official sources
(esa.act.gov.au, BOM) for anything that matters.
