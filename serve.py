#!/usr/bin/env python3
"""Canberra COP PoC server.

Serves the static page and relays two feeds a browser can't reach directly:
- /esa — ACT ESA incidents (upstream sends no CORS headers)
- /tomtom/{z}/{x}/{y}.png — TomTom traffic-flow tiles (keeps the API key
  out of the page source; key lives in the gitignored .env file)
- /firms — NASA FIRMS fire hotspots near Canberra as GeoJSON (CSV upstream,
  key also in .env)
- /rfs — NSW RFS major incidents (GeoJSON upstream, no CORS headers)
- /news — Canberra headlines (RiotACT + Canberra Times RSS merged to JSON)
- /power — electricity outages as GeoJSON: Evoenergy (ACT, scraped from the
  outagesViewModel JSON embedded in their outage-map page) merged with
  Essential Energy (NSW, public KML files behind their outage map)
- /transit — live vehicle positions as GeoJSON, decoded from GTFS-realtime
  protobuf with a minimal stdlib parser (no protobuf dependency). Light rail
  comes from the legacy no-auth feed; buses light up once MyWayPlus API
  credentials land in .env (TC_VP_URL + TC_AUTH_BASIC)
- /airq — ACT air quality stations (data.act.gov.au Socrata), latest hourly
  row per station as GeoJSON
- /closures — ACT road closures as GeoJSON from the TCCS ArcGIS layer,
  active now or starting within 24 h
Run:  python3 serve.py  →  http://localhost:8899
"""
import csv
import io
import json
import os
import re
import struct
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

ESA_FEED = "https://esa.act.gov.au/feeds/allincidents.json"
CACHE_SECONDS = 60  # ESA updates every 60 s; don't hammer them harder
TILE_CACHE_SECONDS = 120  # traffic tiles: save free-tier quota on map pans

_cache = {"time": 0.0, "body": b"[]"}
_tile_cache = {}


def load_env():
    env = {}
    try:
        with open(os.path.join(os.path.dirname(__file__), ".env")) as f:
            for line in f:
                if "=" in line and not line.lstrip().startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env


_env = load_env()
TOMTOM_KEY = _env.get("TOMTOM_API_KEY", "")
FIRMS_KEY = _env.get("FIRMS_MAP_KEY", "")

# west,south,east,north box around the ACT and surrounds
FIRMS_BBOX = "148.2,-36.2,150.0,-34.4"
FIRMS_SENSORS = ("VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT")
FIRMS_DAYS = 2
FIRMS_CACHE_SECONDS = 600  # satellites only pass a few times a day

_firms_cache = {"time": 0.0, "body": b""}


def firms_body():
    if time.time() - _firms_cache["time"] > FIRMS_CACHE_SECONDS:
        feats = []
        for sensor in FIRMS_SENSORS:
            url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
                   f"{FIRMS_KEY}/{sensor}/{FIRMS_BBOX}/{FIRMS_DAYS}")
            with urllib.request.urlopen(url, timeout=15) as r:
                text = r.read().decode()
            for row in csv.DictReader(io.StringIO(text)):
                feats.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates":
                                 [float(row["longitude"]), float(row["latitude"])]},
                    "properties": {
                        "date": row["acq_date"],
                        "time": row["acq_time"].zfill(4),
                        "satellite": row["satellite"],
                        "frp": float(row["frp"] or 0),
                        "daynight": row["daynight"],
                        "confidence": row["confidence"],
                    },
                })
        body = json.dumps({"type": "FeatureCollection",
                           "features": feats}).encode()
        _firms_cache.update(time=time.time(), body=body)
    return _firms_cache["body"]


def esa_body():
    if time.time() - _cache["time"] > CACHE_SECONDS:
        req = urllib.request.Request(ESA_FEED, headers={"User-Agent": "canberra-cop-poc"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        json.loads(body)  # refuse to cache junk
        _cache.update(time=time.time(), body=body)
    return _cache["body"]


NEWS_FEEDS = (
    ("RiotACT", "https://the-riotact.com/feed"),
    ("Canberra Times", "https://www.canberratimes.com.au/rss.xml"),
)
NEWS_CACHE_SECONDS = 600
_news_cache = {"time": 0.0, "body": b"[]"}


def news_body():
    if time.time() - _news_cache["time"] > NEWS_CACHE_SECONDS:
        items = []
        for source, url in NEWS_FEEDS:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "canberra-cop-poc"})
                with urllib.request.urlopen(req, timeout=10) as r:
                    root = ET.fromstring(r.read())
                for it in root.findall(".//item"):
                    try:
                        ts = parsedate_to_datetime(it.findtext("pubDate", "")).timestamp()
                    except Exception:
                        ts = 0
                    items.append({"source": source, "ts": ts,
                                  "title": (it.findtext("title") or "").strip(),
                                  "link": (it.findtext("link") or "").strip()})
            except Exception:
                pass  # one dead feed shouldn't kill the panel
        items.sort(key=lambda i: i["ts"], reverse=True)
        _news_cache.update(time=time.time(),
                           body=json.dumps(items[:12]).encode())
    return _news_cache["body"]


# --- address search (geocoding) ----------------------------------------------
# Nominatim (OpenStreetMap) is free and keyless but asks callers to identify
# themselves and keep volume low. Relaying here lets us set a real User-Agent
# (a browser can't) and bias results to the Canberra region. Results are
# cached per query and upstream calls throttled to Nominatim's 1 req/s limit.
NOMINATIM = "https://nominatim.openstreetmap.org/search"
GEOCODE_UA = "canberra-cop/1.0 (personal situational-awareness map)"
# lon,lat,lon,lat box around the ACT — biases but doesn't hard-limit results
GEOCODE_VIEWBOX = "148.6,-35.05,149.5,-35.65"
GEOCODE_CACHE_SECONDS = 3600
_geocode_cache = {}
_geocode_last = [0.0]


def geocode_body(q):
    key = q.lower().strip()
    hit = _geocode_cache.get(key)
    if hit and time.time() - hit[0] < GEOCODE_CACHE_SECONDS:
        return hit[1]
    wait = 1.0 - (time.time() - _geocode_last[0])  # be polite: ≤1 req/s
    if wait > 0:
        time.sleep(wait)
    qs = urllib.parse.urlencode({
        "q": q, "format": "jsonv2", "countrycodes": "au", "limit": "5",
        "viewbox": GEOCODE_VIEWBOX, "bounded": "0", "addressdetails": "0"})
    req = urllib.request.Request(f"{NOMINATIM}?{qs}",
                                 headers={"User-Agent": GEOCODE_UA})
    with urllib.request.urlopen(req, timeout=10) as r:
        rows = json.loads(r.read())
    _geocode_last[0] = time.time()
    out = [{"name": row.get("display_name"),
            "lat": float(row["lat"]), "lon": float(row["lon"])}
           for row in rows]
    body = json.dumps(out).encode()
    _geocode_cache[key] = (time.time(), body)
    return body


# --- transit (GTFS-realtime) -------------------------------------------------
# Light rail: legacy pre-MyWay+ feed, still live and needs no key.
# Buses: MyWayPlus GTFS-R needs basic-auth credentials from the Transport
# Canberra developer portal (manual approval). Once granted, put the vehicle-
# positions URL and base64(client_id:client_secret) in .env as TC_VP_URL and
# TC_AUTH_BASIC and they merge into the same /transit response.
LIGHTRAIL_PB = "https://files.transport.act.gov.au/feeds/lightrail.pb"
TC_VP_URL = _env.get("TC_VP_URL", "")
TC_AUTH_BASIC = _env.get("TC_AUTH_BASIC", "")
TRANSIT_CACHE_SECONDS = 15

_transit_cache = {"time": 0.0, "body": b""}

OCCUPANCY = ("empty", "many seats free", "few seats free", "standing room",
             "crushed", "full", "not accepting passengers")


def _varint(buf, i):
    v = s = 0
    while True:
        b = buf[i]; i += 1
        v |= (b & 0x7F) << s
        if not b & 0x80:
            return v, i
        s += 7


def _pb_fields(buf):
    """Iterate (field_no, wire_type, value) over one protobuf message. Just
    enough of the wire format to read a GTFS-RT FeedMessage."""
    i, n = 0, len(buf)
    while i < n:
        tag, i = _varint(buf, i)
        fno, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = _varint(buf, i)
        elif wt == 1:
            v, i = buf[i:i + 8], i + 8
        elif wt == 2:
            ln, i = _varint(buf, i)
            v, i = buf[i:i + ln], i + ln
        elif wt == 5:
            v, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"wire type {wt}")
        yield fno, wt, v


def _gtfsrt_vehicles(pb, mode):
    """GTFS-RT FeedMessage bytes → vehicle-position features. Field numbers
    are from the gtfs-realtime.proto spec (entity=2, vehicle=4, …)."""
    feats = []
    for fno, _, entity in _pb_fields(pb):
        if fno != 2:  # FeedEntity
            continue
        vp = next((v for f, _, v in _pb_fields(entity) if f == 4), None)
        if vp is None:  # entity is a trip update or alert, not a vehicle
            continue
        lat = lon = None
        props = {"mode": mode}
        for f, wt, v in _pb_fields(vp):
            if f == 1:  # TripDescriptor
                for f2, _, v2 in _pb_fields(v):
                    if f2 == 5:
                        props["route"] = v2.decode(errors="replace")
            elif f == 2:  # Position (floats)
                for f2, w2, v2 in _pb_fields(v):
                    if w2 != 5:
                        continue
                    x = struct.unpack("<f", v2)[0]
                    if f2 == 1: lat = x
                    elif f2 == 2: lon = x
                    elif f2 == 3: props["bearing"] = round(x)
                    elif f2 == 5: props["speed"] = round(x * 3.6)  # m/s→km/h
            elif f == 5:
                props["ts"] = v
            elif f == 8:  # VehicleDescriptor
                for f2, _, v2 in _pb_fields(v):
                    if f2 == 2:
                        props["label"] = v2.decode(errors="replace")
            elif f == 9:
                props["occupancy"] = (OCCUPANCY[v] if v < len(OCCUPANCY)
                                      else f"code {v}")
        if lat is not None and lon is not None:
            feats.append({"type": "Feature",
                          "geometry": {"type": "Point",
                                       "coordinates": [round(lon, 6),
                                                       round(lat, 6)]},
                          "properties": props})
    return feats


def transit_body():
    if time.time() - _transit_cache["time"] > TRANSIT_CACHE_SECONDS:
        feats = []
        try:
            feats.extend(_gtfsrt_vehicles(_http_get(LIGHTRAIL_PB), "lightrail"))
        except Exception:
            pass
        if TC_VP_URL and TC_AUTH_BASIC:
            try:
                req = urllib.request.Request(TC_VP_URL, headers={
                    "User-Agent": BROWSER_UA,
                    "Authorization": "Basic " + TC_AUTH_BASIC})
                with urllib.request.urlopen(req, timeout=15) as r:
                    feats.extend(_gtfsrt_vehicles(r.read(), "bus"))
            except Exception:
                pass
        _transit_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _transit_cache["body"]


# --- power outages ---------------------------------------------------------
EVO_PAGE = "https://www.evoenergy.com.au/Outages"
EE_KML_CURRENT = "https://www.essentialenergy.com.au/Assets/kmz/current.kml"
EE_KML_FUTURE = "https://www.essentialenergy.com.au/Assets/kmz/future.kml"
# lon/lat box around the COP area — same box the client uses for RFS pins
POWER_BBOX = (148.2, -36.5, 150.5, -34.2)
# both utility sites reject the default urllib UA at the edge
BROWSER_UA = ("Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
              "Gecko/20100101 Firefox/128.0")
POWER_CACHE_SECONDS = 120
EE_FUTURE_CACHE_SECONDS = 1800  # ~3 MB, 900+ statewide records, slow-moving
SCHEDULED_HORIZON_DAYS = 7  # utilities plan weeks out; only show the next week

_power_cache = {"time": 0.0, "body": b""}
_ee_future_cache = {"time": 0.0, "feats": None}


def _http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _beyond_horizon(day, month, year):
    from datetime import date, timedelta
    try:
        start = date(int(year), int(month), int(day))
    except ValueError:
        return False
    return start > date.today() + timedelta(days=SCHEDULED_HORIZON_DAYS)


def _fmt_evo(s):
    # "2026-07-11T15:00:00" → "11/07 15:00"
    m = re.match(r"(\d{4})-(\d\d)-(\d\d)T(\d\d:\d\d)", s or "")
    return f"{m.group(3)}/{m.group(2)} {m.group(4)}" if m else "?"


def _fmt_ee(s):
    # "11/07/2026 15:00:00" → "11/07 15:00"
    m = re.match(r"(\d\d)/(\d\d)/\d{4} (\d\d:\d\d)", s or "")
    return f"{m.group(1)}/{m.group(2)} {m.group(3)}" if m else "?"


def _outage_features(props, centroid, ring):
    """One pin feature at the centroid + one polygon feature if we have a
    ring; both carry the same properties so popups work either way."""
    feats = [{"type": "Feature",
              "geometry": {"type": "Point", "coordinates": centroid},
              "properties": props}]
    if ring and len(ring) >= 3:
        if ring[0] != ring[-1]:
            ring = ring + [ring[0]]
        feats.append({"type": "Feature",
                      "geometry": {"type": "Polygon", "coordinates": [ring]},
                      "properties": props})
    return feats


def _evo_features():
    """The Evoenergy outage map is server-rendered: the page HTML embeds the
    full outage list (with polygons + centroids) as `outagesViewModel = [...]`.
    No separate JSON endpoint exists, so scrape that assignment."""
    html = _http_get(EVO_PAGE).decode("utf-8", "replace")
    m = re.search(r"outagesViewModel\s*=\s*(\[.*?\]);", html, re.S)
    if not m:
        return []
    feats = []
    for o in json.loads(m.group(1)):
        status = (o.get("Status") or "").lower()
        otype = (o.get("Type") or "").lower()
        if status not in ("active", "scheduled"):
            continue  # cancelled / completed / restored = noise
        centroid = json.loads(o.get("PolygonCentroidCoordinate") or "null")
        if not centroid:
            continue
        sev = ("unplanned" if otype == "unplanned" else
               "planned-active" if status == "active" else "scheduled")
        sched = o.get("ScheduledStartDateTime") or ""
        if sev == "scheduled" and len(sched) >= 10 and _beyond_horizon(
                sched[8:10], sched[5:7], sched[0:4]):
            continue
        props = {
            "src": "Evoenergy", "id": o.get("OutageID", "?"),
            "otype": otype, "sev": sev,
            "customers": o.get("AffectedCustomersCount") or 0,
            "where": (o.get("AffectedSuburbs") or "").title(),
            "reason": o.get("Description") or "",
            "start": _fmt_evo(o.get("ActualStartDateTime")
                              or o.get("ScheduledStartDateTime")),
            "eta": _fmt_evo(o.get("ExpectedRestorationDateTime")
                            or o.get("ScheduledEndDateTime")),
        }
        ring = [[p["lng"], p["lat"]]
                for p in json.loads(o.get("PolygonCoordinates") or "[]")]
        feats.extend(_outage_features(
            props, [centroid["lng"], centroid["lat"]], ring))
    return feats


def _ee_parse(kml_bytes, sev_default):
    """Essential Energy KML → outage features inside POWER_BBOX. Placemarks
    carry the details as an HTML blob in <description>; planned/unplanned is
    only encoded in the styleUrl name."""
    ns = "{http://earth.google.com/kml/2.1}"
    w, s, e, n = POWER_BBOX
    feats = []
    for pm in ET.fromstring(kml_bytes).iter(ns + "Placemark"):
        pt = pm.find(f".//{ns}Point/{ns}coordinates")
        ring_el = pm.find(f".//{ns}Polygon//{ns}coordinates")
        ring = []
        if ring_el is not None and ring_el.text:
            ring = [[float(x) for x in pair.split(",")[:2]]
                    for pair in ring_el.text.split()]
        if pt is not None and pt.text:
            lon, lat = [float(x) for x in pt.text.strip().split(",")[:2]]
        elif ring:
            lon = sum(p[0] for p in ring) / len(ring)
            lat = sum(p[1] for p in ring) / len(ring)
        else:
            continue
        if not (w < lon < e and s < lat < n):
            continue
        desc = pm.findtext(f"{ns}description", "")
        kv = {k.strip().rstrip(":").lower(): v.strip() for k, v in
              re.findall(r"<span>([^<]+)</span>([^<]*)", desc)}
        oid = pm.get("id") or (re.search(r"<h2>([^<]+)</h2>", desc) or
                               [None, "?"])[1]
        style = pm.findtext(f"{ns}styleUrl", "")
        otype = "unplanned" if "unplanned" in style else "planned"
        sev = "unplanned" if otype == "unplanned" else sev_default
        m = re.match(r"(\d\d)/(\d\d)/(\d{4})", kv.get("time off", ""))
        if sev == "scheduled" and m and _beyond_horizon(*m.groups()):
            continue
        feats.extend(_outage_features({
            "src": "Essential Energy", "id": oid,
            "otype": otype, "sev": sev,
            "customers": int(kv.get("no. of customers affected") or 0),
            "where": "",
            "reason": kv.get("reason", ""),
            "start": _fmt_ee(kv.get("time off")),
            "eta": _fmt_ee(kv.get("est. time on")),
        }, [lon, lat], ring))
    return feats


def power_body():
    if time.time() - _power_cache["time"] > POWER_CACHE_SECONDS:
        feats = []
        for fetch in (_evo_features, _ee_current_features, _ee_future_features):
            try:
                feats.extend(fetch())
            except Exception:
                pass  # one utility down shouldn't blank the other
        _power_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _power_cache["body"]


def _ee_current_features():
    # their own map JS cache-busts with a random query string; do the same
    return _ee_parse(_http_get(f"{EE_KML_CURRENT}?{int(time.time())}"),
                     "planned-active")


def _ee_future_features():
    if (_ee_future_cache["feats"] is None or
            time.time() - _ee_future_cache["time"] > EE_FUTURE_CACHE_SECONDS):
        feats = _ee_parse(_http_get(f"{EE_KML_FUTURE}?{int(time.time())}"),
                          "scheduled")
        _ee_future_cache.update(time=time.time(), feats=feats)
    return _ee_future_cache["feats"]


# --- BOM warnings ------------------------------------------------------------
# BOM's warning products carry no geometry, so this is a sidebar panel, not a
# map layer. The location endpoint returns everything relevant to Canberra,
# including ACT-forecast-district warnings (fire weather, severe weather,
# total fire bans, sheep graziers, flood…). r3dp5h = Canberra geohash.
BOM_GEOHASH = "r3dp5h"
BOM_WARN_URL = f"https://api.weather.bom.gov.au/v1/locations/{BOM_GEOHASH}/warnings"
BOM_CACHE_SECONDS = 300
_bom_cache = {"time": 0.0, "body": b"[]"}


_bom_detail_cache = {}


def bom_detail_body(wid):
    # warning ids are like NSW_PW017_IDN29000 — restrict chars so this can't
    # be coerced into requesting an arbitrary upstream path
    if not re.fullmatch(r"[A-Za-z0-9_]+", wid):
        raise ValueError("bad warning id")
    hit = _bom_detail_cache.get(wid)
    if hit and time.time() - hit[0] < BOM_CACHE_SECONDS:
        return hit[1]
    req = urllib.request.Request(
        f"https://api.weather.bom.gov.au/v1/warnings/{wid}",
        headers={"User-Agent": BROWSER_UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read()).get("data", {})
    body = json.dumps({"message": d.get("message", ""),
                       "title": d.get("title")}).encode()
    _bom_detail_cache[wid] = (time.time(), body)
    return body


def bom_body():
    if time.time() - _bom_cache["time"] > BOM_CACHE_SECONDS:
        req = urllib.request.Request(BOM_WARN_URL, headers={
            "User-Agent": BROWSER_UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read()).get("data", [])
        items = [{
            "id": w.get("id"),
            "title": w.get("title") or w.get("short_title"),
            "short": w.get("short_title"),
            "severity": w.get("warning_group_type"),  # minor/moderate/major
            "issued": w.get("issue_time"),
            "expires": w.get("expiry_time"),
        } for w in data]
        _bom_cache.update(time=time.time(), body=json.dumps(items).encode())
    return _bom_cache["body"]


RFS_FEED = "https://www.rfs.nsw.gov.au/feeds/majorIncidents.json"
_rfs_cache = {"time": 0.0, "body": b""}


def rfs_body():
    # RFS asks consumers to poll no more often than every 60 s
    if time.time() - _rfs_cache["time"] > 60:
        req = urllib.request.Request(RFS_FEED, headers={"User-Agent": "canberra-cop-poc"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        json.loads(body)  # refuse to cache junk
        _rfs_cache.update(time=time.time(), body=body)
    return _rfs_cache["body"]


# --- ACT air quality ---------------------------------------------------------
# data.act.gov.au Socrata dataset 94a5-zqnn: hourly rows per station
# (Civic / Florey / Monash). Anonymous SODA reads work; newest-first order
# lets us keep just the latest row per station. aqi_site is the station's
# overall AQI (Australian bands: 0-33 very good … 200+ hazardous).
AIRQ_URL = ("https://www.data.act.gov.au/resource/94a5-zqnn.json"
            "?%24order=datetime%20DESC&%24limit=9")
AIRQ_CACHE_SECONDS = 900  # readings are hourly; no point hammering Socrata
_airq_cache = {"time": 0.0, "body": b""}


def airq_body():
    if time.time() - _airq_cache["time"] > AIRQ_CACHE_SECONDS:
        rows = json.loads(_http_get(AIRQ_URL))
        feats, seen = [], set()

        def num(row, k):
            # gas readings are hundredths of a ppm — keep 3 decimals
            try:
                return round(float(row[k]), 3)
            except (KeyError, TypeError, ValueError):
                return None

        for row in rows:  # newest first — first hit per station wins
            name = row.get("name")
            gps = row.get("gps") or {}
            if not name or name in seen or "latitude" not in gps:
                continue
            seen.add(name)
            props = {"station": name, "updated": row.get("datetime", "")}
            for out, col in (("aqi", "aqi_site"), ("pm25", "pm2_5"),
                             ("pm10", "pm10"), ("o3", "o3_1hr"),
                             ("co", "co"), ("no2", "no2")):
                v = num(row, col)
                if v is not None:
                    props[out] = v
            feats.append({"type": "Feature",
                          "geometry": {"type": "Point", "coordinates":
                                       [float(gps["longitude"]),
                                        float(gps["latitude"])]},
                          "properties": props})
        _airq_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _airq_cache["body"]


# --- ACT road closures -------------------------------------------------------
# The data.act.gov.au "Unplanned Road Closures" dataset is only an href card;
# the data lives in a TCCS ArcGIS layer. The similarly-named live layer is a
# graveyard of 2005-2017 "until further notice" rows — the actively-maintained
# one is Road_Closures_public_view_HISTORICAL_ACTUAL (edited daily). Dates are
# epoch-ms UTC; the where clause needs TIMESTAMP literals (a bare epoch number
# comparison silently matches nothing).
CLOSURES_URL = ("https://services1.arcgis.com/E5n4f1VY84i0xSjy/arcgis/rest/"
                "services/Road_Closures_public_view_HISTORICAL_ACTUAL/"
                "FeatureServer/0/query")
CLOSURES_CACHE_SECONDS = 600
CLOSURES_LOOKAHEAD_HOURS = 24  # show what's about to close, not just what is
_closures_cache = {"time": 0.0, "body": b""}


def _strip_html(s):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or "")).strip()


def closures_body():
    if time.time() - _closures_cache["time"] > CLOSURES_CACHE_SECONDS:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        ts = lambda d: d.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")
        qs = urllib.parse.urlencode({
            "where": (f"startTimeClosure <= "
                      f"{ts(now + timedelta(hours=CLOSURES_LOOKAHEAD_HOURS))}"
                      f" AND endTimeClosure >= {ts(now)}"),
            "outFields": "globalid,projectTitle,type,roadsClosed,"
                         "reasonRoadClosure,suburb1,startTimeClosure,"
                         "endTimeClosure",
            "f": "json"})
        data = json.loads(_http_get(f"{CLOSURES_URL}?{qs}"))
        if "error" in data:
            raise ValueError(data["error"])
        now_ms = now.timestamp() * 1000
        feats = []
        for f in data.get("features", []):
            a, g = f["attributes"], f.get("geometry")
            if not g:
                continue
            feats.append({
                "type": "Feature",
                "geometry": {"type": "Point",
                             "coordinates": [g["x"], g["y"]]},
                "properties": {
                    "id": a.get("globalid") or "?",
                    "title": _strip_html(a.get("projectTitle"))[:120],
                    "ctype": a.get("type") or "other",
                    "suburb": (a.get("suburb1") or "").replace("_", " ").title(),
                    "roads": _strip_html(a.get("roadsClosed"))[:400],
                    "reason": _strip_html(a.get("reasonRoadClosure"))[:200],
                    "start": a.get("startTimeClosure"),
                    "end": a.get("endTimeClosure"),
                    "active": bool(a.get("startTimeClosure") and
                                   a["startTimeClosure"] <= now_ms),
                }})
        _closures_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _closures_cache["body"]


# --- SITREP: bundle the cached feed state and have a local LLM write the
# situation summary. Ollama runs on the desktop; its firewall admits only
# ace2, which is exactly where this server lives in production.
OLLAMA_URL = _env.get("OLLAMA_URL", "http://192.168.0.234:11434")
OLLAMA_MODEL = _env.get("OLLAMA_MODEL", "qwen3.5:9b")
SITREP_CACHE_SECONDS = 300
_sitrep_cache = {"time": 0.0, "body": b""}
_wx_cache = {"time": 0.0, "data": {}}


def _wx_current():
    if time.time() - _wx_cache["time"] > 600:
        url = ("https://api.open-meteo.com/v1/forecast?latitude=-35.28"
               "&longitude=149.13&current=temperature_2m,weather_code,"
               "wind_speed_10m,wind_gusts_10m&timezone=Australia%2FSydney")
        with urllib.request.urlopen(url, timeout=10) as r:
            _wx_cache.update(time=time.time(),
                             data=json.loads(r.read()).get("current", {}))
    return _wx_cache["data"]


def _kv_from_desc(desc):
    return {m[0].strip().lower(): m[1].strip()
            for m in re.findall(r"([A-Za-z ]+):\s*(.*?)<br", desc + "<br")}


def _sitrep_context():
    # each source guarded: one dead feed shouldn't kill the summary
    lines = []
    try:
        wx = _wx_current()
        lines.append(f"Weather: {wx.get('temperature_2m')}°C, wind "
                     f"{wx.get('wind_speed_10m')} km/h gusting "
                     f"{wx.get('wind_gusts_10m')} km/h")
    except Exception:
        pass
    try:
        act = [i for i in json.loads(esa_body()) if i.get("state") == "ACT"]
        descs = []
        for i in act:
            kv = _kv_from_desc(i.get("description", ""))
            descs.append(f"{i['title']} ({kv.get('suburb', '?')}, "
                         f"status: {kv.get('status', '?')})")
        lines.append(f"ACT ESA incidents ({len(descs)}): " +
                     ("; ".join(descs[:25]) if descs else "none"))
    except Exception:
        lines.append("ACT ESA incidents: feed unavailable")
    try:
        rfs = json.loads(rfs_body())["features"]
        near = []
        for f in rfs:
            g = f["geometry"]
            if g["type"] == "GeometryCollection":
                g = next((x for x in g["geometries"] if x["type"] == "Point"), None)
            if not g or g["type"] != "Point":
                continue
            lon, lat = g["coordinates"][:2]
            if 148.2 < lon < 150.5 and -36.5 < lat < -34.2:
                near.append(f"{f['properties']['title']} "
                            f"[{f['properties']['category']}]")
        lines.append("NSW RFS alerts nearby: " +
                     ("; ".join(near) if near else "none"))
    except Exception:
        lines.append("NSW RFS alerts: feed unavailable")
    try:
        pins = [f for f in json.loads(power_body())["features"]
                if f["geometry"]["type"] == "Point"
                and f["properties"].get("sev") != "scheduled"]
        descs = [f"{p['properties'].get('otype', 'power')} outage, "
                 f"{p['properties'].get('customers', '?')} customers "
                 f"({str(p['properties'].get('where', ''))[:50]})" for p in pins]
        lines.append(f"Live power outages ({len(descs)}): " +
                     ("; ".join(descs[:10]) if descs else "none"))
    except Exception:
        lines.append("Power outages: feed unavailable")
    try:
        warns = json.loads(bom_body())
        lines.append("BOM warnings: " + ("; ".join(
            f"{w['title']} [{w.get('severity', '?')}]" for w in warns)
            if warns else "none"))
    except Exception:
        lines.append("BOM warnings: feed unavailable")
    try:
        stations = json.loads(airq_body())["features"]
        worst = max(stations, key=lambda f: f["properties"].get("aqi", 0),
                    default=None)
        if worst:
            p = worst["properties"]
            lines.append(f"Air quality: worst station {p['station']} "
                         f"AQI {p.get('aqi', '?')}")
    except Exception:
        pass
    try:
        act = [f["properties"] for f in json.loads(closures_body())["features"]
               if f["properties"]["active"]]
        # routine roadworks would drown the summary — detail emergencies only
        urgent = [c for c in act
                  if c["ctype"] in ("emergency", "inclementWeather")]
        line = (f"Road closures: {len(act)} active "
                f"(mostly routine roadworks/construction)")
        if urgent:
            line += "; EMERGENCY closures: " + "; ".join(
                f"{c['suburb'] or '?'}: {c['roads'][:60]}" for c in urgent[:5])
        lines.append(line)
    except Exception:
        pass
    return "\n".join(lines)


def sitrep_body():
    if time.time() - _sitrep_cache["time"] > SITREP_CACHE_SECONDS:
        prompt = (
            "You are the duty officer on a Canberra situational-awareness "
            "watch floor. Write a terse SITREP of 3-5 sentences from the "
            "data below. Lead with anything urgent. Planned hazard-reduction "
            "burns are routine — do not present them as emergencies. Mention "
            "live power outages with customer counts, weather warnings, and "
            "notable weather. Plain prose, no preamble, no headings, no "
            "markdown.\n\n" + _sitrep_context())
        payload = json.dumps({
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            # qwen3.5 is a hybrid-reasoning model: without think=false it
            # burns the whole budget on chain-of-thought and never answers
            "think": False,
            "stream": False,
            "options": {"num_predict": 400, "temperature": 0.3},
        }).encode()
        req = urllib.request.Request(
            OLLAMA_URL + "/api/chat", data=payload,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=180) as r:
            data = json.loads(r.read())
        text = (data.get("message", {}).get("content") or "").strip()
        if not text:
            raise ValueError("empty completion")
        body = json.dumps({"text": text, "generated": int(time.time()),
                           "model": OLLAMA_MODEL}).encode()
        _sitrep_cache.update(time=time.time(), body=body)
    return _sitrep_cache["body"]


def tomtom_tile(z, x, y):
    key = f"{z}/{x}/{y}"
    hit = _tile_cache.get(key)
    if hit and time.time() - hit[0] < TILE_CACHE_SECONDS:
        return hit[1]
    url = (f"https://api.tomtom.com/traffic/map/4/tile/flow/relative0/"
           f"{z}/{x}/{y}.png?key={TOMTOM_KEY}")
    with urllib.request.urlopen(url, timeout=10) as r:
        body = r.read()
    _tile_cache[key] = (time.time(), body)
    return body


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.send_response(302)
            self.send_header("Location", "/poc.html")
            self.end_headers()
            return
        m = re.fullmatch(r"/tomtom/(\d+)/(\d+)/(\d+)\.png", self.path)
        if m:
            if not TOMTOM_KEY:
                self.send_error(503, "no TOMTOM_API_KEY in .env")
                return
            try:
                body = tomtom_tile(*m.groups())
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "tomtom unreachable")
            return
        if self.path.rstrip("/") == "/sitrep":
            try:
                body = sitrep_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "sitrep generation failed")
            return
        if self.path.rstrip("/") == "/news":
            try:
                body = news_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "news feeds unreachable")
            return
        if self.path.rstrip("/") == "/rfs":
            try:
                body = rfs_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "rfs unreachable")
            return
        if self.path.startswith("/geocode"):
            q = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query).get("q", [""])[0]
            if not q.strip():
                self.send_error(400, "missing q")
                return
            try:
                body = geocode_body(q)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "geocoder unreachable")
            return
        if self.path.startswith("/bomdetail"):
            wid = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query).get("id", [""])[0]
            try:
                body = bom_detail_body(wid)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except ValueError:
                self.send_error(400, "bad id")
            except Exception:
                self.send_error(502, "BOM detail unreachable")
            return
        if self.path.rstrip("/") == "/bom":
            try:
                body = bom_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "BOM warnings unreachable")
            return
        if self.path.rstrip("/") == "/airq":
            try:
                body = airq_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "air quality feed unreachable")
            return
        if self.path.rstrip("/") == "/closures":
            try:
                body = closures_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "closures feed unreachable")
            return
        if self.path.rstrip("/") == "/transit":
            try:
                body = transit_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "transit feed unreachable")
            return
        if self.path.rstrip("/") == "/power":
            try:
                body = power_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "outage feeds unreachable")
            return
        if self.path.rstrip("/") == "/firms":
            if not FIRMS_KEY:
                self.send_error(503, "no FIRMS_MAP_KEY in .env")
                return
            try:
                body = firms_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "firms unreachable")
            return
        if self.path.rstrip("/") == "/esa":
            try:
                body = esa_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
            except Exception:
                body = b'{"error": "esa feed unreachable"}'
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

    def log_message(self, *args):
        pass  # keep the terminal quiet


if __name__ == "__main__":
    print("Canberra COP PoC → http://localhost:8899/poc.html  (Ctrl-C to stop)")
    # Threading: one slow upstream fetch must not stall every other request
    ThreadingHTTPServer(("0.0.0.0", 8899), Handler).serve_forever()
