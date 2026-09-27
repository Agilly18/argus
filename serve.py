#!/usr/bin/env python3
"""Argus — Canberra common operating picture server.

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
- /quakes — Geoscience Australia earthquakes (7-day window), slimmed to
  Australian events plus a box around SE Australia
- /aircraft — adsb.lol positions near Canberra (verbatim relay; one
  shared upstream stream + snapshotted for the time slider)
- /wind — Open-Meteo 5x5 wind grid over the ACT (verbatim relay)
- /weather — Open-Meteo current conditions for Canberra (verbatim relay)
Run:  python3 serve.py  →  http://localhost:8899
"""
import csv
import functools
import gzip
import hashlib
import io
import json
import os
import re
import sqlite3
import struct
import threading
import time
import urllib.error
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

# How long a cached body may keep being served after its upstream refresh
# fails. Past this the endpoint errors again, so a feed that is genuinely
# dead surfaces as dead instead of quietly serving history forever.
STALE_MAX_SECONDS = 3600


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


def _cfg(key, default=None):
    """Config value: real environment wins (so docker-compose `environment:`
    works), then the .env file, then the default."""
    return os.environ.get(key) or _env.get(key, default)


TOMTOM_KEY = _env.get("TOMTOM_API_KEY", "")
FIRMS_KEY = _env.get("FIRMS_MAP_KEY", "")

# --- cost + abuse guards -----------------------------------------------------
# Two things behind this server cost something per request: TomTom traffic
# tiles (metered key, daily free-tier allowance) and the Ollama SITREP (GPU
# time on the desktop). Anyone who can reach Argus can spend both, so both
# get a budget and a per-IP rate limit. These are process-local in-memory
# guards that reset on restart -- they are NOT billing caps. Set a
# provider-side budget for that.
TOMTOM_DAILY_TILE_BUDGET = int(_cfg("TOMTOM_DAILY_TILE_BUDGET", "40000"))
TOMTOM_TILE_CACHE_MAX = int(_cfg("TOMTOM_TILE_CACHE_MAX", "4000"))
GEOCODE_CACHE_MAX = int(_cfg("GEOCODE_CACHE_MAX", "2000"))
RATELIMIT_TOMTOM_PER_MIN = int(_cfg("RATELIMIT_TOMTOM_PER_MIN", "240"))
RATELIMIT_SITREP_PER_MIN = int(_cfg("RATELIMIT_SITREP_PER_MIN", "6"))
RATELIMIT_GEOCODE_PER_MIN = int(_cfg("RATELIMIT_GEOCODE_PER_MIN", "20"))
# Trust a proxy-supplied client IP only from these peers. The Cloudflare
# tunnel connects from loopback, so without this every tunnelled request
# shares one bucket and the per-IP limit means nothing.
TRUSTED_PROXY_PEERS = set(
    p.strip() for p in _cfg("TRUSTED_PROXY_PEERS", "127.0.0.1,::1").split(",")
    if p.strip())

_guard_lock = threading.Lock()
_ratelimit = {}


def _prune_cache(cache, cap):
    """Drop oldest entries from a {key: (time, value)} cache past `cap`.
    These are keyed by caller-controlled input (tile coords, search text),
    so on a reachable instance they would otherwise grow without bound."""
    if len(cache) <= cap:
        return
    doomed = sorted(cache, key=lambda k: cache[k][0])[:len(cache) - cap]
    for key in doomed:
        cache.pop(key, None)


def rate_limited(bucket, client, per_min):
    """True once `client` has used its allowance for the current minute.
    Per-IP, per-process, in-memory: resets on restart, does nothing about a
    distributed source. It exists to stop one bored visitor draining a
    metered quota, not as a security control. 0 or less disables it."""
    if per_min <= 0:
        return False
    window = int(time.time() // 60)
    with _guard_lock:
        for key in [k for k in _ratelimit if k[2] < window - 1]:
            _ratelimit.pop(key, None)
        key = (bucket, client, window)
        count = _ratelimit.get(key, 0) + 1
        _ratelimit[key] = count
    return count > per_min


def stale_ok(cache, label):
    """Serve the last good body when an upstream refresh fails.

    Every feed below caches into a {"time", "body"} dict and re-fetches once
    its TTL passes; an upstream blip used to propagate out as a 502 and the
    layer would empty. Falling back to the cached body keeps the picture up
    while the feed recovers, bounded by STALE_MAX_SECONDS so a dead feed is
    still eventually visible as dead. The history recorder dedupes by hash,
    so a stale body is not written as a new snapshot."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                body = fn(*args, **kwargs)
                watch_ok(label)
                return body
            except Exception as exc:
                watch_fail(label, exc)
                age = time.time() - cache["time"]
                if cache["time"] and age < STALE_MAX_SECONDS:
                    print(f"[argus] {label} upstream failed ({exc}); "
                          f"serving {int(age)}s-old cache", flush=True)
                    return cache["body"]
                raise
        return wrapper
    return deco


# west,south,east,north box around the ACT and surrounds
FIRMS_BBOX = "148.2,-36.2,150.0,-34.4"
FIRMS_SENSORS = ("VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT")
FIRMS_DAYS = 2
FIRMS_CACHE_SECONDS = 600  # satellites only pass a few times a day

_firms_cache = {"time": 0.0, "body": b""}


@stale_ok(_firms_cache, "firms")
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


@stale_ok(_cache, "esa")
def esa_body():
    if time.time() - _cache["time"] > CACHE_SECONDS:
        req = urllib.request.Request(ESA_FEED, headers={"User-Agent": "argus-cop"})
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


@stale_ok(_news_cache, "news")
def news_body():
    if time.time() - _news_cache["time"] > NEWS_CACHE_SECONDS:
        items = []
        for source, url in NEWS_FEEDS:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "argus-cop"})
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
GEOCODE_UA = "argus/0.11 (personal situational-awareness map)"
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
    _prune_cache(_geocode_cache, GEOCODE_CACHE_MAX)
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


@stale_ok(_transit_cache, "transit")
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
        try:
            feats.extend(_evo_record(o))
        except Exception as exc:  # one malformed row must not blank the rest
            oid = o.get("OutageID", "?") if isinstance(o, dict) else "?"
            print(f"[argus] evoenergy: skipped outage {oid} ({exc})",
                  flush=True)
    return feats


def _evo_ring(o):
    """PolygonCoordinates → [[lng, lat], ...], or [] if absent/unparseable."""
    try:
        return [[p["lng"], p["lat"]]
                for p in json.loads(o.get("PolygonCoordinates") or "[]")]
    except (ValueError, KeyError, TypeError):
        return []


def _evo_record(o):
    """One Evoenergy outage → features. Evoenergy sometimes truncates
    PolygonCentroidCoordinate at 40 chars (seen Sep 2026), so fall back to
    the mean of the polygon ring rather than dropping the outage."""
    status = (o.get("Status") or "").lower()
    otype = (o.get("Type") or "").lower()
    if status not in ("active", "scheduled"):
        return []  # cancelled / completed / restored = noise
    ring = _evo_ring(o)
    try:
        c = json.loads(o.get("PolygonCentroidCoordinate") or "null")
        centroid = [c["lng"], c["lat"]] if c else None
    except (ValueError, KeyError, TypeError):
        centroid = None
    if not centroid and ring:
        centroid = [sum(p[0] for p in ring) / len(ring),
                    sum(p[1] for p in ring) / len(ring)]
    if not centroid:
        return []
    sev = ("unplanned" if otype == "unplanned" else
           "planned-active" if status == "active" else "scheduled")
    sched = o.get("ScheduledStartDateTime") or ""
    if sev == "scheduled" and len(sched) >= 10 and _beyond_horizon(
            sched[8:10], sched[5:7], sched[0:4]):
        return []
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
    return _outage_features(props, centroid, ring)


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


POWER_FETCH_NAMES = {"_evo_features": "power/Evoenergy feed",
                     "_ee_current_features": "power/Essential Energy current feed",
                     "_ee_future_features": "power/Essential Energy scheduled feed"}


@stale_ok(_power_cache, "power")
def power_body():
    if time.time() - _power_cache["time"] > POWER_CACHE_SECONDS:
        feats = []
        for fetch in (_evo_features, _ee_current_features, _ee_future_features):
            name = POWER_FETCH_NAMES.get(fetch.__name__, fetch.__name__)
            try:
                feats.extend(fetch())
                watch_ok(name)
            except Exception as exc:  # one utility down shouldn't blank the other
                watch_fail(name, exc)
                print(f"[argus] power: {fetch.__name__} failed ({exc})",
                      flush=True)
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
    _prune_cache(_bom_detail_cache, 500)
    return body


@stale_ok(_bom_cache, "bom")
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


@stale_ok(_rfs_cache, "rfs")
def rfs_body():
    # RFS asks consumers to poll no more often than every 60 s
    if time.time() - _rfs_cache["time"] > 60:
        req = urllib.request.Request(RFS_FEED, headers={"User-Agent": "argus-cop"})
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


@stale_ok(_airq_cache, "airq")
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


@stale_ok(_closures_cache, "closures")
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


# --- ACT suburb boundaries -----------------------------------------------------
# Static reference layer, same ArcGIS host as the closures feed. All 139
# polygons fit a single query (maxRecordCount 1000); maxAllowableOffset
# generalises geometry server-side so the payload stays lean. Suburbs don't
# move — cache for a day.
SUBURBS_URL = ("https://services1.arcgis.com/E5n4f1VY84i0xSjy/arcgis/rest/"
               "services/Suburbs_ACT/FeatureServer/0/query")
SUBURBS_CACHE_SECONDS = 86400
_suburbs_cache = {"time": 0.0, "body": b""}


@stale_ok(_suburbs_cache, "suburbs")
def suburbs_body():
    if time.time() - _suburbs_cache["time"] > SUBURBS_CACHE_SECONDS:
        qs = urllib.parse.urlencode({
            "where": "1=1", "outFields": "SUBURB",
            "maxAllowableOffset": "0.0002", "f": "geojson"})
        body = _http_get(f"{SUBURBS_URL}?{qs}")
        data = json.loads(body)
        if "error" in data:
            raise ValueError(data["error"])
        _suburbs_cache.update(time=time.time(), body=body)
    return _suburbs_cache["body"]


# --- earthquakes -------------------------------------------------------------
# Geoscience Australia's GeoServer WFS; the 7-day layer is global (~50
# events), so keep Australian-flagged quakes plus anything in a box around
# SE Australia (offshore Tasman events aren't flagged in-Australia). The raw
# blob is ~90 KB of solver metadata — slim it to what the popup needs.
QUAKES_URL = ("https://earthquakes.ga.gov.au/geoserver/earthquakes/ows"
              "?service=WFS&version=1.0.0&request=GetFeature"
              "&typeName=earthquakes:earthquakes_seven_days"
              "&outputFormat=application/json")
QUAKES_BBOX = (140.0, -44.0, 155.0, -28.0)  # lon/lat box, SE Aus + offshore
QUAKES_CACHE_SECONDS = 600
_quakes_cache = {"time": 0.0, "body": b""}


@stale_ok(_quakes_cache, "quakes")
def quakes_body():
    if time.time() - _quakes_cache["time"] > QUAKES_CACHE_SECONDS:
        data = json.loads(_http_get(QUAKES_URL))
        w, s, e, n = QUAKES_BBOX
        feats = []
        for f in data.get("features", []):
            lon, lat = f["geometry"]["coordinates"][:2]
            p = f["properties"]
            if p.get("located_in_australia") != "Y" and not (
                    w < lon < e and s < lat < n):
                continue
            mag = p.get("preferred_magnitude")
            feats.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "id": p.get("earthquake_id") or p.get("event_id"),
                    "mag": round(mag, 1) if mag is not None else None,
                    "place": p.get("description") or "?",
                    "time": p.get("epicentral_time"),
                    "depth": round(p["depth"]) if p.get("depth") is not None
                             else None,
                    "felt": p.get("felt_reports_count") or 0,
                    "url": p.get("felt_report_url") or "",
                }})
        feats.sort(key=lambda f: f["properties"]["time"] or "", reverse=True)
        _quakes_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _quakes_cache["body"]


# --- aircraft (adsb.lol relay) ------------------------------------------------
# The browser used to hit the upstream directly, but relaying gives one shared
# stream for all viewers AND lets the recorder snapshot positions for the time
# slider. Body is the verbatim upstream JSON ({"ac": [...]}) so the client
# parser is unchanged.
# Upstream was airplanes.live until Aug 2026, when they closed the public API:
# every endpoint now answers 403 "contact us at contact@airplanes.live" unless
# you feed them (/feed-status reports our IP as running no beast/mlat client).
# adsb.lol serves the same readsb v2 schema — hex/flight/lat/lon/alt_baro/gs/
# track/seen/t/category/dbFlags, everything poc.html reads — so the swap is the
# URL alone. Its lack of CORS headers (why we passed it over in v0.02) stopped
# mattering the day this became a server-side relay.
# Same terms as before: non-commercial, ~1 req/s. The permanent fix is still
# the parked RTL-SDR build — our own receiver, nobody's terms.
CBR_LAT, CBR_LON, CBR_RADIUS_NM = -35.28, 149.13, 60  # keep in sync w/ poc.html CBR
AIRCRAFT_URL = (f"https://api.adsb.lol/v2/point/"
                f"{CBR_LAT}/{CBR_LON}/{CBR_RADIUS_NM}")
AIRCRAFT_CACHE_SECONDS = 12  # matches the client poll; polite floor is ~10 s
_aircraft_cache = {"time": 0.0, "body": b'{"ac":[]}'}


# Optional own receiver: a readsb/dump1090 aircraft.json on the LAN, e.g.
# http://192.168.0.234:8080/data/aircraft.json. Merged by hex over adsb.lol
# (fresher position wins) and used alone if adsb.lol is down -- the answer to
# depending on someone else's API terms. Unset = adsb.lol only.
LOCAL_ADSB_URL = _cfg("LOCAL_ADSB_URL", "")
# position-ish fields a local sighting may overwrite; identity fields (r, t,
# dbFlags) stay adsb.lol's, since a bare readsb has no aircraft database
LOCAL_POS_KEYS = ("lat", "lon", "alt_baro", "alt_geom", "gs", "track",
                  "baro_rate", "geom_rate", "squawk", "seen", "seen_pos")
_local_adsb = {"up": None}


def _nm_from_cbr(lat, lon):
    """Great-circle distance from the CBR centre in nautical miles."""
    import math
    p1, p2 = math.radians(CBR_LAT), math.radians(lat)
    dlat, dlon = p2 - p1, math.radians(lon - CBR_LON)
    h = (math.sin(dlat / 2) ** 2 +
         math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2)
    return 2 * 3440.065 * math.asin(math.sqrt(h))


def _local_aircraft():
    """Aircraft from the local receiver inside the CBR radius, or []. Logs
    only on up/down transitions: when the receiver host is off this is
    polled every 12 s and would otherwise flood the log."""
    if not LOCAL_ADSB_URL:
        return []
    try:
        req = urllib.request.Request(LOCAL_ADSB_URL,
                                     headers={"User-Agent": GEOCODE_UA})
        with urllib.request.urlopen(req, timeout=3) as r:
            ac = json.loads(r.read()).get("aircraft", [])
    except Exception as exc:
        if _local_adsb["up"] is not False:
            print(f"[argus] local receiver unreachable ({exc}); "
                  f"adsb.lol only", flush=True)
        _local_adsb["up"] = False
        return []
    if _local_adsb["up"] is not True:
        print("[argus] local receiver up", flush=True)
    _local_adsb["up"] = True
    return [a for a in ac
            if isinstance(a, dict) and a.get("hex")
            and a.get("lat") is not None and a.get("lon") is not None
            and a.get("seen_pos", 0) < 60
            and _nm_from_cbr(a["lat"], a["lon"]) <= CBR_RADIUS_NM]


def _merge_local(data, local):
    """Fold local sightings into the adsb.lol body, tagging them src=local."""
    by_hex = {a.get("hex"): i for i, a in enumerate(data.get("ac", []))}
    for la in local:
        i = by_hex.get(la["hex"])
        if i is None:
            data["ac"].append(dict(la, src="local"))
            continue
        up = data["ac"][i]
        if la.get("seen_pos", 99) <= up.get("seen_pos", up.get("seen", 99)):
            merged = dict(up, src="local")
            merged.update({k: la[k] for k in LOCAL_POS_KEYS if k in la})
            data["ac"][i] = merged
    return data


@stale_ok(_aircraft_cache, "aircraft")
def aircraft_body():
    if time.time() - _aircraft_cache["time"] > AIRCRAFT_CACHE_SECONDS:
        local = _local_aircraft()
        try:
            req = urllib.request.Request(AIRCRAFT_URL,
                                         headers={"User-Agent": GEOCODE_UA})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())  # refuse to cache junk
        except Exception:
            if not local:
                raise  # stale_ok serves the last good body
            data = {"ac": []}  # adsb.lol down: own receiver carries on
        if local:
            data = _merge_local(data, local)
        _aircraft_cache.update(time=time.time(),
                               body=json.dumps(data).encode())
    return _aircraft_cache["body"]


# --- aircraft enrichment (adsbdb) --------------------------------------------
# adsb.lol already carries registration + ICAO type; adsbdb adds the route
# (callsign -> origin/destination), airline, full type name, owner and a
# photo. Looked up on demand when a plane is clicked, never per poll. No
# published rate limit, so: cache hard (negatives too), space upstream calls,
# rate-limit per client.
ADSBDB_BASE = "https://api.adsbdb.com/v0"
ACINFO_TTL = 24 * 3600
ACINFO_NEG_TTL = 3600
ACINFO_CACHE_MAX = int(_cfg("ACINFO_CACHE_MAX", "3000"))
RATELIMIT_ACINFO_PER_MIN = int(_cfg("RATELIMIT_ACINFO_PER_MIN", "30"))
_acinfo_cache = {}
_acinfo_lock = threading.Lock()
_acinfo_last = [0.0]


def _adsbdb(kind, key):
    """One adsbdb record ('aircraft'/<hex> or 'callsign'/<cs>), or None."""
    hit = _acinfo_cache.get((kind, key))
    if hit and time.time() - hit[0] < (ACINFO_TTL if hit[1] is not None
                                       else ACINFO_NEG_TTL):
        return hit[1]
    with _acinfo_lock:  # serialise + space upstream calls (≤4/s)
        wait = 0.25 - (time.time() - _acinfo_last[0])
        if wait > 0:
            time.sleep(wait)
        _acinfo_last[0] = time.time()
    req = urllib.request.Request(f"{ADSBDB_BASE}/{kind}/{key}",
                                 headers={"User-Agent": GEOCODE_UA})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read()).get("response")
    except urllib.error.HTTPError as exc:
        if exc.code == 429 or exc.code >= 500:
            raise  # transient: don't cache
        data = None  # 404 unknown / 400 unparseable callsign: a real "no"
    data = data if isinstance(data, dict) else None
    _acinfo_cache[(kind, key)] = (time.time(), data)
    _prune_cache(_acinfo_cache, ACINFO_CACHE_MAX)
    return data


def _airport(a):
    if not isinstance(a, dict):
        return None
    return {"iata": a.get("iata_code") or a.get("icao_code") or "",
            "city": a.get("municipality") or "", "name": a.get("name") or ""}


def acinfo_body(hex_, callsign):
    """Compact enrichment for one aircraft. Each half fails independently."""
    out = {}
    if re.fullmatch(r"[0-9a-f]{6}", hex_):
        try:
            a = (_adsbdb("aircraft", hex_) or {}).get("aircraft") or {}
            out.update({k: a.get(v) for k, v in (
                ("reg", "registration"), ("type", "type"),
                ("maker", "manufacturer"), ("owner", "registered_owner"),
                ("photo", "url_photo_thumbnail"), ("photo_url", "url_photo"))
                if a.get(v)})
        except Exception:
            out["aircraft_error"] = True
    if re.fullmatch(r"[A-Z0-9]{3,8}", callsign):
        try:
            fr = (_adsbdb("callsign", callsign) or {}).get("flightroute") or {}
            if fr:
                out["airline"] = (fr.get("airline") or {}).get("name")
                out["from"] = _airport(fr.get("origin"))
                out["to"] = _airport(fr.get("destination"))
        except Exception:
            out["route_error"] = True
    return json.dumps(out).encode()


# --- flight track backfill (adsb.lol traces) --------------------------------
# readsb trace files: `trace` rows are [dt, lat, lon, alt, gs, track, flags,
# ...] offset from `timestamp`; flags bit 2 marks the start of a new leg. We
# return just the current leg, so a click shows where THIS flight came from,
# not the aircraft's whole day. trace_full lags a few minutes, so the tail is
# topped up from trace_recent.
TRACE_BASE = "https://adsb.lol/data/traces"
TRACE_CACHE_SECONDS = 60
TRACE_MAX_POINTS = 600
RATELIMIT_TRACK_PER_MIN = int(_cfg("RATELIMIT_TRACK_PER_MIN", "20"))
_track_cache = {}


def _trace(hex_, kind):
    req = urllib.request.Request(f"{TRACE_BASE}/{hex_[-2:]}/trace_{kind}_{hex_}.json",
                                 headers={"User-Agent": GEOCODE_UA,
                                          "Referer": "https://adsb.lol/"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []
        raise
    if raw[:2] == b"\x1f\x8b":   # served pre-gzipped, header or not
        raw = gzip.decompress(raw)
    d = json.loads(raw)
    base = d.get("timestamp", 0)
    return [(base + p[0], p) for p in d.get("trace", []) if len(p) > 6]


def actrack_body(hex_):
    """GeoJSON LineString of the aircraft's current leg, or an empty FC."""
    hit = _track_cache.get(hex_)
    if hit and time.time() - hit[0] < TRACE_CACHE_SECONDS:
        return hit[1]
    pts = _trace(hex_, "full")
    last_t = pts[-1][0] if pts else 0
    pts += [x for x in _trace(hex_, "recent") if x[0] > last_t]
    start = max((i for i, (_, p) in enumerate(pts) if (p[6] or 0) & 2),
                default=0)
    leg = [(t, p) for t, p in pts[start:] if p[1] is not None and p[2] is not None]
    step = max(1, len(leg) // TRACE_MAX_POINTS)
    if leg:
        leg = leg[::step] + ([] if (len(leg) - 1) % step == 0 else [leg[-1]])
    fc = {"type": "FeatureCollection", "features": []}
    if len(leg) >= 2:
        fc["features"].append({"type": "Feature", "properties": {
            "hex": hex_, "from": int(leg[0][0]), "to": int(leg[-1][0]),
            "points": len(leg)}, "geometry": {"type": "LineString",
            "coordinates": [[round(p[2], 5), round(p[1], 5)] for _, p in leg]}})
    body = json.dumps(fc).encode()
    _track_cache[hex_] = (time.time(), body)
    _prune_cache(_track_cache, 500)
    return body


# --- OpenStreetMap context layers (Overpass) ---------------------------------
# Static-ish reference data: military land + ALPR cameras around the ACT.
# Refreshed weekly; a failed refresh keeps the last good copy indefinitely
# (it's reference, not live) and retries in an hour. Community-mapped: shows
# what OSM contributors have tagged, not an authoritative register.
OVERPASS_MIRRORS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
)
OSM_BBOX = "-36.0,148.7,-35.0,149.6"  # s,w,n,e: ACT + Queanbeyan/Bungendore
OSM_CACHE_SECONDS = 7 * 86400
_military_cache = {"time": 0.0, "body": b""}
_alpr_cache = {"time": 0.0, "body": b""}


def _overpass(query):
    last = None
    for url in OVERPASS_MIRRORS:
        try:
            req = urllib.request.Request(
                url, data=urllib.parse.urlencode({"data": query}).encode(),
                headers={"User-Agent": GEOCODE_UA})
            # these queries answer in seconds when Overpass is healthy; a
            # long wait means a queued/overloaded mirror — move on
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())["elements"]
        except Exception as exc:
            last = exc
    raise last


_osm_lock = threading.Lock()   # one Overpass query at a time: be polite


def _static_osm(cache, label, build):
    """Serve `cache`, rebuilding weekly; keep the old copy on failure."""
    if time.time() - cache["time"] <= OSM_CACHE_SECONDS:
        return cache["body"]
    # one Overpass build at a time; anyone arriving mid-build gets the old
    # copy, or a fast failure (the page retries) — never a minutes-long queue
    if not _osm_lock.acquire(blocking=False):
        if cache["body"]:
            return cache["body"]
        raise RuntimeError(f"{label} still loading")
    try:
        if time.time() - cache["time"] <= OSM_CACHE_SECONDS:
            return cache["body"]
        try:
            cache.update(time=time.time(), body=json.dumps(
                {"type": "FeatureCollection", "features": build()}).encode())
        except Exception as exc:
            if not cache["body"]:
                raise
            print(f"[argus] {label} refresh failed ({exc}); keeping "
                  f"last copy, retry in 1 h", flush=True)
            cache["time"] = time.time() - OSM_CACHE_SECONDS + 3600
        return cache["body"]
    finally:
        _osm_lock.release()


def _warm_static():
    """Fetch the OSM layers at startup so the first toggle is instant;
    Overpass has slow patches, so retry stragglers a few times."""
    todo = [military_body, alpr_body, infra_body]
    for attempt in range(4):
        failed = []
        for fn in todo:
            try:
                fn()
            except Exception as exc:
                failed.append(fn)
                print(f"[argus] warm-up: {fn.__name__} failed ({exc})",
                      flush=True)
            time.sleep(5)   # spacing between Overpass queries
        if not failed:
            return
        todo = failed
        time.sleep(300 * (attempt + 1))


def _osm_geoms(el):
    """Overpass `out geom` element -> list of GeoJSON geometries."""
    def line(pts):
        c = [[p["lon"], p["lat"]] for p in pts or [] if p]
        if len(c) >= 4 and c[0] == c[-1]:
            return {"type": "Polygon", "coordinates": [c]}
        return {"type": "LineString", "coordinates": c} if len(c) >= 2 else None
    if el["type"] == "node":
        return [{"type": "Point", "coordinates": [el["lon"], el["lat"]]}]
    if el["type"] == "way":
        return [g for g in [line(el.get("geometry"))] if g]
    # multipolygon: outer rings usually arrive split across several ways,
    # so stitch segments end-to-end (reversing as needed) before closing
    segs = [[(p["lon"], p["lat"]) for p in m.get("geometry") or [] if p]
            for m in el.get("members", [])
            if m.get("type") == "way" and m.get("role") != "inner"]
    segs = [sg for sg in segs if len(sg) >= 2]
    rings = []
    while segs:
        ring = segs.pop(0)
        grew = True
        while grew and ring[0] != ring[-1]:
            grew = False
            for i, sg in enumerate(segs):
                if sg[0] == ring[-1]:
                    ring += sg[1:]
                elif sg[-1] == ring[-1]:
                    ring += sg[-2::-1]
                elif sg[-1] == ring[0]:
                    ring = sg[:-1] + ring
                elif sg[0] == ring[0]:
                    ring = sg[:0:-1] + ring
                else:
                    continue
                segs.pop(i)
                grew = True
                break
        rings.append([list(p) for p in ring])
    return [g for g in (line([{"lon": x, "lat": y} for x, y in r])
                        for r in rings) if g]


def _military_features():
    els = _overpass(f"[out:json][timeout:90];"
                    f"(nwr[\"landuse\"=\"military\"]({OSM_BBOX});"
                    f"nwr[\"military\"]({OSM_BBOX}););out geom;")
    feats = []
    for el in els:
        t = el.get("tags", {})
        props = {"name": t.get("name", ""), "operator": t.get("operator", ""),
                 "kind": (t.get("military") or "military land").replace("_", " "),
                 "osm": f"{el['type']}/{el['id']}"}
        geoms = _osm_geoms(el)
        feats += [{"type": "Feature", "geometry": g, "properties": props}
                  for g in geoms]
        b = el.get("bounds")
        if props["name"] and b and el["type"] != "node":  # one label per site
            feats.append({"type": "Feature", "properties": dict(props, label=1),
                          "geometry": {"type": "Point", "coordinates": [
                              (b["minlon"] + b["maxlon"]) / 2,
                              (b["minlat"] + b["maxlat"]) / 2]}})
    return feats


def _alpr_features():
    els = _overpass(f"[out:json][timeout:60];"
                    f"node[\"surveillance:type\"~\"ALPR\",i]({OSM_BBOX});out;")
    return [{"type": "Feature",
             "geometry": {"type": "Point", "coordinates": [e["lon"], e["lat"]]},
             "properties": {
                 "operator": e.get("tags", {}).get("operator", ""),
                 "manufacturer": e.get("tags", {}).get("manufacturer", ""),
                 "direction": (e.get("tags", {}).get("camera:direction")
                               or e.get("tags", {}).get("direction", "")),
                 "zone": e.get("tags", {}).get("surveillance:zone", ""),
                 "osm": f"node/{e['id']}"}}
            for e in els if e.get("type") == "node"]


# critical infrastructure: SOCI-style sectors that matter for a local
# picture. Points only (`out center`) — these are reference pins, not
# footprints. Unnamed farm dams (hundreds of them) are dropped.
INFRA_QUERY = ("[out:json][timeout:90];("
               "nwr[\"telecom\"=\"data_center\"]({b});"
               "nwr[\"building\"=\"data_center\"]({b});"
               "nwr[\"waterway\"=\"dam\"][\"name\"]({b});"
               "nwr[\"man_made\"~\"^(water_works|wastewater_plant|reservoir_covered)$\"]({b});"
               "nwr[\"power\"~\"^(substation|plant)$\"]({b});"
               "nwr[\"telecom\"=\"exchange\"]({b});"
               "nwr[\"tower:type\"=\"communication\"][\"name\"]({b});"
               "nwr[\"amenity\"=\"hospital\"]({b});"
               ");out center tags;")
_infra_cache = {"time": 0.0, "body": b""}


def _infra_sector(t):
    if "data_center" in (t.get("telecom"), t.get("building")):
        return "data"
    if t.get("waterway") == "dam" or t.get("man_made") in (
            "water_works", "wastewater_plant", "reservoir_covered"):
        return "water"
    if t.get("power") in ("substation", "plant"):
        return "energy"
    if t.get("telecom") == "exchange" or t.get("tower:type") == "communication":
        return "comms"
    if t.get("amenity") == "hospital":
        return "health"
    return None


def _infra_features():
    feats, seen = [], set()
    for el in _overpass(INFRA_QUERY.format(b=OSM_BBOX)):
        t = el.get("tags", {})
        sector = _infra_sector(t)
        c = el.get("center") or ({"lon": el.get("lon"), "lat": el.get("lat")}
                                 if "lon" in el else None)
        key = f"{el['type']}/{el['id']}"
        if not sector or not c or key in seen:
            continue
        seen.add(key)
        kind = (t.get("power") or t.get("man_made") or t.get("waterway")
                or t.get("telecom") or t.get("amenity") or "")
        if t.get("tower:type") == "communication":
            kind = "comms tower"
        feats.append({"type": "Feature",
                      "geometry": {"type": "Point", "coordinates": [c["lon"], c["lat"]]},
                      "properties": {"sector": sector, "name": t.get("name", ""),
                                     "operator": t.get("operator", ""),
                                     "kind": kind.replace("_", " "), "osm": key}})
    return feats


def infra_body():
    return _static_osm(_infra_cache, "infra", _infra_features)


def military_body():
    return _static_osm(_military_cache, "military", _military_features)


def alpr_body():
    return _static_osm(_alpr_cache, "alpr", _alpr_features)


# --- recent satellite imagery (NASA HLS via CMR, tiles from GIBS) ------------
# Harmonized Landsat Sentinel-2: 30 m true colour, a pass every ~2-3 days.
# serve.py only answers "which days have a pass over the ACT, how cloudy";
# the page pulls tiles straight from GIBS (keyless, CORS *).
CMR_GRANULES = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
HLS_COLLECTIONS = {"S30": "C2021957295-LPCLOUD",   # Sentinel-2
                   "L30": "C2021957657-LPCLOUD"}   # Landsat 8/9
IMAGERY_BBOX = "148.95,-35.5,149.3,-35.1"  # w,s,e,n: urban ACT
IMAGERY_DAYS = 45
IMAGERY_CACHE_SECONDS = 3 * 3600
_imagery_cache = {"time": 0.0, "body": b""}


@stale_ok(_imagery_cache, "imagery")
def imagery_body():
    if time.time() - _imagery_cache["time"] > IMAGERY_CACHE_SECONDS:
        from datetime import datetime, timedelta, timezone
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=IMAGERY_DAYS)
        days = {}
        for prod, coll in HLS_COLLECTIONS.items():
            url = (f"{CMR_GRANULES}?collection_concept_id={coll}"
                   f"&bounding_box={IMAGERY_BBOX}"
                   f"&temporal={start:%Y-%m-%dT%H:%M:%SZ},{end:%Y-%m-%dT%H:%M:%SZ}"
                   f"&sort_key=-start_date&page_size=200")
            for it in json.loads(_http_get(url, timeout=30)).get("items", []):
                u = it.get("umm", {})
                day = (u.get("TemporalExtent", {}).get("RangeDateTime", {})
                       .get("BeginningDateTime", ""))[:10]
                cloud = next((float(a["Values"][0]) for a in
                              u.get("AdditionalAttributes", [])
                              if a.get("Name") == "CLOUD_COVERAGE"), None)
                # GranuleUR = HLS.S30.T55HFA.2026266T001109.v2.0 — the MGRS
                # tile; a pass often clips the ACT with only one of them
                parts = (u.get("GranuleUR") or "").split(".")
                tile = parts[2] if len(parts) > 2 else "?"
                if len(day) == 10:
                    days.setdefault((day, prod), {})[tile] = cloud
        full = max((len(t) for t in days.values()), default=0)
        out = [{"date": d, "product": p,
                "cloud": round(max(c for c in t.values() if c is not None))
                if any(c is not None for c in t.values()) else None,
                "coverage": round(len(t) / full, 2) if full else 0}
               for (d, p), t in days.items()]
        out.sort(key=lambda x: (x["date"], x["product"]), reverse=True)
        _imagery_cache.update(time=time.time(), body=json.dumps(out).encode())
    return _imagery_cache["body"]




# --- wind field (Open-Meteo relay) --------------------------------------------
# Same 5x5 grid over the ACT as poc.html's WIND_GRID; one multi-location call.
# Relayed (rather than browser-direct) so the recorder can snapshot it.
_WIND_LATS = ",".join(str(la) for la in (-34.95, -35.2, -35.45, -35.7, -35.95)
                      for _ in range(5))
_WIND_LONS = ",".join(str(lo) for _ in range(5)
                      for lo in (148.65, 148.95, 149.25, 149.55, 149.85))
WIND_URL = ("https://api.open-meteo.com/v1/forecast"
            f"?latitude={_WIND_LATS}&longitude={_WIND_LONS}"
            "&current=wind_speed_10m,wind_direction_10m,wind_gusts_10m")
WIND_CACHE_SECONDS = 1800  # client refreshes every 30 min
_wind_cache = {"time": 0.0, "body": b"[]"}


@stale_ok(_wind_cache, "wind")
def wind_body():
    if time.time() - _wind_cache["time"] > WIND_CACHE_SECONDS:
        with urllib.request.urlopen(WIND_URL, timeout=10) as r:
            body = r.read()
        json.loads(body)
        _wind_cache.update(time=time.time(), body=body)
    return _wind_cache["body"]


# --- Windy webcams --------------------------------------------------------------
# Visual ground truth: live webcam stills near Canberra via the Windy Webcams
# API v3 (free tier). Key stays server-side (WINDY_KEY in .env / container
# env). Free-tier image URLs carry tokens that EXPIRE AFTER 10 MINUTES — the
# cache TTL must stay under that or popups show dead thumbnails.
WINDY_KEY = _cfg("WINDY_KEY", "")
WEBCAMS_URL = ("https://api.windy.com/webcams/api/v3/webcams"
               "?nearby=-35.28,149.13,150&limit=50"
               "&include=images,location,player,urls")
WEBCAMS_CACHE_SECONDS = 480   # 8 min < the 10-min image-token expiry
_webcams_cache = {"time": 0.0, "body": b""}


@stale_ok(_webcams_cache, "webcams")
def webcams_body():
    if time.time() - _webcams_cache["time"] > WEBCAMS_CACHE_SECONDS:
        req = urllib.request.Request(
            WEBCAMS_URL, headers={"x-windy-api-key": WINDY_KEY,
                                  "User-Agent": "argus/0.11"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        feats = []
        for cam in data.get("webcams", []):
            loc = cam.get("location") or {}
            if loc.get("latitude") is None:
                continue
            images = ((cam.get("images") or {}).get("current") or {})
            feats.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates":
                             [loc["longitude"], loc["latitude"]]},
                "properties": {
                    "id": cam.get("webcamId"),
                    "title": cam.get("title") or "webcam",
                    "status": cam.get("status") or "?",
                    "city": loc.get("city") or "",
                    "preview": images.get("preview") or images.get("thumbnail") or "",
                    "page": ((cam.get("urls") or {}).get("detail")
                             or (cam.get("player") or {}).get("day") or ""),
                }})
        _webcams_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _webcams_cache["body"]


# --- wind field for the particle-flow layer ------------------------------------
# A denser Open-Meteo grid than /wind's 5x5 arrows: 14x14 over the wider
# region, converted server-side to u/v components (m/s) so the client's
# particle advection just does bilinear lookups. Hourly refresh — a 196-point
# multi-location call is weighted as ~196 calls by Open-Meteo, so hourly keeps
# the day's spend around 4.7k of the 10k free budget.
WF_NX = WF_NY = 14
WF_LON0, WF_LON1 = 147.8, 150.4   # west, east
WF_LAT0, WF_LAT1 = -36.6, -34.2   # south, north
_wf_lats, _wf_lons = [], []
for _j in range(WF_NY):
    for _i in range(WF_NX):
        _wf_lats.append(WF_LAT0 + (WF_LAT1 - WF_LAT0) * _j / (WF_NY - 1))
        _wf_lons.append(WF_LON0 + (WF_LON1 - WF_LON0) * _i / (WF_NX - 1))
WINDFIELD_URL = ("https://api.open-meteo.com/v1/forecast"
                 f"?latitude={','.join(f'{v:.3f}' for v in _wf_lats)}"
                 f"&longitude={','.join(f'{v:.3f}' for v in _wf_lons)}"
                 "&current=wind_speed_10m,wind_direction_10m")
WINDFIELD_CACHE_SECONDS = 3600
_windfield_cache = {"time": 0.0, "body": b""}


@stale_ok(_windfield_cache, "windfield")
def windfield_body():
    if time.time() - _windfield_cache["time"] > WINDFIELD_CACHE_SECONDS:
        import math
        with urllib.request.urlopen(WINDFIELD_URL, timeout=20) as r:
            data = json.loads(r.read())
        if isinstance(data, dict):   # single-location shape, shouldn't happen
            data = [data]
        u, v = [], []
        for loc in data:
            cur = loc.get("current", {})
            spd = (cur.get("wind_speed_10m") or 0) / 3.6   # km/h -> m/s
            # meteorological direction = where wind comes FROM; flow vector
            # points the opposite way
            to_rad = math.radians(((cur.get("wind_direction_10m") or 0) + 180) % 360)
            u.append(round(spd * math.sin(to_rad), 2))
            v.append(round(spd * math.cos(to_rad), 2))
        _windfield_cache.update(time=time.time(), body=json.dumps({
            "nx": WF_NX, "ny": WF_NY,
            "lon0": WF_LON0, "lon1": WF_LON1,
            "lat0": WF_LAT0, "lat1": WF_LAT1,
            "u": u, "v": v}).encode())
    return _windfield_cache["body"]


# --- current weather (Open-Meteo relay) ---------------------------------------
# Mirrors poc.html's fetchWeather call exactly (superset of _wx_current's
# fields — that one stays as-is for the SITREP path). Verbatim body relay.
WEATHER_URL = (f"https://api.open-meteo.com/v1/forecast"
               f"?latitude={CBR_LAT}&longitude={CBR_LON}"
               "&current=temperature_2m,apparent_temperature,precipitation,"
               "weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
               "&timezone=Australia%2FSydney")
WEATHER_CACHE_SECONDS = 600  # client polls every 10 min
_weather_cache = {"time": 0.0, "body": b"{}"}


@stale_ok(_weather_cache, "weather")
def weather_body():
    if time.time() - _weather_cache["time"] > WEATHER_CACHE_SECONDS:
        with urllib.request.urlopen(WEATHER_URL, timeout=10) as r:
            body = r.read()
        json.loads(body)
        _weather_cache.update(time=time.time(), body=body)
    return _weather_cache["body"]


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
        # quakes are rare — only worth a line when felt or roughly nearby
        import math
        qs = []
        for f in json.loads(quakes_body())["features"]:
            lon, lat = f["geometry"]["coordinates"]
            p = f["properties"]
            near = math.hypot((lon - 149.13) * 92, (lat + 35.28) * 111) < 300
            if p["felt"] or near:
                qs.append(f"M{p['mag']} {p['place']} ({p['time']}"
                          f"{', felt reports: ' + str(p['felt']) if p['felt'] else ''})")
        if qs:
            lines.append("Earthquakes (7 days): " + "; ".join(qs[:5]))
    except Exception:
        pass
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


# TomTom tiles are the only upstream here billed per request, against a daily
# free-tier allowance. The counter is keyed by UTC day to match how that
# allowance resets. Over budget we keep serving whatever is already in the tile
# cache -- stale traffic beats a blank layer, and both beat a surprise bill --
# and stop calling upstream entirely until the day rolls over.
_tile_budget = {"day": "", "used": 0}


def _tile_budget_take():
    """Claim one upstream tile fetch. False when the day's budget is spent."""
    day = time.strftime("%Y-%m-%d", time.gmtime())
    with _guard_lock:
        if _tile_budget["day"] != day:
            _tile_budget.update(day=day, used=0)
        if _tile_budget["used"] >= TOMTOM_DAILY_TILE_BUDGET:
            return False
        _tile_budget["used"] += 1
        return True


def valid_tile(z, x, y):
    """Reject coordinates that cannot exist. The route regex only proves these
    are digits, so without this a request for z=99 becomes a real upstream call
    and a permanent cache entry."""
    if not 0 <= z <= 22:
        return False
    span = 1 << z
    return 0 <= x < span and 0 <= y < span


def tomtom_tile(z, x, y):
    z, x, y = int(z), int(x), int(y)
    if not valid_tile(z, x, y):
        raise ValueError("tile out of range")
    key = f"{z}/{x}/{y}"
    hit = _tile_cache.get(key)
    if hit and time.time() - hit[0] < TILE_CACHE_SECONDS:
        return hit[1]
    if not _tile_budget_take():
        if hit:
            return hit[1]  # stale, but free
        raise RuntimeError("daily TomTom tile budget spent")
    url = (f"https://api.tomtom.com/traffic/map/4/tile/flow/relative0/"
           f"{z}/{x}/{y}.png?key={TOMTOM_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = r.read()
    except Exception:
        if hit:
            return hit[1]
        raise
    _tile_cache[key] = (time.time(), body)
    _prune_cache(_tile_cache, TOMTOM_TILE_CACHE_MAX)
    return body


# --- history recorder + time-slider store ------------------------------------
# An always-on background thread snapshots the incident feeds into SQLite so
# the client can scrub backwards in time. It calls the same *_body() functions
# the live endpoints use, so it respects their caches and generates no more
# upstream load than a single live viewer. Snapshots are gzipped and only
# written when the body actually changed (incident feeds are near-static when
# quiet), which keeps the store to a few MB over the retention window.
HISTORY_DB = _cfg("COP_DB",
                  os.path.join(os.path.dirname(__file__), "history.db"))
HISTORY_HOURS = int(_cfg("COP_HISTORY_HOURS", "72"))

# source name -> (body function, poll interval seconds). The interval matches
# each feed's own cache TTL — polling faster would just re-read the same cache.
HISTORY_SOURCES = {
    "esa": (esa_body, 60),
    "rfs": (rfs_body, 60),
    "power": (power_body, 120),
    "firms": (firms_body, 600),
    "closures": (closures_body, 600),
    "quakes": (quakes_body, 600),
    "airq": (airq_body, 900),
    # movers: recorded at 30 s (not the 12/15 s live cadence) — replay through
    # a slider doesn't need that fidelity and it quarters the storage. Every
    # snapshot differs (positions move), so md5 dedup never fires for these.
    "aircraft": (aircraft_body, 30),
    "transit": (transit_body, 30),
    # ambient context: cheap, dedup-heavy
    "wind": (wind_body, 1800),
    "weather": (weather_body, 600),
    "bom": (bom_body, 300),
    "news": (news_body, 600),
}
# empty payload per source, shaped like the live body so the client's parser
# is unchanged when a time has no snapshot at/before it (esa/bom/news/wind are
# bare lists, aircraft is {"ac": []}, weather is an object; everything else is
# a GeoJSON FeatureCollection).
_EMPTY_BODY = {"esa": b"[]", "bom": b"[]", "news": b"[]", "wind": b"[]",
               "aircraft": b'{"ac":[]}', "weather": b"{}"}
_EMPTY_FC = b'{"type":"FeatureCollection","features":[]}'

_last_hash = {}  # source -> md5 of the last stored body, to skip duplicates


def _db_conn():
    conn = sqlite3.connect(HISTORY_DB, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")   # concurrent reads while recording
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _db_init():
    with _db_conn() as c:
        c.execute("CREATE TABLE IF NOT EXISTS snapshots ("
                  "source TEXT NOT NULL, t INTEGER NOT NULL, body BLOB NOT NULL,"
                  " PRIMARY KEY (source, t))")


def _record(source, body):
    """Store a gzipped snapshot, but only if it changed since the last one."""
    h = hashlib.md5(body).hexdigest()
    if _last_hash.get(source) == h:
        return
    with _db_conn() as c:
        c.execute("INSERT OR REPLACE INTO snapshots(source, t, body) "
                  "VALUES (?, ?, ?)",
                  (source, int(time.time()), gzip.compress(body)))
    _last_hash[source] = h


def _prune():
    cutoff = int(time.time()) - HISTORY_HOURS * 3600
    with _db_conn() as c:
        c.execute("DELETE FROM snapshots WHERE t < ?", (cutoff,))


# --- empty-layer watch --------------------------------------------------------
# Three silent failures in Sep 2026 — Evoenergy blanked by one malformed row,
# ESA dropping every ambulance record, Essential Energy's future.kml going 404
# — all kept serving 200 OK with a plausible but hollow layer, and nothing
# noticed. This watches per-group feature counts plus upstream errors, learns
# from the history DB how long each group is normally empty, and raises an
# alarm when one stays empty (or a fetch keeps failing) for longer than that.
# Feeds where empty is simply "quiet" (rfs, firms, quakes, bom) aren't watched.
WATCH_FLOOR_S = int(_cfg("WATCH_EMPTY_FLOOR_MIN", "30")) * 60
WATCH_FAIL_S = int(_cfg("WATCH_FAIL_MIN", "60")) * 60
WATCH_FORGET_S = 30 * 86400   # a group empty this long is dropped, not nagged
WATCH_STATE = os.path.join(os.path.dirname(os.path.abspath(HISTORY_DB)),
                           "layer-watch.json")
TELEGRAM_BOT_TOKEN = _cfg("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = _cfg("TELEGRAM_CHAT_ID", "")
_watch_lock = threading.Lock()
_watch = {"groups": {}, "fails": {}, "alarms": {}, "baseline_at": 0.0}


def _watch_groups(source, body):
    """{group: count} for one recorded body ({} = source not watched)."""
    d = json.loads(body)
    if source == "power":
        g = {"power/Evoenergy": 0, "power/Essential Energy": 0}
        for f in d.get("features", []):
            if f.get("geometry", {}).get("type") == "Point":
                k = "power/" + f.get("properties", {}).get("src", "?")
                g[k] = g.get(k, 0) + 1
        return g
    if source == "esa":
        return {"esa/all": len(d), "esa/ambulance": sum(
            1 for i in d if (i.get("title") or "").upper().startswith("AMBULANCE"))}
    if source == "transit":
        g = {"transit/lightrail": 0, "transit/bus": 0}
        for f in d.get("features", []):
            k = "transit/" + (f.get("properties", {}).get("mode") or "?")
            g[k] = g.get(k, 0) + 1
        return g
    if source == "aircraft":
        return {"aircraft": len(d.get("ac", []))}
    if source in ("airq", "closures"):
        return {source: len(d.get("features", []))}
    if source == "news":
        return {"news": len(d)}
    return {}


def _fmt_dur(sec):
    sec = int(sec)
    if sec < 3600:
        return f"{sec // 60} min"
    if sec < 86400:
        return f"{sec // 3600} h {sec % 3600 // 60:02d} min"
    return f"{sec // 86400} d {sec % 86400 // 3600} h"


def _watch_baseline():
    """Replay the history window: per group, the longest COMPLETED empty
    stretch (the "normal" gap) and when it was last non-empty. Snapshots are
    change-driven, so a snapshot's count holds until the next one."""
    runs = {}   # group -> {"zero_since", "max_gap", "last_nonzero"}
    with _db_conn() as c:
        for source in HISTORY_SOURCES:
            for t, blob in c.execute("SELECT t, body FROM snapshots WHERE "
                                     "source=? ORDER BY t", (source,)):
                try:
                    groups = _watch_groups(source, gzip.decompress(blob))
                except Exception:
                    continue
                for g, n in groups.items():
                    r = runs.setdefault(g, {"zero_since": None, "max_gap": 0,
                                            "last_nonzero": None})
                    if n > 0:
                        if r["zero_since"] is not None and r["last_nonzero"]:
                            r["max_gap"] = max(r["max_gap"], t - r["zero_since"])
                        r["zero_since"] = None
                        r["last_nonzero"] = t
                    elif r["zero_since"] is None:
                        r["zero_since"] = t
    now = time.time()
    with _watch_lock:
        for g, r in runs.items():
            st = _watch["groups"].setdefault(g, {"count": None,
                                                 "last_nonzero": None})
            st["max_gap"] = r["max_gap"]
            if r["last_nonzero"] is not None:
                # non-empty until the snapshot that emptied it (or still now)
                seen = r["zero_since"] if r["zero_since"] else now
                st["last_nonzero"] = max(st["last_nonzero"] or 0, seen)
        _watch["baseline_at"] = now


def _watch_load():
    try:
        with open(WATCH_STATE) as f:
            saved = json.load(f)
    except (OSError, ValueError):
        return
    with _watch_lock:
        for g, ts in saved.get("last_nonzero", {}).items():
            _watch["groups"].setdefault(g, {"count": None, "max_gap": 0,
                                            "last_nonzero": None})
            _watch["groups"][g]["last_nonzero"] = ts


def _watch_save():
    with _watch_lock:
        data = {"last_nonzero": {g: st["last_nonzero"] for g, st in
                                 _watch["groups"].items() if st["last_nonzero"]}}
    tmp = WATCH_STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, WATCH_STATE)


def _watch_observe(source, body):
    try:
        groups = _watch_groups(source, body)
    except Exception:
        return
    now = time.time()
    with _watch_lock:
        for g, n in groups.items():
            st = _watch["groups"].setdefault(g, {"count": None, "max_gap": 0,
                                                 "last_nonzero": None})
            st["count"] = n
            if n > 0:
                st["last_nonzero"] = now


def watch_ok(key):
    with _watch_lock:
        _watch["fails"].pop(key, None)


def watch_fail(key, exc):
    with _watch_lock:
        f = _watch["fails"].setdefault(key, {"first": time.time()})
        f["last"], f["err"] = time.time(), str(exc)[:160]


def _telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    def send():
        try:
            data = urllib.parse.urlencode({"chat_id": TELEGRAM_CHAT_ID,
                                           "text": text}).encode()
            urllib.request.urlopen(urllib.request.Request(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data=data), timeout=15).read()
        except Exception as exc:
            print(f"[argus] watch: telegram send failed ({exc})", flush=True)
    threading.Thread(target=send, daemon=True).start()


def _watch_evaluate():
    now = time.time()
    alarms = {}
    with _watch_lock:
        for g, st in list(_watch["groups"].items()):
            last = st.get("last_nonzero")
            if last and now - last > WATCH_FORGET_S:
                print(f"[argus] watch: forgetting {g} (empty "
                      f"{_fmt_dur(now - last)})", flush=True)
                del _watch["groups"][g]
                continue
            # watched only once seen non-empty; judged only on a live reading
            if not last or st.get("count") != 0:
                continue
            gap, normal = now - last, st.get("max_gap", 0)
            limit = max(WATCH_FLOOR_S, 1.5 * normal)
            if gap > limit:
                alarms["empty:" + g] = {
                    "kind": "empty", "what": g, "since": int(last),
                    "msg": f"{g} has been empty for {_fmt_dur(gap)} "
                           f"(longest normal gap: {_fmt_dur(normal)})"}
        for k, f in list(_watch["fails"].items()):
            if now - f.get("last", 0) > 1800:
                del _watch["fails"][k]   # nobody's asking any more (on-demand
                continue                 # feeds): stale, not failing
            if now - f["first"] > WATCH_FAIL_S:
                alarms["fail:" + k] = {
                    "kind": "failing", "what": k, "since": int(f["first"]),
                    "msg": f"{k} failing for {_fmt_dur(now - f['first'])}: "
                           f"{f.get('err', '?')}"}
        prev = _watch["alarms"]
        _watch["alarms"] = alarms
    for k in alarms.keys() - prev.keys():
        print(f"[argus] WATCH ALARM: {alarms[k]['msg']}", flush=True)
        _telegram("⚠ Argus layer alarm: " + alarms[k]["msg"])
    for k in prev.keys() - alarms.keys():
        print(f"[argus] watch cleared: {prev[k]['what']}", flush=True)
        _telegram("✓ Argus: " + prev[k]["what"] + " is back")


def watch_body():
    now = time.time()
    with _watch_lock:
        return json.dumps({
            "alarms": sorted(_watch["alarms"].values(), key=lambda a: a["since"]),
            "groups": {g: {"count": st.get("count"),
                           "last_nonzero": int(st["last_nonzero"]) if st.get("last_nonzero") else None,
                           "normal_gap": int(st.get("max_gap", 0))}
                       for g, st in sorted(_watch["groups"].items())},
            "failing": {k: {"since": int(f["first"]), "err": f.get("err")}
                        for k, f in _watch["fails"].items()},
            "baseline_age": int(now - _watch["baseline_at"]) if _watch["baseline_at"] else None,
            "telegram": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)}).encode()


def _recorder():
    _db_init()
    # seed last-hash from the newest stored snapshot per source so a restart
    # doesn't immediately write a duplicate of what's already there
    try:
        with _db_conn() as c:
            for source in HISTORY_SOURCES:
                row = c.execute("SELECT body FROM snapshots WHERE source=? "
                                "ORDER BY t DESC LIMIT 1", (source,)).fetchone()
                if row:
                    _last_hash[source] = hashlib.md5(
                        gzip.decompress(row[0])).hexdigest()
    except Exception:
        pass
    _watch_load()
    try:
        _watch_baseline()
    except Exception as exc:
        print(f"[argus] watch: baseline failed ({exc})", flush=True)
    next_due = {s: 0.0 for s in HISTORY_SOURCES}
    last_prune = last_eval = 0.0
    while True:
        now = time.time()
        for source, (fn, interval) in HISTORY_SOURCES.items():
            if now < next_due[source]:
                continue
            next_due[source] = now + interval
            try:
                body = fn()
                _record(source, body)
                _watch_observe(source, body)
            except Exception:
                pass  # a dead feed (or missing API key) mustn't stop the rest
        if now - last_eval > 60:
            last_eval = now
            try:
                if now - _watch["baseline_at"] > 6 * 3600:
                    _watch_baseline()
                _watch_evaluate()
                _watch_save()
            except Exception as exc:
                print(f"[argus] watch: evaluate failed ({exc})", flush=True)
        if now - last_prune > 3600:
            last_prune = now
            try:
                _prune()
            except Exception:
                pass
        time.sleep(5)


def history_body(source, at):
    """(as_of_epoch, body_bytes) for the newest snapshot at/before `at`, or
    (0, empty-shaped body) if nothing was recorded that early."""
    if source not in HISTORY_SOURCES:
        raise KeyError(source)
    with _db_conn() as c:
        row = c.execute("SELECT t, body FROM snapshots WHERE source=? AND t<=? "
                        "ORDER BY t DESC LIMIT 1", (source, at)).fetchone()
    if not row:
        return 0, _EMPTY_BODY.get(source, _EMPTY_FC)
    return int(row[0]), gzip.decompress(row[1])


# --- 72 h incident heat --------------------------------------------------------
# Unique ESA incidents seen in the recorder's last ESAHIST_HOURS of snapshots,
# flattened to bare GeoJSON points — density fuel for the client heatmap.
# Reads our own history DB, so it costs no upstream calls.
ESAHIST_HOURS = 72
ESAHIST_CACHE_SECONDS = 600
_esahist_cache = {"time": 0.0, "body": b""}


def esahist_body():
    if time.time() - _esahist_cache["time"] > ESAHIST_CACHE_SECONDS:
        cutoff = int(time.time()) - ESAHIST_HOURS * 3600
        with _db_conn() as c:
            rows = c.execute("SELECT body FROM snapshots WHERE source='esa' "
                             "AND t>=? ORDER BY t", (cutoff,)).fetchall()
        seen = {}  # guid -> (lon, lat); later snapshots win
        for (blob,) in rows:
            try:
                items = json.loads(gzip.decompress(blob))
            except Exception:
                continue
            for i in items:
                if i.get("state") != "ACT":
                    continue
                pt = (i.get("point") or {}).get("coordinates")
                if not pt or len(pt) != 2:
                    continue
                try:  # feed ships [lat, lon] as strings
                    lat, lon = float(pt[0]), float(pt[1])
                except (TypeError, ValueError):
                    continue
                seen[i.get("guid") or i.get("title")] = (lon, lat)
        feats = [{"type": "Feature",
                  "geometry": {"type": "Point", "coordinates": [lon, lat]},
                  "properties": {}} for lon, lat in seen.values()]
        _esahist_cache.update(time=time.time(), body=json.dumps(
            {"type": "FeatureCollection", "features": feats}).encode())
    return _esahist_cache["body"]


# --- activity histogram --------------------------------------------------------
# The recorder's md5 dedup means a snapshot row only exists when a feed's
# content CHANGED — so row count per hour is a free "how eventful was this
# hour" signal for shading the time slider. Movers (aircraft/transit) and
# ambient sources change every tick, so only incident-ish sources count.
ACTIVITY_SOURCES = ("esa", "rfs", "power", "firms", "closures", "quakes",
                    "airq", "bom")
ACTIVITY_CACHE_SECONDS = 300
_activity_cache = {"time": 0.0, "body": b""}


def history_activity_body():
    if time.time() - _activity_cache["time"] > ACTIVITY_CACHE_SECONDS:
        cutoff = int(time.time()) - HISTORY_HOURS * 3600
        marks = ",".join("?" * len(ACTIVITY_SOURCES))
        with _db_conn() as c:
            rows = c.execute(
                f"SELECT (t/3600)*3600 AS hr, COUNT(*) FROM snapshots "
                f"WHERE t >= ? AND source IN ({marks}) "
                f"GROUP BY hr ORDER BY hr",
                (cutoff, *ACTIVITY_SOURCES)).fetchall()
        _activity_cache.update(time=time.time(), body=json.dumps(
            {"buckets": [[int(r[0]), int(r[1])] for r in rows]}).encode())
    return _activity_cache["body"]


def history_range_body():
    """Per-source and overall min/max snapshot times, so the slider knows how
    far back it can scrub."""
    out = {}
    with _db_conn() as c:
        for source in HISTORY_SOURCES:
            r = c.execute("SELECT MIN(t), MAX(t), COUNT(*) FROM snapshots "
                          "WHERE source=?", (source,)).fetchone()
            out[source] = {"min": r[0], "max": r[1], "count": r[2]}
    mins = [v["min"] for v in out.values() if v["min"]]
    maxs = [v["max"] for v in out.values() if v["max"]]
    return json.dumps({"sources": out,
                       "min": min(mins) if mins else None,
                       "max": max(maxs) if maxs else None,
                       "now": int(time.time())}).encode()


class Handler(SimpleHTTPRequestHandler):
    def client_ip(self):
        """Best available client address, for rate limiting.

        Behind the Cloudflare tunnel every request arrives from loopback, so
        CF-Connecting-IP is the only thing separating one visitor from another
        -- but it is caller-supplied, so honour it only from a peer we have
        designated as a proxy."""
        peer = self.client_address[0]
        if peer in TRUSTED_PROXY_PEERS:
            fwd = (self.headers.get("CF-Connecting-IP")
                   or self.headers.get("X-Forwarded-For", "").split(",")[0])
            if fwd.strip():
                return fwd.strip()
        return peer

    def too_many(self, bucket, per_min):
        """Send a 429 and return True when the caller is over its allowance."""
        if rate_limited(bucket, self.client_ip(), per_min):
            self.send_error(429, "rate limited")
            return True
        return False

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.send_response(302)
            self.send_header("Location", "/poc.html")
            self.end_headers()
            return
        if self.path.rstrip("/") == "/history/range":
            try:
                body = history_range_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "history unavailable")
            return
        # NB: must precede the /history/<source> regex, which would otherwise
        # swallow "activity" as an unknown source
        if self.path.rstrip("/") == "/history/activity":
            try:
                body = history_activity_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "activity unavailable")
            return
        hm = re.fullmatch(r"/history/(\w+)",
                          urllib.parse.urlparse(self.path).path)
        if hm:
            at = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query).get("at", [""])[0]
            try:
                at = int(float(at)) if at else int(time.time())
            except ValueError:
                self.send_error(400, "bad at")
                return
            try:
                as_of, body = history_body(hm.group(1), at)
            except KeyError:
                self.send_error(404, "unknown source")
                return
            except Exception:
                self.send_error(502, "history unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Snapshot-Time", str(as_of))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        m = re.fullmatch(r"/tomtom/(\d+)/(\d+)/(\d+)\.png", self.path)
        if m:
            if not TOMTOM_KEY:
                self.send_error(503, "no TOMTOM_API_KEY in .env")
                return
            if self.too_many("tomtom", RATELIMIT_TOMTOM_PER_MIN):
                return
            try:
                body = tomtom_tile(*m.groups())
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except ValueError:
                self.send_error(400, "bad tile coordinate")
            except RuntimeError:
                self.send_error(503, "daily tile budget spent")
            except Exception:
                self.send_error(502, "tomtom unreachable")
            return
        if self.path.rstrip("/") == "/sitrep":
            if self.too_many("sitrep", RATELIMIT_SITREP_PER_MIN):
                return
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
        if self.path.rstrip("/") == "/aircraft":
            try:
                body = aircraft_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "aircraft feed unreachable")
            return
        if self.path.rstrip("/") == "/wind":
            try:
                body = wind_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "wind feed unreachable")
            return
        if self.path.rstrip("/") == "/webcams":
            if not WINDY_KEY:
                self.send_error(503, "no WINDY_KEY in .env")
                return
            try:
                body = webcams_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "webcams unreachable")
            return
        if self.path.rstrip("/") == "/windfield":
            try:
                body = windfield_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "wind field unreachable")
            return
        if self.path.rstrip("/") == "/weather":
            try:
                body = weather_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "weather feed unreachable")
            return
        if self.path.startswith("/geocode"):
            q = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query).get("q", [""])[0]
            if not q.strip():
                self.send_error(400, "missing q")
                return
            if self.too_many("geocode", RATELIMIT_GEOCODE_PER_MIN):
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
        if self.path.rstrip("/") == "/quakes":
            try:
                body = quakes_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "earthquake feed unreachable")
            return
        if self.path.startswith("/acinfo"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            hex_ = qs.get("hex", [""])[0].strip().lower()
            cs = qs.get("cs", [""])[0].strip().upper()
            if not hex_ and not cs:
                self.send_error(400, "missing hex/cs")
                return
            if self.too_many("acinfo", RATELIMIT_ACINFO_PER_MIN):
                return
            body = acinfo_body(hex_, cs)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith("/actrack"):
            hex_ = urllib.parse.parse_qs(urllib.parse.urlparse(
                self.path).query).get("hex", [""])[0].strip().lower()
            if not re.fullmatch(r"[0-9a-f]{6}", hex_):
                self.send_error(400, "bad hex")
                return
            if self.too_many("actrack", RATELIMIT_TRACK_PER_MIN):
                return
            try:
                body = actrack_body(hex_)
            except Exception:
                self.send_error(502, "track source unreachable")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        for route, fn, what in (("/military", military_body, "OSM military"),
                                ("/infra", infra_body, "OSM infrastructure"),
                                ("/health/watch", watch_body, "layer watch"),
                                ("/alpr", alpr_body, "OSM ALPR"),
                                ("/imagery", imagery_body, "imagery catalog")):
            if self.path.rstrip("/") == route:
                try:
                    body = fn()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception:
                    self.send_error(502, f"{what} unreachable")
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
        if self.path.rstrip("/") == "/suburbs":
            try:
                body = suburbs_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "suburb boundaries unreachable")
            return
        if self.path.rstrip("/") == "/esahist":
            try:
                body = esahist_body()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_error(502, "incident history unavailable")
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
    print("Argus → http://localhost:8899/poc.html  (Ctrl-C to stop)")
    # Always-on recorder feeds the time slider; runs even with no viewers.
    threading.Thread(target=_recorder, daemon=True).start()
    threading.Thread(target=_warm_static, daemon=True).start()
    # Threading: one slow upstream fetch must not stall every other request
    ThreadingHTTPServer(("0.0.0.0", 8899), Handler).serve_forever()
