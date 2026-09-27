import importlib.util, sys, time, urllib.request
spec = importlib.util.spec_from_file_location("argus_serve", "/home/andrew/Projects/argus/serve.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
fails = []
def check(name, cond):
    print(("  ok   " if cond else "  FAIL ") + name)
    if not cond: fails.append(name)

print("valid_tile:")
check("z=0 origin ok", m.valid_tile(0,0,0))
check("z=13 canberra ok", m.valid_tile(13,7373,4879))
check("z=99 rejected", not m.valid_tile(99,0,0))
check("z=23 rejected", not m.valid_tile(23,0,0))
check("x past span rejected", not m.valid_tile(2,4,0))
check("x at span-1 ok", m.valid_tile(2,3,3))

print("tomtom_tile input validation:")
try:
    m.tomtom_tile("99","0","0"); check("z=99 raises ValueError", False)
except ValueError: check("z=99 raises ValueError", True)
except Exception as e: check("z=99 raises ValueError (got %r)" % e, False)

print("daily budget:")
m.TOMTOM_DAILY_TILE_BUDGET = 2
m._tile_budget.update(day="", used=0)
check("take 1", m._tile_budget_take())
check("take 2", m._tile_budget_take())
check("take 3 refused", not m._tile_budget_take())
m._tile_budget.update(day="1999-01-01", used=99)
check("new UTC day resets", m._tile_budget_take())

print("budget exhausted serves stale tile, no upstream call:")
m.TOMTOM_DAILY_TILE_BUDGET = 0
m._tile_budget.update(day="", used=0)
m._tile_cache["5/1/1"] = (0.0, b"STALEPNG")   # time 0 => past TTL
called = []
_real = urllib.request.urlopen
urllib.request.urlopen = lambda *a, **k: called.append(a) or (_ for _ in ()).throw(AssertionError("upstream called"))
try:
    check("returns cached bytes", m.tomtom_tile("5","1","1") == b"STALEPNG")
    check("no upstream call", not called)
    try:
        m.tomtom_tile("5","2","2"); check("no cache + no budget raises", False)
    except RuntimeError: check("no cache + no budget raises RuntimeError", True)
    except Exception as e: check("expected RuntimeError, got %r" % e, False)
finally:
    urllib.request.urlopen = _real

print("_prune:")
c = {str(i): (float(i), b"x") for i in range(10)}
m._prune_cache(c, 4)
check("capped to 4", len(c) == 4)
check("kept newest", set(c) == {"6","7","8","9"})
m._prune_cache(c, 100); check("under cap untouched", len(c) == 4)

print("rate_limited:")
m._ratelimit.clear()
check("first 3 of 3 pass", not any(m.rate_limited("b","1.2.3.4",3) for _ in range(3)))
check("4th blocked", m.rate_limited("b","1.2.3.4",3))
check("other IP unaffected", not m.rate_limited("b","5.6.7.8",3))
check("other bucket unaffected", not m.rate_limited("other","1.2.3.4",3))
m._ratelimit.clear()
check("per_min=0 disables", not any(m.rate_limited("b","1.2.3.4",0) for _ in range(50)))

print("stale_ok:")
cache = {"time": time.time(), "body": b"GOOD"}
@m.stale_ok(cache, "test")
def boom(): raise urllib.error.URLError("down")
check("serves stale on failure", boom() == b"GOOD")
cache["time"] = time.time() - (m.STALE_MAX_SECONDS + 10)
try:
    boom(); check("too-old cache re-raises", False)
except urllib.error.URLError: check("too-old cache re-raises", True)
cold = {"time": 0.0, "body": b""}
@m.stale_ok(cold, "cold")
def boom2(): raise urllib.error.URLError("down")
try:
    boom2(); check("never-warm cache re-raises", False)
except urllib.error.URLError: check("never-warm cache re-raises", True)
ok = {"time": 0.0, "body": b""}
@m.stale_ok(ok, "ok")
def fine(): return b"FRESH"
check("success path passes through", fine() == b"FRESH")
check("__name__ preserved", fine.__name__ == "fine")

print("\ndecorators applied to feeds:")
for fn in ("esa_body","firms_body","rfs_body","aircraft_body","weather_body","webcams_body"):
    check(fn + " wrapped", getattr(m, fn).__wrapped__ is not None)

print("\nevoenergy per-record guard:")
ring = '[{"lat":-35.26,"lng":149.13},{"lat":-35.27,"lng":149.14},{"lat":-35.28,"lng":149.13}]'
base = {"Status": "Active", "Type": "Unplanned", "OutageID": "T1",
        "PolygonCoordinates": ring}
ok = m._evo_record(dict(base, PolygonCentroidCoordinate='{"lat":-35.27,"lng":149.133}'))
check("good centroid kept", ok and ok[0]["geometry"]["coordinates"] == [149.133, -35.27])
# the real 2026-09-09 failure: centroid truncated at exactly 40 chars
trunc = m._evo_record(dict(base, PolygonCentroidCoordinate='{"lat":-35.264601999999996,"lng":149.138'))
check("truncated centroid falls back to ring mean",
      trunc and abs(trunc[0]["geometry"]["coordinates"][0] - 149.1333) < 1e-3)
check("no centroid, bad ring -> dropped, no raise",
      m._evo_record(dict(base, PolygonCentroidCoordinate="{", PolygonCoordinates="[{")) == [])
check("completed status filtered", m._evo_record(dict(base, Status="Completed")) == [])
_real_get = m._http_get
page = ('x outagesViewModel = [' + ','.join(__import__("json").dumps(r) for r in (
    dict(base, OutageID="A", PolygonCentroidCoordinate='{"lat":-35.27,"lng":149.13}'),
    dict(base, OutageID="B", PolygonCentroidCoordinate='{"lat":-35.2', PolygonCoordinates="nope"),
    ["not", "a", "dict"],
    dict(base, OutageID="C", PolygonCentroidCoordinate='{"lat":-35.29,"lng":149.12}'),
)) + '];').encode()
m._http_get = lambda *a, **k: page
try:
    ids = {f["properties"]["id"] for f in m._evo_features()}
    check("one bad record doesn't blank the utility (A and C survive)", ids == {"A", "C"})
finally:
    m._http_get = _real_get

print("\nlocal ADS-B receiver merge:")
up = {"ac": [{"hex": "7c1111", "r": "VH-AAA", "t": "B738", "dbFlags": 1,
              "lat": -35.0, "lon": 149.0, "seen_pos": 5}]}
loc = [{"hex": "7c1111", "lat": -35.1, "lon": 149.1, "seen_pos": 1},
       {"hex": "7c2222", "lat": -35.2, "lon": 149.2, "seen_pos": 2}]
out = m._merge_local(__import__("copy").deepcopy(up), loc)
a = {x["hex"]: x for x in out["ac"]}
check("fresher local position wins", a["7c1111"]["lat"] == -35.1)
check("identity fields kept from adsb.lol", a["7c1111"]["r"] == "VH-AAA" and a["7c1111"]["dbFlags"] == 1)
check("local-only aircraft added + tagged", a.get("7c2222", {}).get("src") == "local")
stale = m._merge_local(__import__("copy").deepcopy(up), [dict(loc[0], seen_pos=30)])
check("staler local position ignored", stale["ac"][0]["lat"] == -35.0)
check("100 NM away is outside the radius", m._nm_from_cbr(-35.28, 151.17) > m.CBR_RADIUS_NM)
# adsb.lol down + local up => local carries on instead of stale/502
m._aircraft_cache.update(time=0.0, body=b'{"ac":[]}')
_real_local, _real_open = m._local_aircraft, urllib.request.urlopen
m._local_aircraft = lambda: [dict(loc[1])]
urllib.request.urlopen = lambda *a, **k: (_ for _ in ()).throw(OSError("adsb.lol down"))
try:
    body = __import__("json").loads(m.aircraft_body())
    check("adsb.lol down -> own receiver still serves", [x["hex"] for x in body["ac"]] == ["7c2222"])
finally:
    m._local_aircraft, urllib.request.urlopen = _real_local, _real_open
m.LOCAL_ADSB_URL = ""
check("unset LOCAL_ADSB_URL -> no local fetch", m._local_aircraft() == [])

print("\nempty-layer watch:")
now = time.time()
m.WATCH_STATE = "/tmp/argus-test-watch.json"
m._watch.update(groups={}, fails={}, alarms={})
m._watch_observe("power", b'{"features":[{"geometry":{"type":"Point"},"properties":{"src":"Evoenergy"}}]}')
check("groups counted incl. zero sibling",
      m._watch["groups"]["power/Evoenergy"]["count"] == 1 and m._watch["groups"]["power/Essential Energy"]["count"] == 0)
check("never-seen group isn't judged", (m._watch_evaluate() or True) and not m._watch["alarms"])
m._watch["groups"]["power/Evoenergy"].update(count=0, last_nonzero=now - 40 * 60, max_gap=5 * 60)
m._watch_evaluate()
check("empty 40 min, normal gap 5 min -> alarm", "empty:power/Evoenergy" in m._watch["alarms"])
m._watch["groups"]["power/Evoenergy"].update(max_gap=4 * 3600)
m._watch_evaluate()
check("same gap but normal nights run 4 h -> no alarm", "empty:power/Evoenergy" not in m._watch["alarms"])
m.watch_fail("esa", OSError("HTTP 404"))
m._watch["fails"]["esa"]["first"] = now - 2 * 3600
m._watch_evaluate()
check("failing 2 h -> alarm", "fail:esa" in m._watch["alarms"])
m._watch["fails"]["esa"]["last"] = now - 3600
m._watch_evaluate()
check("nobody retried for 1 h -> stale, dropped", "fail:esa" not in m._watch["alarms"])
check("quiet feeds not watched", m._watch_groups("rfs", b'{"features":[]}') == {})
m._watch_save(); m._watch["groups"].clear(); m._watch_load()
check("last-seen persists across restart", "power/Evoenergy" in m._watch["groups"])

print("\n%d failed" % len(fails))
sys.exit(1 if fails else 0)
