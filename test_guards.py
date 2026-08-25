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

print("\n%d failed" % len(fails))
sys.exit(1 if fails else 0)
