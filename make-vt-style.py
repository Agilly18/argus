#!/usr/bin/env python3
"""Build vt-style.json — the Tokyo-Night vector basemap — from OpenFreeMap's
"dark" style.

OFM's dark style is pure greyscale, so the recolour is a luminance ramp: every
colour keeps its lightness role but lands on the Tokyo-Night blue scale.
Re-run when OFM changes their style (rare); output is committed.

  python3 make-vt-style.py          # writes vt-style.json next to this file
"""
import json
import re
import urllib.request
from pathlib import Path

SRC = "https://tiles.openfreemap.org/styles/dark"
OUT = Path(__file__).with_name("vt-style.json")

# luminance (0-1) -> Tokyo-Night anchor; interpolated in RGB between stops
RAMP = [
    (0.00, (0x16, 0x16, 0x1e)),
    (0.05, (0x1a, 0x1b, 0x26)),
    (0.10, (0x1f, 0x23, 0x35)),
    (0.20, (0x24, 0x28, 0x3b)),
    (0.40, (0x41, 0x48, 0x68)),
    (0.60, (0x56, 0x5f, 0x89)),
    (0.80, (0x78, 0x7c, 0x99)),
    (1.00, (0xa9, 0xb1, 0xd6)),
]
# per-layer overrides applied after the ramp (id without the vt- prefix)
LAYER_TINT = {
    "water": "#1c2333",             # water reads blue, not grey
    "waterway": "#1c2333",
    "landuse_park": "#1e232b",      # parks barely-there green-blue
    "landcover_wood": "#1d222a",
}
# carto's attribution string carries every feed credit — keep it alive when
# the raster basemaps are hidden by crediting the vector source the same way
ATTRIBUTION = ("© OpenStreetMap © OpenFreeMap · aircraft: airplanes.live · "
               "wx: Open-Meteo · radar: RainViewer · outages: Evoenergy / "
               "Essential Energy · transit: Transport Canberra · air/roads: "
               "ACT Gov · quakes: Geoscience Australia · terrain: Mapzen/AWS")


def _parse(c):
    """colour string -> (r, g, b, a) or None if unparseable"""
    c = c.strip()
    m = re.fullmatch(r"#([0-9a-fA-F]{3})", c)
    if m:
        return tuple(int(x * 2, 16) for x in m.group(1)) + (1.0,)
    m = re.fullmatch(r"#([0-9a-fA-F]{6})", c)
    if m:
        h = m.group(1)
        return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4)) + (1.0,)
    m = re.fullmatch(r"rgba?\(([^)]*)\)", c)
    if m:
        p = [float(x) for x in m.group(1).replace(" ", "").split(",")]
        return (p[0], p[1], p[2], p[3] if len(p) > 3 else 1.0)
    m = re.fullmatch(r"hsla?\(([^)]*)\)", c)
    if m:
        p = m.group(1).replace(" ", "").replace("%", "").split(",")
        h, s, l = float(p[0]), float(p[1]) / 100, float(p[2]) / 100
        a = float(p[3]) if len(p) > 3 else 1.0
        # hsl -> rgb (s≈0 in this style, but do it properly anyway)
        cc = (1 - abs(2 * l - 1)) * s
        x = cc * (1 - abs((h / 60) % 2 - 1))
        mm = l - cc / 2
        r, g, b = [(cc, x, 0), (x, cc, 0), (0, cc, x),
                   (0, x, cc), (x, 0, cc), (cc, 0, x)][int(h // 60) % 6]
        return ((r + mm) * 255, (g + mm) * 255, (b + mm) * 255, a)
    return None


def _ramp(lum):
    for (l0, c0), (l1, c1) in zip(RAMP, RAMP[1:]):
        if lum <= l1:
            t = 0 if l1 == l0 else (lum - l0) / (l1 - l0)
            return tuple(round(a + (b - a) * t) for a, b in zip(c0, c1))
    return RAMP[-1][1]


def recolour(c):
    p = _parse(c)
    if p is None:
        return c
    r, g, b, a = p
    lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255
    nr, ng, nb = _ramp(lum)
    if a >= 1.0:
        return f"#{nr:02x}{ng:02x}{nb:02x}"
    return f"rgba({nr},{ng},{nb},{round(a, 2)})"


def walk(o):
    if isinstance(o, dict):
        return {k: walk(v) for k, v in o.items()}
    if isinstance(o, list):
        return [walk(v) for v in o]
    if isinstance(o, str) and (o.startswith(("#", "rgb", "hsl"))):
        return recolour(o)
    return o


def main():
    # OFM 403s urllib's default UA (same story as Essential Energy's KML)
    req = urllib.request.Request(SRC, headers={"User-Agent": "argus/0.05"})
    style = json.loads(urllib.request.urlopen(req, timeout=20).read())
    out = {"sources": style["sources"], "layers": []}
    out["sources"].get("openmaptiles", {})["attribution"] = ATTRIBUTION
    for lyr in style["layers"]:
        lay = lyr.get("layout", {})
        # oneway arrows are pure sprite icons — drop; city/town dots keep
        # their text but lose the icon so no sprite sheet is needed
        if "icon-image" in lay:
            if not lay.get("text-field"):
                continue
            lay.pop("icon-image", None)
        # fill-pattern also points at the sprite sheet — plain fill instead
        lyr.get("paint", {}).pop("fill-pattern", None)
        # OFM ships Noto Sans, but fonts.openmaptiles.org's Noto glyph PBFs
        # fail MapLibre 5's stricter parser (Open Sans parses fine, and it's
        # what every Argus layer already uses) — unify on it
        if "text-font" in lay:
            lay["text-font"] = ["Open Sans Regular"]
        lyr = walk(lyr)
        for lid, col in LAYER_TINT.items():
            if lyr["id"] == lid:
                for k in ("fill-color", "line-color"):
                    if k in lyr.get("paint", {}):
                        lyr["paint"][k] = col
        lyr["id"] = "vt-" + lyr["id"]
        out["layers"].append(lyr)
    OUT.write_text(json.dumps(out, indent=1))
    print(f"wrote {OUT} — {len(out['layers'])} layers, "
          f"sources: {list(out['sources'])}")


if __name__ == "__main__":
    main()
