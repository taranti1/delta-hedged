import sys, glob, orjson
sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
import zstandard as zstd, io
rows = []
for f in sorted(glob.glob("/Users/thomast/Desktop/delta-hedged/data/raw/clock/2026-09-25/*.jsonl.zst")):
    with open(f, "rb") as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.splitlines():
        try:
            r = orjson.loads(line)
        except Exception:
            continue
        raw = r.get("d") if isinstance(r, dict) else None
        rec = raw if isinstance(raw, dict) else (orjson.loads(raw) if isinstance(raw, (str, bytes)) else r)
        if isinstance(rec, dict) and "wall_ns" in rec and "mono_ns" in rec:
            rows.append((rec["wall_ns"], rec["mono_ns"], rec.get("offset_s"), rec.get("src")))
rows.sort()
print(len(rows), "samples")
if rows:
    w0, m0 = rows[0][0], rows[0][1]
    # segment by process restarts (mono jumps backwards)
    seg = [rows[0]]
    for r in rows[1:]:
        if r[1] < seg[-1][1]:
            break
        seg.append(r)
    for r in seg[:: max(1, len(seg)//12)] + [seg[-1]]:
        el = (r[0]-w0)/1e9
        drift_ms = ((r[0]-w0)-(r[1]-m0))/1e6
        print(f"elapsed {el:8.0f}s  wall-mono drift {drift_ms:+8.2f} ms  sntp offset {r[2]}  src {r[3]}")
print("--- anchored-clock error vs true time = (wall-mono drift) + sntp offset")
pts = [((r[0]-w0)/1e9, ((r[0]-w0)-(r[1]-m0))/1e6 + (r[2] or 0)*1000) for r in seg if r[2] is not None]
for el, e in pts[:: max(1, len(pts)//10)] + [pts[-1]]:
    print(f"elapsed {el:8.0f}s  error {e:+8.2f} ms")
import statistics
n=len(pts); xs=[p[0] for p in pts]; ys=[p[1] for p in pts]
mx, my = statistics.mean(xs), statistics.mean(ys)
b = sum((x-mx)*(y-my) for x,y in pts)/sum((x-mx)**2 for x in xs)
print(f"slope {b*3600:.2f} ms/hour = {b*1e3:.2f} ppm; intercept {my-b*mx:.1f} ms; hours to 250 ms: {(250-(my-b*mx))/(b*3600):.1f}")
