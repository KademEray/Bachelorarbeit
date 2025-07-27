# compute_stats.py

import re
from pathlib import Path

import pandas as pd

# ─── Einstellungen ─────────────────────────────────────────────────────────
RES_DIR   = Path("results")
PATTERN   = "*_results.csv"
OUT_FILE  = RES_DIR / "constellation_stats.csv"
METRICS   = ["duration_ms", "server_ms", "avg_cpu", "avg_mem", "disk_mb"]

# ─── Helper zum Laden jeder CSV und Extrahieren von 'users' ────────────────
def load_csv(path: Path) -> pd.DataFrame:
    m = re.match(r"(\d+)_", path.name)
    users = int(m.group(1)) if m else -1
    df = pd.read_csv(path)
    df["users"] = users
    return df

# ─── 1. Alle CSVs laden, Warm-up raus, Variante zusammenbauen ─────────────
frames = []
for file in RES_DIR.glob(PATTERN):
    df = load_csv(file)
    df = df[df["phase"] == "steady"]               # keine Warm-ups
    df["variant"] = df["db"] + "_" + df["mode"]
    frames.append(df)

df = pd.concat(frames, ignore_index=True)

# ─── 2. Gruppieren und Metriken berechnen ────────────────────────────────
stats = (
    df
    .groupby(["users", "concurrency", "variant"], as_index=False)[METRICS]
    .agg(["mean", "std", "var"])
)

# ─── 3. Spalten flattenen und in CSV schreiben ───────────────────────────
stats.columns = [
    f"{metric}_{stat}"
    for metric, stat in stats.columns.to_flat_index()
]
stats.to_csv(OUT_FILE, index=False)
print(f"💾 Stats gespeichert → {OUT_FILE}")