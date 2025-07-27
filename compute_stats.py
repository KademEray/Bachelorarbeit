# compute_stats.py

import re
from pathlib import Path

import pandas as pd

# ─── Einstellungen ─────────────────────────────────────────────────────────
RES_DIR    = Path("results")
PATTERN    = "*_results.csv"
OUT_CONS   = RES_DIR / "constellation_stats.csv"
OUT_COMP   = RES_DIR / "complexity_stats.csv"
METRICS    = ["duration_ms", "avg_cpu", "avg_mem"]
COMPLEXITY_ORDER = ["easy", "medium", "complex", "very_complex", "create", "update", "delete"]

# ─── Complexity-Mapping ────────────────────────────────────────────────────
def map_complexity(q: int) -> str:
    if   1  <= q <=  3:  return "easy"
    if   4  <= q <=  6:  return "medium"
    if   7  <= q <=  9:  return "complex"
    if  10 <= q <= 12:  return "very_complex"
    if  13 <= q <= 16:  return "create"
    if  17 <= q <= 20:  return "update"
    return "delete"

# ─── CSV laden und Grunddaten aufbereiten ──────────────────────────────────
def load_csv(path: Path) -> pd.DataFrame:
    users = int(re.match(r"(\d+)_", path.name).group(1))
    df = pd.read_csv(path)
    df = df[df["phase"] == "steady"]
    df["users"]      = users
    df["variant"]    = df["db"] + "_" + df["mode"]
    df["complexity"] = pd.Categorical(
        df["query_no"].astype(int).map(map_complexity),
        categories=COMPLEXITY_ORDER,
        ordered=True
    )
    return df

# ─── 1. Alle CSVs einlesen ─────────────────────────────────────────────────
frames = [load_csv(f) for f in RES_DIR.glob(PATTERN)]
df = pd.concat(frames, ignore_index=True)

# ─── 2. Stats pro Konstellation (users, concurrency, variant) ─────────────
cons_stats = (
    df
    .groupby(["users", "concurrency", "variant"], observed=True, as_index=False)[METRICS]
    .agg(["mean", "std", "var"])
)
# Spalten flachmachen: nur Metrik-MultiIndex-Einträge mit nicht-leerem Unterlabel
new_cols = []
for col in cons_stats.columns:
    if isinstance(col, tuple) and col[1]:
        new_cols.append(f"{col[0]}_{col[1]}")
    else:
        new_cols.append(col[0] if isinstance(col, tuple) else col)
cons_stats.columns = new_cols

cons_stats.to_csv(OUT_CONS, index=False)
print(f"💾 Konstellation-Stats → {OUT_CONS}")

# ─── 3. Stats pro Complexity (users, concurrency, variant, complexity) ─────
comp_stats = (
    df
    .groupby(["users", "concurrency", "variant", "complexity"], 
             observed=True, as_index=False)[METRICS]
    .agg(["mean", "std", "var"])
)
# Spalten flachmachen wie oben
new_cols = []
for col in comp_stats.columns:
    if isinstance(col, tuple) and col[1]:
        new_cols.append(f"{col[0]}_{col[1]}")
    else:
        new_cols.append(col[0] if isinstance(col, tuple) else col)
comp_stats.columns = new_cols

# Sortieren in definierter Reihenfolge
comp_stats["complexity"] = pd.Categorical(
    comp_stats["complexity"],
    categories=COMPLEXITY_ORDER,
    ordered=True
)
comp_stats = comp_stats.sort_values(
    ["users", "concurrency", "variant", "complexity"],
    ignore_index=True
)

comp_stats.to_csv(OUT_COMP, index=False)
print(f"💾 Complexity-Stats     → {OUT_COMP}")
