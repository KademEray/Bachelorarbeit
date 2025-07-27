# compute_significance.py

import re
from pathlib import Path
import itertools

import numpy as np
import pandas as pd
from scipy import stats

# ─── Einstellungen ─────────────────────────────────────────────────────────
RES_DIR     = Path("results")
PATTERN     = "*_results.csv"
OUT_CI      = RES_DIR / "ci_duration.csv"
OUT_SIG     = RES_DIR / "significance_duration.csv"
ALPHA       = 0.05  # 95% Konfidenz


# ─── CSV laden & Grunddaten aufbereiten ────────────────────────────────────
def load_raw(path: Path) -> pd.DataFrame:
    users = int(re.match(r"(\d+)_", path.name).group(1))
    df = pd.read_csv(path)
    df = df[df["phase"] == "steady"]
    df["users"]      = users
    df["variant"]    = df["db"] + "_" + df["mode"]
    return df

frames = [load_raw(f) for f in RES_DIR.glob(PATTERN)]
df = pd.concat(frames, ignore_index=True)


# ─── 1. Konfidenzintervalle für duration_ms je user/concurrency/variant ───
cis = []
group_cols = ["users", "concurrency", "variant"]
for (u, c, v), g in df.groupby(group_cols, observed=True):
    data = g["duration_ms"].dropna()
    n    = data.size
    if n < 2:
        continue
    mean = data.mean()
    std  = data.std(ddof=1)
    se   = std / np.sqrt(n)
    # t-Wert für 95% CI
    t_val = stats.t.ppf(1 - ALPHA/2, df=n-1)
    lo, hi = mean - t_val * se, mean + t_val * se
    cis.append({
        "users":       u,
        "concurrency": c,
        "variant":     v,
        "n":           n,
        "mean":        mean,
        "std":         std,
        "ci_lower":    lo,
        "ci_upper":    hi,
    })

ci_df = pd.DataFrame(cis)
ci_df.to_csv(OUT_CI, index=False)
print(f"💾  95 % CI → {OUT_CI}")


# ─── 2. Paarweise t-Tests zwischen Varianten ───────────────────────────────
# für jede User/Concurrency-Kombination alle Variant-Paare vergleichen
tests = []
variants = df["variant"].unique()
for (u, c), grp in df.groupby(["users", "concurrency"], observed=True):
    # für jedes Paar (v1, v2)
    for v1, v2 in itertools.combinations(variants, 2):
        d1 = grp.loc[grp["variant"]==v1, "duration_ms"].dropna()
        d2 = grp.loc[grp["variant"]==v2, "duration_ms"].dropna()
        if len(d1) < 2 or len(d2) < 2:
            continue
        stat, p = stats.ttest_ind(d1, d2, equal_var=False)
        tests.append({
            "users":       u,
            "concurrency": c,
            "variant_1":   v1,
            "variant_2":   v2,
            "t_stat":      stat,
            "p_value":     p,
            "significant": p < ALPHA
        })

sig_df = pd.DataFrame(tests)
sig_df.to_csv(OUT_SIG, index=False)
print(f"💾  Signifikanztests → {OUT_SIG}")
