# compute_significance.py

import re
from pathlib import Path
import itertools

import numpy as np
import pandas as pd
from scipy import stats

# ─── Einstellungen ─────────────────────────────────────────────────────────
RES_DIR       = Path("results")
PATTERN       = "*_results.csv"
ALPHA         = 0.05  # 95 % Konfidenz

# Ausgabedateien für Complexity
OUT_CI_C      = RES_DIR / "ci_duration_by_complexity.csv"
OUT_P99_C     = RES_DIR / "p99_by_complexity.csv"
OUT_SIG_C     = RES_DIR / "significance_by_complexity.csv"

# Ausgabedateien für Query
OUT_CI_Q      = RES_DIR / "ci_duration_by_query.csv"
OUT_P99_Q     = RES_DIR / "p99_by_query.csv"
OUT_SIG_Q     = RES_DIR / "significance_by_query.csv"

# Komplexitäts-Mapping
COMPLEX_MAP = {
    range(1,  4):   "easy",
    range(4,  7):   "medium",
    range(7, 10):   "complex",
    range(10,13):   "very_complex",
    range(13,17):   "create",
    range(17,21):   "update",
    range(21,25):   "delete",
}
COMPLEXITY_ORDER = ["easy","medium","complex","very_complex","create","update","delete"]

def map_complexity(q):
    q = int(q)
    for r,label in COMPLEX_MAP.items():
        if q in r:
            return label
    return "unknown"

# ─── CSV laden & aufbereiten ───────────────────────────────────────────────
def load_raw(path: Path) -> pd.DataFrame:
    users = int(re.match(r"(\d+)_", path.name).group(1))
    df = pd.read_csv(path)
    df = df[df["phase"] == "steady"]
    df["users"]      = users
    df["variant"]    = df["db"] + "_" + df["mode"]
    df["query_no"]   = df["query_no"].astype(int)
    df["complexity"] = df["query_no"].map(map_complexity).astype(
        pd.CategoricalDtype(COMPLEXITY_ORDER, ordered=True)
    )
    return df

# alle CSVs einlesen
frames = [load_raw(f) for f in RES_DIR.glob(PATTERN)]
df = pd.concat(frames, ignore_index=True)

variants = df["variant"].unique()

# ─── 1. CI by complexity ───────────────────────────────────────────────────
cis_c = []
for (u,c,v,comp), g in df.groupby(["users","concurrency","variant","complexity"], observed=True):
    data = g["duration_ms"].dropna()
    n = len(data)
    if n<2: continue
    m = data.mean(); s = data.std(ddof=1); se = s/np.sqrt(n)
    t = stats.t.ppf(1-ALPHA/2, df=n-1)
    cis_c.append({
        "users":u, "concurrency":c, "variant":v, "complexity":comp,
        "n":n, "mean":m, "std":s,
        "ci_lower":m-t*se, "ci_upper":m+t*se
    })
ci_c_df = pd.DataFrame(cis_c).sort_values(
    ["users","concurrency","variant","complexity"], ignore_index=True
)
ci_c_df.to_csv(OUT_CI_C, index=False)
print(f"💾 CI by complexity → {OUT_CI_C}")

# ─── 2. p99 by complexity ──────────────────────────────────────────────────
p99_c = []
for (u,c,v,comp), g in df.groupby(["users","concurrency","variant","complexity"], observed=True):
    vals = g["duration_ms"].dropna()
    if vals.empty: continue
    p99_c.append({
        "users":u, "concurrency":c, "variant":v, "complexity":comp,
        "p99_duration": np.percentile(vals,99)
    })
p99_c_df = pd.DataFrame(p99_c).sort_values(
    ["users","concurrency","variant","complexity"], ignore_index=True
)
p99_c_df.to_csv(OUT_P99_C, index=False)
print(f"💾 p99 by complexity → {OUT_P99_C}")

# ─── 3. Significance by complexity ────────────────────────────────────────
tests_c = []
for (u,c,comp), grp in df.groupby(["users","concurrency","complexity"], observed=True):
    for v1,v2 in itertools.combinations(variants,2):
        d1 = grp.loc[grp["variant"]==v1,"duration_ms"].dropna()
        d2 = grp.loc[grp["variant"]==v2,"duration_ms"].dropna()
        if len(d1)<2 or len(d2)<2: continue
        stat,p = stats.ttest_ind(d1,d2,equal_var=False)
        tests_c.append({
            "users":u, "concurrency":c, "complexity":comp,
            "variant_1":v1, "variant_2":v2,
            "t_stat":stat, "p_value":p, "significant": p<ALPHA
        })
sig_c_df = pd.DataFrame(tests_c).sort_values(
    ["users","concurrency","complexity","variant_1","variant_2"],
    ignore_index=True
)
sig_c_df.to_csv(OUT_SIG_C, index=False)
print(f"💾 significance by complexity → {OUT_SIG_C}")

# ──────────────────────────────────────────────────────────────────────────
# ─── 4. CI by query ───────────────────────────────────────────────────────
cis_q = []
for (u,c,v,qno), g in df.groupby(["users","concurrency","variant","query_no"], observed=True):
    data = g["duration_ms"].dropna()
    n = len(data)
    if n<2: continue
    m = data.mean(); s = data.std(ddof=1); se = s/np.sqrt(n)
    t = stats.t.ppf(1-ALPHA/2, df=n-1)
    cis_q.append({
        "users":u, "concurrency":c, "variant":v, "query_no":qno,
        "n":n, "mean":m, "std":s,
        "ci_lower":m-t*se, "ci_upper":m+t*se
    })
ci_q_df = pd.DataFrame(cis_q).sort_values(
    ["users","concurrency","variant","query_no"], ignore_index=True
)
ci_q_df.to_csv(OUT_CI_Q, index=False)
print(f"💾 CI by query → {OUT_CI_Q}")

# ─── 5. p99 by query ──────────────────────────────────────────────────────
p99_q = []
for (u,c,v,qno), g in df.groupby(["users","concurrency","variant","query_no"], observed=True):
    vals = g["duration_ms"].dropna()
    if vals.empty: continue
    p99_q.append({
        "users":u, "concurrency":c, "variant":v, "query_no":qno,
        "p99_duration": np.percentile(vals,99)
    })
p99_q_df = pd.DataFrame(p99_q).sort_values(
    ["users","concurrency","variant","query_no"], ignore_index=True
)
p99_q_df.to_csv(OUT_P99_Q, index=False)
print(f"💾 p99 by query → {OUT_P99_Q}")

# ─── 6. Significance by query ─────────────────────────────────────────────
tests_q = []
for (u,c,qno), grp in df.groupby(["users","concurrency","query_no"], observed=True):
    for v1,v2 in itertools.combinations(variants,2):
        d1 = grp.loc[grp["variant"]==v1,"duration_ms"].dropna()
        d2 = grp.loc[grp["variant"]==v2,"duration_ms"].dropna()
        if len(d1)<2 or len(d2)<2: continue
        stat,p = stats.ttest_ind(d1,d2,equal_var=False)
        tests_q.append({
            "users":u, "concurrency":c, "query_no":qno,
            "variant_1":v1, "variant_2":v2,
            "t_stat":stat, "p_value":p, "significant": p<ALPHA
        })
sig_q_df = pd.DataFrame(tests_q).sort_values(
    ["users","concurrency","query_no","variant_1","variant_2"],
    ignore_index=True
)
sig_q_df.to_csv(OUT_SIG_Q, index=False)
print(f"💾 significance by query → {OUT_SIG_Q}")
