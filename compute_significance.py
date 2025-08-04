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
OUT_P50_C    = RES_DIR / "p50_by_complexity.csv"

# Ausgabedateien für Query
OUT_CI_Q      = RES_DIR / "ci_duration_by_query.csv"
OUT_P99_Q     = RES_DIR / "p99_by_query.csv"
OUT_SIG_Q     = RES_DIR / "significance_by_query.csv"
OUT_P50_Q    = RES_DIR / "p50_by_query.csv"

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

def cohen_d(sample1:pd.Series, sample2:pd.Series)->float:
    n1, n2  = len(sample1), len(sample2)
    if n1 < 2 or n2 < 2:
        return np.nan
    s1, s2  = sample1.std(ddof=1), sample2.std(ddof=1)
    # gepoolte Varianz
    s_pooled = np.sqrt(((n1-1)*s1**2 + (n2-1)*s2**2)/(n1+n2-2))
    if s_pooled == 0:
        return np.nan
    return (sample1.mean() - sample2.mean()) / s_pooled

def compute_percentile(df:pd.DataFrame, group_cols:list,
                       p:int, out_path:Path, label:str):
    rows=[]
    for key,g in df.groupby(group_cols,observed=True):
        vals=g["duration_ms"].dropna()
        if vals.empty: continue
        rows.append(dict(zip(group_cols,key))|{label:np.percentile(vals,p)})
    pd.DataFrame(rows).sort_values(group_cols,ignore_index=True)\
        .to_csv(out_path, index=False)
    print(f"💾 {label} → {out_path}")

def compute_significance(df:pd.DataFrame, group_cols:list, out_path:Path):
    rows=[]
    for key,grp in df.groupby(group_cols,observed=True):
        for v1,v2 in itertools.combinations(variants,2):
            d1 = grp.loc[grp["variant"]==v1,"duration_ms"].dropna()
            d2 = grp.loc[grp["variant"]==v2,"duration_ms"].dropna()
            if len(d1)<2 or len(d2)<2: continue
            stat,p = stats.ttest_ind(d1,d2,equal_var=False)
            rows.append(dict(zip(group_cols,key))|{
                "variant_1":v1,"variant_2":v2,
                "t_stat":stat,"p_value":p,
                "significant":p<ALPHA,
                "cohen_d":cohen_d(d1,d2)        #  ⇦  NEU
            })
    pd.DataFrame(rows).sort_values(group_cols+["variant_1","variant_2"],
                                   ignore_index=True)\
      .to_csv(out_path,index=False)
    print(f"💾 significance → {out_path}")

def compute_ci(df: pd.DataFrame, group_cols: list, out_path: Path):
    rows = []
    for key, g in df.groupby(group_cols, observed=True):
        data = g["duration_ms"].dropna()
        n = len(data)
        if n < 2:
            continue
        m, s = data.mean(), data.std(ddof=1)
        se   = s / np.sqrt(n)
        t    = stats.t.ppf(1 - ALPHA/2, df=n-1)
        rows.append(dict(zip(group_cols, key)) | {
            "n": n, "mean": m, "std": s,
            "ci_lower": m - t * se,
            "ci_upper": m + t * se
        })
    (pd.DataFrame(rows)
       .sort_values(group_cols, ignore_index=True)
       .to_csv(out_path, index=False))
    print(f"💾 CI  → {out_path}")


# Helper-Calls (ersetzt die Hand-Schleifen)
compute_ci(df, ["users","concurrency","variant","complexity"], OUT_CI_C)
compute_ci(df, ["users","concurrency","variant","query_no"],  OUT_CI_Q)

compute_percentile(df, ["users","concurrency","variant","complexity"], 99, OUT_P99_C, "p99_duration")
compute_percentile(df, ["users","concurrency","variant","complexity"], 50, OUT_P50_C, "p50_duration")
compute_percentile(df, ["users","concurrency","variant","query_no"],  99, OUT_P99_Q, "p99_duration")
compute_percentile(df, ["users","concurrency","variant","query_no"],  50, OUT_P50_Q, "p50_duration")

compute_significance(df, ["users","concurrency","complexity"], OUT_SIG_C)
compute_significance(df, ["users","concurrency","query_no"],   OUT_SIG_Q)
