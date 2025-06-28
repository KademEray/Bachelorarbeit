# analyse.py
# ---------------------------------------------------------------------------
# Auswertung der Benchmark-CSVs – Gesamt-Ø  +  Ø getrennt nach User-Zahl
# ---------------------------------------------------------------------------
from pathlib import Path
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# ───────────────────────────────── Einstellungen ────────────────────────────
RES_DIR   = Path("results")
PLOT_DIR  = Path("plots")
PLOT_DIR.mkdir(exist_ok=True)

CONCURRENCY = [1, 3, 5, 10]
QUERY_IDS   = list(range(1, 25))

COLOR_CMAP  = plt.get_cmap("Blues")    # für die 4 Linien pro Diagramm
plt.rcParams.update({"figure.autolayout": True,
                     "figure.dpi":        110})

# ────────────────────────────── Helper --------------------------------------
def savefig(name: str):
    path = PLOT_DIR / f"{name}.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    print(f"💾  plots/{path.name}")

def load_with_users(csv_path: Path) -> pd.DataFrame:
    """
    CSV lesen und Spalte 'users' aus dem Dateinamen herausziehen.
    """
    m = re.match(r"(\d+)_", csv_path.name)
    users = int(m.group(1)) if m else -1
    df   = pd.read_csv(csv_path)
    df["users"] = users
    return df

# ────────────────────────────── CSV einlesen --------------------------------
csv_files = list(RES_DIR.glob("*_results.csv"))
if not csv_files:
    raise SystemExit("⚠️  Keine *_results.csv im Ordner 'results/' gefunden!")

print("Gefundene Dateien:", ", ".join(f.name for f in csv_files))

df_raw = (pd.concat(load_with_users(f) for f in csv_files)
            .query("phase == 'steady'")                  # Warm-ups raus
            .assign(variant=lambda d: d["db"] + "_" + d["mode"],
                    query_no=lambda d: d["query_no"].astype(int))
            .reset_index(drop=True))

# ────────────── Pivot-Tabelle (Ø über repeats / rounds / users) ──────────────
pivot_all = (
    df_raw.groupby(["variant", "concurrency", "query_no"])
          .agg(duration_ms=("per_query_ms", "mean"),
               avg_cpu     =("avg_cpu",      "mean"),
               avg_mem     =("avg_mem",      "mean"))
          .reset_index())

# ────────────── Pivot pro User-Größe  (100, 1000 …)  ─────────────────────────
pivot_by_user = (
    df_raw.groupby(["users", "variant", "concurrency", "query_no"])
          .agg(duration_ms=("per_query_ms", "mean"),
               avg_cpu     =("avg_cpu",      "mean"),
               avg_mem     =("avg_mem",      "mean"))
          .reset_index())

# ─────────────────────────── Zeichen-Funktionen ─────────────────────────────
def line_plots(source: pd.DataFrame, tag: str):
    """
    Erstellt für jede Variante ein Liniendiagramm (4 Linien = Concurrency),
    einmal für Duration, CPU und RAM.
    `tag` kommt als Suffix in den Dateinamen, z. B. "_all" oder "_u100".
    """
    metrics = [
        ("duration_ms", "Average Duration (ms)", "A_duration"),
        ("avg_cpu",     "Average CPU (%)",       "B_cpu"),
        ("avg_mem",     "Average RAM (MB)",      "C_ram")
    ]

    for metric, ylabel, prefix in metrics:
        for variant, g_var in source.groupby("variant"):
            fig, ax = plt.subplots(figsize=(10, 4))
            for i, conc in enumerate(CONCURRENCY):
                g = (g_var[g_var["concurrency"] == conc]
                        .set_index("query_no")
                        .reindex(QUERY_IDS)[metric])
                ax.plot(QUERY_IDS, g,
                        marker="o",
                        color=COLOR_CMAP(0.35 + i*0.15),
                        label=f"{conc} Threads")

            ax.set_title(f"{variant} – {ylabel} je Query")
            ax.set_xlabel("Query-ID")
            ax.set_ylabel(ylabel)
            ax.set_xticks(QUERY_IDS)
            ax.xaxis.set_major_locator(MaxNLocator(integer=True))
            ax.yaxis.grid(True, linestyle=":", alpha=.6)
            ax.legend(title="Concurrency")
            savefig(f"{prefix}{tag}_{variant}")
            plt.close(fig)

def grouped_bars(source: pd.DataFrame, tag: str):
    """
    Erzeugt für jede Concurrency ein Balkendiagramm:
    x = Query-ID, 4 Balken = Varianten, y = Duration.
    """
    bar_w   = 0.18
    x_pos   = np.arange(len(QUERY_IDS))

    for conc in CONCURRENCY:
        fig, ax = plt.subplots(figsize=(12, 4))
        for j, variant in enumerate(sorted(source["variant"].unique())):
            y = (source.query("concurrency == @conc & variant == @variant")
                        .set_index("query_no")
                        .reindex(QUERY_IDS)["duration_ms"]
                        .values)
            ax.bar(x_pos + (j-1.5)*bar_w, y,
                   width=bar_w,
                   label=variant)

        ax.set_title(f"Concurrency {conc} – Average Duration per Query")
        ax.set_xlabel("Query-ID")
        ax.set_ylabel("Average Duration (ms)")
        ax.set_xticks(x_pos, QUERY_IDS)
        ax.yaxis.grid(True, linestyle=":", alpha=.6)
        ax.legend(fontsize=8, title="Variante")
        savefig(f"D_conc{conc}{tag}_duration_grouped")
        plt.close(fig)

# ───────────────────── Gesamtdurchschnitt (alle Users) ───────────────────────
print("\n▶  Plots für ALLE Runs zusammen")
line_plots(pivot_all, tag="_all")
grouped_bars(pivot_all, tag="_all")

# ───────────────────── Plots pro User-Größe (z. B. 100 / 1000) ──────────────
for users, g_user in pivot_by_user.groupby("users"):
    print(f"\n▶  Plots für User-Größe {users}")
    suffix = f"_u{users}"
    line_plots(g_user, tag=suffix)
    grouped_bars(g_user, tag=suffix)

print("\n✅  Fertig!  Alle Diagramme liegen jetzt im Ordner  plots/")
