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
RES_DIR   = Path("results")     # Ordner mit den Ergebnis-CSV-Dateien
PLOT_DIR  = Path("plots")       # Ordner für die Ausgabegrafiken
PLOT_DIR.mkdir(exist_ok=True)  # Ordner erstellen, falls nicht vorhanden

CONCURRENCY = [1, 3, 5, 10]     # Anzahl gleichzeitiger Nutzer in den Tests
QUERY_IDS   = list(range(1, 25))# IDs der Abfragen (1–24)

# Farbpalette für Liniengrafiken (z. B. für 4 Kurven pro Diagramm)
COLOR_CMAP  = plt.get_cmap("Blues")

# Matplotlib-Standardwerte anpassen (z. B. für hohe Auflösung)
plt.rcParams.update({
    "figure.autolayout": True,  # automatischer Abstand von Elementen
    "figure.dpi":        1000    # hohe Auflösung für Druck/Export
})


# ────────────────────────────── Helper --------------------------------------
def savefig(name: str):
    """
    Speichert das aktuelle Diagramm als PNG-Datei im PLOT_DIR-Ordner.
    
    Parameter:
    - name (str): Dateiname (ohne Erweiterung)
    """
    path = PLOT_DIR / f"{name}.png"
    plt.savefig(path, dpi=600, bbox_inches="tight")
    print(f"💾  plots/{path.name}")  # Hinweis in der Konsole


def load_with_users(csv_path: Path) -> pd.DataFrame:
    """
    Liest eine Benchmark-CSV-Datei ein und extrahiert die Anzahl der Benutzer
    aus dem Dateinamen (z. B. '10_results.csv' → users = 10).
    
    Parameter:
    - csv_path (Path): Pfad zur CSV-Datei
    
    Rückgabe:
    - pd.DataFrame: eingelesene Daten mit zusätzlicher 'users'-Spalte
    """
    m = re.match(r"(\d+)_", csv_path.name)
    users = int(m.group(1)) if m else -1  # Fallback: -1, falls keine Zahl gefunden
    df = pd.read_csv(csv_path)
    df["users"] = users
    return df


# ────────────────────────────── CSV einlesen --------------------------------
csv_files = list(RES_DIR.glob("*_results.csv"))
if not csv_files:
    raise SystemExit("⚠️  Keine *_results.csv im Ordner 'results/' gefunden!")

print("Gefundene Dateien:", ", ".join(f.name for f in csv_files))

df_raw = (
    pd.concat(load_with_users(f) for f in csv_files)
      .query("phase == 'steady'")  # Entfernt Messdaten aus der Aufwärmphase
      .assign(
          variant=lambda d: d["db"] + "_" + d["mode"],  # Kombination aus DB + Modus
          query_no=lambda d: d["query_no"].astype(int)  # Query-Nr. als int (für Sortierung)
      )
      .reset_index(drop=True)
)


# ────────────── Pivot-Tabelle (Ø über repeats / rounds / users) ──────────────
pivot_all = (
    df_raw.groupby(["variant", "concurrency", "query_no"])
          .agg(duration_ms=("duration_ms", "mean"),   # Durchschnittliche Dauer pro Query
               avg_cpu     =("avg_cpu",      "mean"),   # Durchschnittliche CPU-Auslastung
               avg_mem     =("avg_mem",      "mean"))   # Durchschnittlicher Speicherverbrauch
          .reset_index()
)


# ────────────── Pivot pro User-Größe  (100, 1000 …)  ─────────────────────────
pivot_by_user = (
    df_raw.groupby(["users", "variant", "concurrency", "query_no"])
          .agg(duration_ms=("duration_ms", "mean"),   # Ø pro Benutzergruppe
               avg_cpu     =("avg_cpu",      "mean"),
               avg_mem     =("avg_mem",      "mean"))
          .reset_index()
)


# ─────────────────────────── Zeichen-Funktionen ─────────────────────────────
def line_plots(source: pd.DataFrame, tag: str):
    """
    Erstellt für jede Datenbank-Variante ein Liniendiagramm (eine Linie pro Concurrency-Stufe),
    und zwar für:
        - Ausführungsdauer (ms)
        - CPU-Auslastung (%)
        - RAM-Verbrauch (MB)
    
    Der Parameter `tag` bestimmt das Suffix des Dateinamens (z. B. "_all" oder "_u100").
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


def grouped_bars(source: pd.DataFrame, tag: str) -> None:
    """
    Gruppierte Balkendiagramme:
      – je Concurrency-Stufe ein Plot
      – Balken gruppiert nach Query-ID, je Variante ein Balken
      – automatische Entscheidung:
            * symlog-y-Achse   (wenn Range ≥ 20×)
            * sonst Bildhöhe   (wenn Range ≥ 5×)
    """
    bar_w     = 0.18
    x_pos     = np.arange(len(QUERY_IDS))
    variants  = sorted(source["variant"].unique())

    for conc in CONCURRENCY:
        # ---------- Datenmatrix bauen -----------------------------------
        y = {}
        for v in variants:
            mask = (source["concurrency"] == conc) & (source["variant"] == v)
            y[v] = (source.loc[mask]
                          .set_index("query_no")
                          .reindex(QUERY_IDS)["duration_ms"]
                          .to_numpy())
        y_all       = np.concatenate(list(y.values()))
        global_min  = np.nanmin(y_all)
        global_max  = np.nanmax(y_all)
        ratio       = global_max / max(global_min, 1e-9)

        # ---------- Layout bestimmen ------------------------------------
        fig_h   = 4
        use_log = ratio >= 20
        if use_log:
            yscale = ("symlog", {"linthresh": global_min * 2})
        elif ratio >= 5:
            fig_h += 0.6 * np.log10(ratio)      # Höhe strecken
            yscale = ("linear", {})
        else:
            yscale = ("linear", {})

        fig, ax = plt.subplots(figsize=(12, fig_h))

        # ---------- Balken zeichnen -------------------------------------
        offset = -(len(variants)-1)/2 * bar_w
        for j, v in enumerate(variants):
            ax.bar(x_pos + offset + j*bar_w, y[v],
                   width=bar_w, label=v)

        # ---------- Achsen & Styling ------------------------------------
        name, kw = yscale
        ax.set_yscale(name, **kw)
        ax.set_xlabel("Query-ID")
        ax.set_ylabel("Average Duration (ms)")
        ax.set_title(f"Concurrency {conc} – Avg. Duration per Query")
        ax.set_xticks(x_pos, QUERY_IDS)
        ax.yaxis.grid(True, linestyle=":", alpha=.6, which="both")
        ax.legend(title="Variante", fontsize=8)

        savefig(f"D_conc{conc}{tag}_duration_grouped")
        plt.close(fig)


# ─────────────────────────  NEUER PLOT  ────────────────────────────────────
def bars_conc_variant(df: pd.DataFrame, *, all_users: bool = False) -> None:
    """
    Balkendiagramm(e) Ø-Duration_ms  vs.  Concurrency  &  Variante.

    Parameters
    ----------
    df         : DataFrame   –  muss Spalten  users, concurrency, variant, duration_ms enthalten
    all_users  : bool        –  True  →  Daten aller User zusammengenommen (ein einziger Plot)
                               False →  es wird für jede User-Größe (df['users'].unique()) ein Plot erstellt
    """
    # Mittelwert über alle Query-IDs bilden
    base = (
        df.groupby(["users", "concurrency", "variant"], as_index=False)["duration_ms"]
          .mean()
    )

    var_order = sorted(base["variant"].unique())
    cmap      = plt.get_cmap("tab10")
    colors    = {v: cmap(i) for i, v in enumerate(var_order)}
    bar_w     = 0.18
    x_pos     = np.arange(len(CONCURRENCY))

    # Helper, um genau EIN Balkendiagramm zu zeichnen
    def _draw(ax, g, title_suffix, filename_suffix):
        offset = -(len(var_order)-1)/2 * bar_w
        for j, v in enumerate(var_order):
            ys = (
                g[g["variant"] == v]
                  .set_index("concurrency")
                  .reindex(CONCURRENCY)["duration_ms"]
                  .to_numpy()
            )
            ax.bar(x_pos + offset + j*bar_w,
                   ys,
                   width=bar_w,
                   color=colors[v],
                   label=v)
            # Balkenbeschriftung
            for xp, val in zip(x_pos + offset + j*bar_w, ys):
                ax.text(xp, val, f"{val:.0f}",
                        ha="center", va="bottom",
                        fontsize=6, rotation=90)

        ax.set_title(f"Average Duration – {title_suffix}")
        ax.set_xlabel("Concurrency (Threads)")
        ax.set_ylabel("Average Duration (ms)")
        ax.set_xticks(x_pos, [str(c) for c in CONCURRENCY])
        ax.yaxis.grid(True, linestyle=":", alpha=.6)
        ax.legend(title="Variante", fontsize=8)
        savefig(f"E_users{filename_suffix}_conc_vs_variant")
        plt.close(ax.figure)

    # -------- alle User zusammen --------
    if all_users:
        g = (
            base.groupby(["concurrency", "variant"], as_index=False)["duration_ms"]
                .mean()
        )
        fig, ax = plt.subplots(figsize=(8, 4))
        _draw(ax, g.assign(users="ALL"), "ALL Users", "ALL")
        return

    # -------- getrennt nach User-Größe --------
    for users, g in base.groupby("users"):
        fig, ax = plt.subplots(figsize=(8, 4))
        _draw(ax, g, f"{users} Users", users)


# ─────────────────────────  SUMMARY → CSV  ────────────────────────────
def export_summary_csv(
    df: pd.DataFrame,
    out_dir: Path = Path("results"),
    decimals: int = 1,
) -> None:
    """
    • summary_table.csv   – Ø-Werte (plus Gesamtzeile 'ALL')
    • per_query_table.csv – dieselben Metriken pro Query
      Reihenfolge: Duration → CPU → RAM → Disk
    """
    out_dir.mkdir(exist_ok=True)

    # ───────── feste Reihenfolgen ─────────
    METRIC_ORDER  = ["duration_ms", "avg_cpu", "avg_mem", "disk_mb"]
    VARIANT_ORDER = [
        "postgres_normal",
        "postgres_optimized",
        "neo4j_normal",
        "neo4j_optimized",
    ]

    # Variantenspalte in geordnete Kategorie umwandeln  ⟶   Pivot hält die Reihenfolge
    df = df.copy()
    df["variant"] = pd.Categorical(df["variant"],
                                   categories=VARIANT_ORDER,
                                   ordered=True)

    # --------------------------------------------------
    # 1️⃣  SUMMARY  (Ø über alle Queries & Wiederholungen)
    # --------------------------------------------------
    summary = (
        df.groupby(["users", "concurrency", "variant"],
                   observed=True)
          .agg({m: ("mean") for m in METRIC_ORDER})
          .round(decimals)
          .pivot_table(index   = ["users", "concurrency"],
                       columns = "variant",
                       values  = METRIC_ORDER,
                       sort=False,      # ⇦ behält METRIC_ORDER & VARIANT_ORDER
                       observed=True)              
    )

    # Spaltennamen flatten:  duration_ms_postgres_normal …
    summary.columns = [f"{m}_{v}" for m, v in summary.columns.to_flat_index()]
    summary = summary.reset_index()

    # Gesamtzeile 'ALL'
    overall = (summary.drop(columns=["users", "concurrency"])
                      .mean(numeric_only=True)
                      .to_frame().T
                      .round(decimals))
    overall.insert(0, "concurrency", "")
    overall.insert(0, "users", "ALL")
    summary = pd.concat([summary, overall], ignore_index=True)

    summary.to_csv(out_dir / "summary_table.csv", index=False)
    print(f"💾 summary_table.csv geschrieben → {out_dir}")

    # --------------------------------------------------
    # 2️⃣  PER-QUERY-TABELLE  (Ausreißer sichtbar)
    # --------------------------------------------------
    per_q = (
        df.groupby(["users", "concurrency", "variant", "query_no"],
                   observed=True)
          .agg({m: ("mean") for m in METRIC_ORDER})
          .round(decimals)
          .pivot_table(index   = ["users", "concurrency", "query_no"],
                       columns = "variant",
                       values  = METRIC_ORDER,
                       sort=False,
                       observed=True)
    )
    per_q.columns = [f"{m}_{v}" for m, v in per_q.columns.to_flat_index()]
    per_q = per_q.reset_index()

    per_q.to_csv(out_dir / "per_query_table.csv", index=False)
    print(f"💾 per_query_table.csv geschrieben → {out_dir}")

# ───────────────────── Gesamtdurchschnitt (alle Users) ───────────────────────
print("\n▶  Plots für ALLE Runs zusammen")
line_plots(pivot_all, tag="_all")
grouped_bars(pivot_all, tag="_all")
bars_conc_variant(df_raw, all_users=True)


# ───────────────────── Plots pro User-Größe (z. B. 100 / 1000 / 10000) ───────
for users, g_user in pivot_by_user.groupby("users"):
    print(f"\n▶  Plots für User-Größe {users}")
    suffix = f"_u{users}"
    line_plots(g_user, tag=suffix)
    grouped_bars(g_user, tag=suffix)
    bars_conc_variant(g_user)

export_summary_csv(df_raw)   # schreibt results/summary_table.csv

print("\n✅  Fertig!  Alle Diagramme liegen jetzt im Ordner  plots/")
