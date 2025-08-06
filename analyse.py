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

# ─────────────────────────────── Helper: Datacheck ───────────────────────────
def _has_valid_values(arr_like) -> bool:
    """
    Liefert True, wenn das übergebene Array / Series
    mindestens einen finite(n) Zahlenwert enthält.
    """
    if isinstance(arr_like, pd.Series):
        arr_like = arr_like.to_numpy()
    return np.isfinite(arr_like).any()


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
               server_ms  =("server_ms",  "mean"),
               avg_cpu     =("avg_cpu",      "mean"),   # Durchschnittliche CPU-Auslastung
               avg_mem     =("avg_mem",      "mean"))   # Durchschnittlicher Speicherverbrauch
          .reset_index()
)


# ────────────── Pivot pro User-Größe  (100, 1000 …)  ─────────────────────────
pivot_by_user = (
    df_raw.groupby(["users", "variant", "concurrency", "query_no"])
          .agg(duration_ms=("duration_ms", "mean"),   # Ø pro Benutzergruppe
               server_ms  =("server_ms",  "mean"),
               avg_cpu     =("avg_cpu",      "mean"),
               avg_mem     =("avg_mem",      "mean"))
          .reset_index()
)


# ─────────────────────────── Zeichen-Funktionen ─────────────────────────────
def line_plots(source: pd.DataFrame, tag: str):
    metrics = [
        ("duration_ms", "Average Duration (ms)", "A_duration"),
        ("server_ms",   "Server Execution (ms)", "B_server"),
        ("avg_cpu",     "Average CPU (%)",       "C_cpu"),
        ("avg_mem",     "Average RAM (MB)",      "D_ram"),
    ]

    for metric, ylabel, prefix in metrics:
        for variant, g_var in source.groupby("variant"):
            fig, ax = plt.subplots(figsize=(10, 4))
            plotted = False

            for i, conc in enumerate(CONCURRENCY):
                g = (
                    g_var[g_var["concurrency"] == conc]
                      .set_index("query_no")
                      .reindex(QUERY_IDS)[metric]
                )
                if not _has_valid_values(g.to_numpy()):
                    continue  # keine Werte → nächste Linie

                valid_x = g.index[~g.isna()]
                ax.plot(
                    valid_x,
                    g.loc[valid_x],
                    marker="o",
                    color=COLOR_CMAP(0.35 + i * 0.15),
                    label=f"{conc} Threads",
                )
                plotted = True

            if not plotted:  # keine einzige Linie gezeichnet → Plot verwerfen
                plt.close(fig)
                print(f"⚠️  {metric}/{variant} – zu wenig Daten, Plot übersprungen")
                continue

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
    Gruppierte Balkendiagramme (je Concurrency ein Plot)
    """
    bar_w    = 0.18
    x_pos    = np.arange(len(QUERY_IDS))
    variants = sorted(source["variant"].unique())

    for conc in CONCURRENCY:
        # ---------- Datenmatrix bauen -----------------------------------
        y = {}
        for v in variants:
            mask = (source["concurrency"] == conc) & (source["variant"] == v)
            y[v] = (source.loc[mask]
                          .set_index("query_no")
                          .reindex(QUERY_IDS)["duration_ms"]
                          .to_numpy())
        y_all = np.concatenate(list(y.values()))
        if not _has_valid_values(y_all):
            print(f"⚠️  Concurrency {conc} – keine Daten, Plot übersprungen")
            continue

        global_min = np.nanmin(y_all)
        global_max = np.nanmax(y_all)
        ratio      = global_max / max(global_min, 1e-9)

        # ---------- Layout bestimmen ------------------------------------
        fig_h   = 4
        use_log = ratio >= 20
        if use_log:
            yscale = ("symlog", {"linthresh": global_min * 2})
        elif ratio >= 5:
            fig_h += 0.6 * np.log10(ratio)
            yscale = ("linear", {})
        else:
            yscale = ("linear", {})

        fig, ax = plt.subplots(figsize=(12, fig_h))

        # ---------- Balken zeichnen -------------------------------------
        offset = -(len(variants) - 1) / 2 * bar_w
        for j, v in enumerate(variants):
            if not _has_valid_values(y[v]):       # Variante komplett leer → skip
                continue
            ax.bar(
                x_pos + offset + j * bar_w,
                y[v],
                width=bar_w,
                label=v,
            )

        # ---------- Speichern oder verwerfen ----------------------------
        if not ax.patches:                        # gar kein Balken → nichts speichern
            plt.close(fig)
            print(f"⚠️  Concurrency {conc} – alle Varianten leer, Plot übersprungen")
            continue

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



# ─────────────────────────  BARS PLOT  ────────────────────────────────────
def bars_conc_variant(df: pd.DataFrame, *, all_users: bool = False) -> None:
    """
    Balkendiagramm(e) Ø-Duration_ms  vs.  Concurrency  &  Variante
    """
    # Mittelwert über alle Query-IDs bilden
    base = (
        df.groupby(["users", "concurrency", "variant"], as_index=False)["duration_ms"]
          .mean()
    )

    # komplette Variant-Palette (für konsistente Farben)
    var_palette = sorted(base["variant"].unique())
    cmap        = plt.get_cmap("tab10")
    palette     = {v: cmap(i) for i, v in enumerate(var_palette)}
    bar_w       = 0.18
    x_pos       = np.arange(len(CONCURRENCY))

    # ───────────────────────── Helper: EIN Balkendiagramm ───────────────────
    def _draw(ax, g, title_suffix: str, filename_suffix: str) -> None:
        offset = -(len(var_palette) - 1) / 2 * bar_w
        for j, v in enumerate(var_palette):
            ys = (
                g[g["variant"] == v]
                  .set_index("concurrency")
                  .reindex(CONCURRENCY)["duration_ms"]
                  .to_numpy()
            )
            if not _has_valid_values(ys):
                continue                     # nichts für diese Variante
            ax.bar(
                x_pos + offset + j * bar_w,
                ys,
                width=bar_w,
                color=palette[v],
                label=v,
            )
            # Balkenbeschriftung
            for xp, val in zip(x_pos + offset + j * bar_w, ys):
                ax.text(
                    xp, val, f"{val:.0f}",
                    ha="center", va="bottom",
                    fontsize=6, rotation=90,
                )

        # Wurde überhaupt etwas gezeichnet?
        if not ax.patches:
            plt.close(ax.figure)
            print(f"⚠️  {title_suffix}: keine Daten, Plot übersprungen")
            return

        ax.set_title(f"Average Duration – {title_suffix}")
        ax.set_xlabel("Concurrency (Threads)")
        ax.set_ylabel("Average Duration (ms)")
        ax.set_xticks(x_pos, [str(c) for c in CONCURRENCY])
        ax.yaxis.grid(True, linestyle=":", alpha=.6)
        ax.legend(title="Variante", fontsize=8)

        savefig(f"E_users{filename_suffix}_conc_vs_variant")
        plt.close(ax.figure)

    # ───────────────────────── alle User zusammen ───────────────────────────
    if all_users:
        g_all = (
            base.groupby(["concurrency", "variant"], as_index=False)["duration_ms"]
                .mean()
        )
        fig, ax = plt.subplots(figsize=(8, 4))
        _draw(ax, g_all.assign(users="ALL"), "ALL Users", "ALL")
        return

    # ───────────────────────── getrennt nach User-Größe ─────────────────────
    for users, g in base.groupby("users"):
        fig, ax = plt.subplots(figsize=(8, 4))
        _draw(ax, g, f"{users} Users", str(users))



def bars_variant_users(df: pd.DataFrame) -> None:
    # Ø-Volumen pro Variante & User-Gruppe
    base = (
        df.groupby(["variant", "users"], observed=True)["volume_mb"]
          .mean()
          .reset_index()
    )

    var_order  = sorted(base["variant"].unique())
    user_order = sorted(base["users"].unique())
    bar_w      = 0.8 / len(user_order)               # Clusterbreite
    x_pos      = np.arange(len(var_order))
    offset0    = -(len(user_order) - 1) / 2 * bar_w

    fig, ax = plt.subplots(figsize=(8, 4))
    cmap    = plt.get_cmap("tab10")

    for j, users in enumerate(user_order):
        ys = (
            base[base["users"] == users]
                .set_index("variant")
                .reindex(var_order)["volume_mb"]
                .to_numpy()
        )
        if not _has_valid_values(ys):                # komplette User-Gruppe leer?
            continue

        ax.bar(
            x_pos + offset0 + j * bar_w,
            ys,
            width=bar_w,
            color=cmap(j),
            label=f"{users:,} Users",
        )
        # optionale Balkenbeschriftung
        for xp, val in zip(x_pos + offset0 + j * bar_w, ys):
            if np.isfinite(val):
                ax.text(
                    xp, val, f"{val:.1f}",
                    ha="center", va="bottom",
                    fontsize=6, rotation=90,
                )

    # Wurde überhaupt etwas gezeichnet?
    if not ax.patches:
        plt.close(fig)
        print("⚠️  Volume-Plot – keine Daten, Plot übersprungen")
        return

    ax.set_xlabel("Variante")
    ax.set_ylabel("Volume (MB)")
    ax.set_xticks(x_pos, var_order, rotation=15)
    ax.yaxis.grid(True, linestyle=":", alpha=.6)
    ax.set_title("Datenbank-Volume – Variante vs. User-Größe")
    ax.legend(title="User-Gruppe", fontsize=8)

    savefig("F_variant_vs_users_volume")
    plt.close(fig)


# ─────────────────────────  SUMMARY → CSV  ────────────────────────────
def export_summary_csv(
    df: pd.DataFrame,
    out_dir: Path = Path("results"),
    decimals: int = 1,
) -> None:
    """
    • summary_table.csv        – Ø-Werte (plus Gesamtzeile 'ALL')
    • per_query_table.csv      – Ø je Query-ID
    • per_complexity_table.csv – Ø je Komplexitätsgruppe
                                  (Easy, Medium, …, Delete)
    Reihenfolge der Metriken: Duration → ServerTime → CPU → RAM → Disk
    """
    out_dir.mkdir(exist_ok=True)

    # ───────── feste Reihenfolgen ─────────
    METRIC_ORDER  = ["duration_ms", "server_ms", "avg_cpu", "avg_mem", "disk_mb"]
    VARIANT_ORDER = ["postgres_normal", "postgres_optimized",
                     "neo4j_normal",   "neo4j_optimized"]

    df = df.copy()
    df["variant"] = pd.Categorical(df["variant"],
                                   categories=VARIANT_ORDER,
                                   ordered=True)

    # ────────────────────────────────────────────────────────────────────────
    # 0️⃣  Hilfsspalte »complexity« aus query_no ableiten
    # ----------------------------------------------------------------------
    def _complexity(q):
        if   1  <= q <=  3:  return "easy"
        elif 4  <= q <=  6:  return "medium"
        elif 7  <= q <=  9:  return "complex"
        elif 10 <= q <= 12:  return "very_complex"
        elif 13 <= q <= 16:  return "create"
        elif 17 <= q <= 20:  return "update"
        else:                return "delete"        # 21–24
    df["complexity"] = df["query_no"].astype(int).map(_complexity)

    COMPLEXITY_ORDER = ["easy", "medium", "complex",
                        "very_complex", "create", "update", "delete"]
    df["complexity"] = pd.Categorical(df["complexity"],
                                      categories=COMPLEXITY_ORDER,
                                      ordered=True)

    # ────────────────────────────────────────────────────────────────────────
    # 1️⃣  SUMMARY  (Ø über alle Queries & Wiederholungen)
    # ----------------------------------------------------------------------
    summary = (
        df.groupby(["users", "concurrency", "variant"], observed=True)
          .agg({m: "mean" for m in METRIC_ORDER})
          .round(decimals)
          .pivot_table(index   = ["users", "concurrency"],
                       columns = "variant",
                       values  = METRIC_ORDER,
                       sort=False,
                       observed=True)
    )
    summary.columns = [f"{m}_{v}" for m, v in summary.columns.to_flat_index()]
    summary = summary.reset_index()

    # Gesamtzeile 'ALL'
    overall = (summary.drop(columns=["users", "concurrency"])
                      .mean(numeric_only=True).to_frame().T.round(decimals))
    overall.insert(0, "concurrency", "")
    overall.insert(0, "users", "ALL")
    summary = pd.concat([summary, overall], ignore_index=True)

    summary.to_csv(out_dir / "summary_table.csv", index=False)
    print(f"💾 summary_table.csv geschrieben → {out_dir}")

    # ────────────────────────────────────────────────────────────────────────
    # 2️⃣  PER-QUERY-TABELLE  (Ausreißer)
    # ----------------------------------------------------------------------
    per_q = (
        df.groupby(["users", "concurrency", "variant", "query_no"], observed=True)
          .agg({m: "mean" for m in METRIC_ORDER})
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

    # ────────────────────────────────────────────────────────────────────────
    # 3️⃣  PER-COMPLEXITY-TABELLE  (Easy … Delete)
    # ----------------------------------------------------------------------
    per_c = (
        df.groupby(["users", "concurrency", "variant", "complexity"], observed=True)
          .agg({m: "mean" for m in METRIC_ORDER})
          .round(decimals)
          .pivot_table(index   = ["users", "concurrency", "complexity"],
                       columns = "variant",
                       values  = METRIC_ORDER,
                       sort=False,
                       observed=True)
    )
    per_c.columns = [f"{m}_{v}" for m, v in per_c.columns.to_flat_index()]
    per_c = per_c.reset_index()
    per_c.to_csv(out_dir / "per_complexity_table.csv", index=False)
    print(f"💾 per_complexity_table.csv geschrieben → {out_dir}")

# ───────────────────── Gesamtdurchschnitt (alle Users) ───────────────────────
print("\n▶  Plots für ALLE Runs zusammen")
line_plots(pivot_all, tag="_all")
grouped_bars(pivot_all, tag="_all")
bars_conc_variant(df_raw, all_users=True)
vol_df = pd.read_csv("results/volume_sizes.csv")
bars_variant_users(vol_df)

# ───────────────────── Plots pro User-Größe (z. B. 100 / 1000 / 10000) ───────
for users, g_user in pivot_by_user.groupby("users"):
    print(f"\n▶  Plots für User-Größe {users}")
    suffix = f"_u{users}"
    line_plots(g_user, tag=suffix)
    grouped_bars(g_user, tag=suffix)
    bars_conc_variant(g_user)

export_summary_csv(df_raw)   # schreibt results/summary_table.csv

print("\n✅  Fertig!  Alle Diagramme liegen jetzt im Ordner  plots/")
