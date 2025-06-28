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
    "figure.dpi":        300    # hohe Auflösung für Druck/Export
})


# ────────────────────────────── Helper --------------------------------------
def savefig(name: str):
    """
    Speichert das aktuelle Diagramm als PNG-Datei im PLOT_DIR-Ordner.
    
    Parameter:
    - name (str): Dateiname (ohne Erweiterung)
    """
    path = PLOT_DIR / f"{name}.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
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
          .agg(duration_ms=("per_query_ms", "mean"),   # Durchschnittliche Dauer pro Query
               avg_cpu     =("avg_cpu",      "mean"),   # Durchschnittliche CPU-Auslastung
               avg_mem     =("avg_mem",      "mean"))   # Durchschnittlicher Speicherverbrauch
          .reset_index()
)


# ────────────── Pivot pro User-Größe  (100, 1000 …)  ─────────────────────────
pivot_by_user = (
    df_raw.groupby(["users", "variant", "concurrency", "query_no"])
          .agg(duration_ms=("per_query_ms", "mean"),   # Ø pro Benutzergruppe
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


def grouped_bars(source: pd.DataFrame, tag: str):
    """
    Erstellt gruppierte Balkendiagramme:
      – Für jede Concurrency-Stufe (1, 3, 5, 10) ein separates Diagramm
      – x-Achse: Query-IDs
      – y-Achse: durchschnittliche Ausführungsdauer (ms)
      – 1 Balkengruppe pro Query, darin je 1 Balken pro Datenbank-Variante
    Der Parameter `tag` wird im Dateinamen verwendet (z. B. "_all", "_u100").
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


# ───────────────────── Plots pro User-Größe (z. B. 100 / 1000 / 10000) ───────
for users, g_user in pivot_by_user.groupby("users"):
    print(f"\n▶  Plots für User-Größe {users}")
    suffix = f"_u{users}"
    line_plots(g_user, tag=suffix)
    grouped_bars(g_user, tag=suffix)

print("\n✅  Fertig!  Alle Diagramme liegen jetzt im Ordner  plots/")
