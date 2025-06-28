# Bachelorarbeit

**Titel der Abschlussarbeit:** Performancevergleich von PostgreSQL und Neo4j in typischen eCommerce-Webseiten
**Autor:** Eray Kadem  
**Zeitraum:** 30.05.2025 - 08.08.2025
**Erstgutachter*in:** Prof. Dr. Arif Wider
**Zweitgutachter*in:** Lucas Larisch
**Repository‑Lizenz:** MIT  
**Daten / Grafiken:** CC BY 4.0

---

## 1  Was befindet sich in diesem Repository ?

| Pfad                          | Inhalt / Zweck | Wichtigste Werkzeuge |
|------------------------------|----------------|----------------------|
| `generate_data.py`           | synthetischer Shop‑Dataset (JSON) | *pandas*, *faker* |
| `export_sql_cypher.py`       | Export der JSON‑Daten in SQL‑ und Cypher‑Skripte | |
| `postgresql_normal/`, `postgresql_optimized/` | Docker‑Images & Skripte für PostgreSQL (Baseline / Indizes + Tuning) | PostgreSQL 17.5 |
| `neo4j_normal/`, `neo4j_optimized/`           | Docker‑Images & Skripte für Neo4j (Baseline / verkürzte Relationen) | Neo4j 5.26.6 |
| `performance_benchmark.py`   | misst Dauer, QPS, CPU, RAM, Disk & Netz I/O für 24 Shop‑Queries × 4 Concurrency‑Stufen | *psycopg2*, *neo4j*, *tqdm* |
| `main.py`                    | Orchestriert kompletten Lauf: Daten­generierung → Importe → Benchmark (2 Rounds × 2 User‑Skalen) | |
| `analyse.py`                 | fasst die CSV‑Ergebnisse zu Diagrammen (.png) zusammen | *pandas*, *matplotlib* |
| `results/` *(.gitignored)*   | erzeugte Mess‑CSV‑Dateien (je Variante, Round & User‑Level) | |
| `plots/` *(.gitignored)*     | automatisch gespeicherte PNG‑Grafiken | |
| `logs/`                      | Laufzeit‑Logs der Benchmarks | `logging` |

---

## 2  Installation & erster Testlauf

```bash
# 1) Repository klonen
git clone https://github.com/KademEray/Bachelorarbeit.git
cd Bachelorarbeit

# 2) Dataset downloaden
Herunterladen: https://www.kaggle.com/datasets/asaniczka/amazon-uk-products-dataset-2023
Entpacken und die Datei product_dataset.csv in den /product_data legen

# 3) Python‑Umgebung anlegen (empfohlen)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 4) Abhängigkeiten installieren
pip install -r requirements.txt

# 5) Docker ≥ 24 installiert?  → Starten Sie einen kompletten Durchlauf
python main.py
```

Die Mess‑CSVs landen in `results/`,
die fertigen Diagramme nach `python analyse.py` in `plots/`.

---

## 3  Abhängigkeiten (aus `requirements.txt`)
Python 3.10.11

```
pandas==2.2.3
numpy==1.26.4
matplotlib==3.9.4
neo4j==5.28.1
psycopg2-binary==2.9.10
tqdm==4.67.1
Faker==37.3.0
```

Docker‑Images:  
* `postgres:17.5` (Normal & Optimized)  
* `neo4j:5.26.6`

---

## 4  Ablagestruktur & Namenskonventionen

```
results/
└─ {users}_{variant}_{round}_{repetitions}_{warmups}_results.csv
plots/
└─ <Prefix>_<Variante>.png   # s. analyse.py
```

Jede CSV‑Zeile enthält u. a.:

| Spalte            | Beschreibung |
|-------------------|--------------|
| `db`, `mode`      | postgres/neo4j × normal/optimized |
| `concurrency`     | 1, 3, 5, 10 Parallel‑Threads |
| `query_no` 1‑24   | Shop‑Workload (SELECT, INSERT, UPDATE, DELETE) |
| `duration_ms`     | Gesamtdauer des Batch‑Runs |
| `per_query_ms`    | Dauer ÷ Concurrency |
| `qps`             | Queries pro Sekunde |
| `avg_cpu`, `avg_mem` | mittlere Container‑Ressourcen |

---

## 5  Qualitätssicherung

* **Logging** – alle Skripte schreiben ausführliche Zeitstempel‑Logs nach `logs/`.  

---

## 6  Datenschutz & Lizenz

Die erzeugten Daten sind rein synthetisch; es werden **keine** Personen­daten verarbeitet.
Alle CSV‑Ergebnisse und Diagramme stehen unter **CC BY 4.0**.  
Der Quellcode selbst ist **MIT‑lizenziert** (siehe `LICENSE`).

---

## 7  Kontakt

Bei Fragen gerne ein Issue erstellen.

---

*README‑Vorlage angelehnt an die „Software‑Code‑Dokumentation / Abschlussarbeit (FitForFDM)“ Checkliste v1.0 (CC BY 4.0).*