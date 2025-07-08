# Performancevergleich von PostgreSQL und Neo4j in typischen eCommerce-Webseiten 
**Bachelorarbeit**

|                             |                                            |
|-----------------------------|--------------------------------------------|
| **Autor**                  | Eray Kadem |
| **Bearbeitungszeitraum**   | (wird nach Abschluss eingetragen) |
| **Erstgutachter\***        | Prof. Dr. Arif Wider |
| **Zweitgutachter\***       | Lucas Larisch |
| **Repository‑Lizenz**      | MIT |
| **Daten- & Ergebnis-Lizenz** | CC BY 4.0 |
| **Kontakt**                | bitte Issue auf GitHub eröffnen |

---

## 1  Projektzusammenfassung – *Was?*

Diese Arbeit untersucht, **wie sich relationale (PostgreSQL 17.5) und graphbasierte (Neo4j 5.26.6) Datenbanken** bei einem realistischen E‑Commerce‑Workload verhalten.  
Dazu werden

* **synthetische Shopdaten** in zwei Größenordnungen erzeugt (100 & 1000 Nutzer),
* identische CRUD‑Workloads (24 Queries × 4 Concurrency‑Stufen) ausgeführt und
* Metriken wie Latenz, QPS, CPU, RAM sowie Disk‑ und Netzwerk‑I/O erfasst.

---

## 2  Datenherkunft – *Wer, Woher?*

| Quelle | Lizenz/Identifier |
|--------|------------------|
| **Roh-Produktdaten:** Amazon UK Products Dataset 2023 (Kaggle) | Open Data Commons Attribution (ODC-By) v1.0 |
| **Generierte Shopdaten:** `generate_data.py` | MIT |
| **Benchmark‑CSVs & Plots:** automatisch erzeugt | MIT |

**Zitation des Datensatzes**

> Saniczka, A. (2023): *Amazon UK Products Dataset 2023.*  
> Bereitgestellt über Kaggle.  
> Lizenz: **Open Data Commons Attribution License (ODC-By) v1.0**  
> URL: <https://www.kaggle.com/datasets/asaniczka/amazon-uk-products-dataset-2023>

Alle Generierte Shopdaten sind **vollständig synthetisch** und enthalten **keine personenbezogenen Informationen**.

---

## 3  Datenformate & ‑umfang – *Welche, Wie viel?*

| Artefakt | Format 
|----------|--------
| Shop‑JSON je Nutzer‑Skalierung | `.json` 
| Import‑CSV (PostgreSQL / Neo4j) | `.csv` 
| Benchmark‑Ergebnisse | `.csv` 
| Auswertungs‑Plots | `.png` 

---

## 4  Werkzeuge, Versionen & Hardware

| Einsatzbereich                 | Tool / Bibliothek            | Version |
|--------------------------------|------------------------------|---------|
| **Datengenerierung**           | Python (CPython)             | 3.10.11 |
|                                | `pandas`                     | 2.2.3   |
|                                | `Faker`                      | 37.3.0  |
| **Datenbanken&nbsp;(Docker)**  | PostgreSQL                   | 17.5    |
|                                | Neo4j                        | 5.26.6  |
| **DB-Treiber**                 | `psycopg2-binary`            | 2.9.10  |
|                                | `neo4j` (Python-Driver)      | 5.28.1  |
| **Benchmarking**               | `concurrent.futures`         | builtin |
|                                | Docker CLI / Engine          | ≥ 24    |
| **Auswertung & Visualisierung**| `pandas`                     | 2.2.3   |
|                                | `numpy`                      | 1.26.4  |
|                                | `matplotlib`                 | 3.9.4   |
| **Utilities**                  | `tqdm` (Progress-Bars)       | 4.67.1  |

Die vollständige, exakt versionierte Abhängigkeitsliste liegt in `requirements.txt`

# Hardware- & Software-Umgebung
| Komponente       | Wert |
|------------------|------|
| **CPU**          | AMD Ryzen 5 7600X - 6 C / 12 T, max 4.7 GHz |
| **RAM**          | 32 GB DDR5 |
| **Disk 1**       | KIOXIA EXCERIA PRO SSD – 931 GB |
| **Disk 2**       | KIOXIA EXCERIA PRO SSD – 931 GB |
| **Betriebssystem** | Windows 11 (22H2) |
| **Docker Engine** | 28.2.2, (build e6534b4) |

---

## 5  Ablage‑ & Benennungskonvention – *Wo?*

```
results/
└─ {users}_{variant}_{round}_{repetitions}_{warmups}_results.csv
plots/
└─ <Prefix>_<Variante>.png          # siehe analyse.py
logs/
└─ benchmark.log
```

*`variant`* ∈ {`pg_normal`, `pg_opt`, `neo_normal`, `neo_opt`}

---

## 6  Qualitätssicherung

| Schritt | Maßnahme |
|---------|----------|
| **Reproduzierbare Umgebung** | feste Docker‑Tags & `requirements.txt` |
| **Validierung** | Konsistenz‑Checks der Datengeneratoren |
| **Logging** | vollständige Laufzeit‑Logs in `logs/` |


---

## 7  Datenschutz

Alle Daten sind synthetisch ➜ **keine DSGVO‑Relevanz**.

---

## 8  Installation & Schnellstart

```bash
# Repository klonen
git clone https://github.com/KademEray/Bachelorarbeit.git
cd Bachelorarbeit

# Roh‑Produktdaten von Kaggle laden, entpacken und CSV nach ./product_data/ kopieren
# https://www.kaggle.com/datasets/asaniczka/amazon-uk-products-dataset-2023

# Virtuelle Umgebung anlegen
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scriptsctivate

# Abhängigkeiten installieren
pip install -r requirements.txt

# Kompletten Workflow ausführen (ca. 15‑20 Minuten)
python main.py
```

*Mess‑CSVs erscheinen in `results/`, Diagramme nach `python analyse.py` in `plots/`.*

---

## 9  Lizenz‑ & Zugriffshinweise

* **Code:** MIT‑Lizenz (siehe `LICENSE`)
* **Benchmark‑Ergebnisse & Plots:** CC BY 4.0  
  Bitte bei Weiterverwendung Autor & Quelle angeben.
* **Externe Rohdaten:** jeweilige Ursprungs‑Lizenz (siehe Kaggle‑Link)

---

## 10  Weiterführende Ressourcen

* FitForFDM‑Checkliste „Software‑Code‑Dokumentation / Abschlussarbeit“  
* Offizielle Dokumentation zu PostgreSQL 17 & Neo4j 5

---

