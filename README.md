# Performancevergleich von PostgreSQL und Neo4j in typischen eCommerce-Webseiten 
**Bachelorarbeit**

| Feld                           | Inhalt                                                      |
|--------------------------------|-------------------------------------------------------------|
| **Autor**                      | Eray Kadem                                                  |
| **Bearbeitungs­zeitraum**      | 30.5.2025 - 08.08.2025       |
| **Erstgutachter**              | Prof. Dr. Arif Wider                                        |
| **Zweitgutachter**             | Lucas Larisch                                               |
| **Repository-Lizenz**          | MIT                                                         |
| **Daten- & Ergebnis-Lizenz**   | CC BY 4.0                                                   |
| **Kontakt**                    | Bitte ein GitHub-Issue eröffnen                            |

---

## 1 Projekt­zusammenfassung – *Was wurde untersucht?*

Diese Arbeit vergleicht **relationale Datenbanken (PostgreSQL 17.5)** mit **Graphdaten­banken (Neo4j 5.26.6)** unter realitätsnaher Shop­last. Dafür werden  

* **synthetische Nutz-, Bestell- und Produkt­daten** in drei Größenordnungen (1.000 / 10.000 / 100.000 Nutzer) erzeugt,  
* **24 CRUD-/Analyse-Queries** bei vier Parallelitäts­stufen (1 / 3 / 5 / 10 Threads) ausgeführt und  
* **Latenz, CPU, RAM und belegter Speicher** automatisiert aufgezeichnet.

---

## 2 Daten­herkunft – *Wer liefert welche Rohdaten?*

| Quelle | Lizenz / Identifier |
|--------|--------------------|
| **Roh-Produktdaten**: *Amazon UK Products Dataset 2023* (Kaggle) | ODC-By 1.0 |
| **Generierte Shopdaten** (*generate_data.py*) | MIT |
| **Benchmark-CSVs & Plots** (automatisch erzeugt) | MIT |

> **Zitation des externen Datensatzes**  
> Saniczka, A. (2023). *Amazon UK Products Dataset 2023* [Data set]. Kaggle.  
> <https://doi.org/10.34740/kaggle/ds/3864183>  
> Lizenz: ODC-By 1.0

Alle Shop-Datensätze sind **vollständig synthetisch** und enthalten *keine* personen­bezogenen Daten.

---

## 3 Daten­formate & -umfang – *Welche Dateien entstehen?*

| Artefakt                              | Format      | typische Größe |
|---------------------------------------|-------------|----------------|
| Shop-Export je Skalierung             | `.json`     | ≈ 40 – 400 MB |
| Import­dateien (PostgreSQL / Neo4j)   | `.csv`      | ≈ 30 – 300 MB |
| Benchmark-Ergebnisse                  | `.csv`      | ≈ 5 – 25 MB  |
| Auswertungs­grafiken                  | `.png`      | < 500 kB pro Plot |

---

## 4 Werkzeuge, Versionen & Hardware

| Kategorie                     | Tool / Bibliothek          | Version |
|-------------------------------|----------------------------|---------|
| **Python**                    | Python 3.11.9 |
| **Datengenerierung**          | `pandas` 2.2.3, `faker` 37.3.0 |
| **Container-DBs**             | PostgreSQL 17.5, Neo4j 5.26.6 |
| **Treiber**                   | `psycopg2-binary` 2.9.10, `neo4j` 5.28.1 |
| **Benchmark**                 | `concurrent.futures`, Docker ≥ 24 |
| **Visualisierung**            | `matplotlib` 3.9.4, `numpy` 1.26.4 |
| **Hilfstools**                | `tqdm` 4.67.1, `scipy` 1.16.1 |

**Hardware (Testhost)**  

| Komponente      | Spezifikation |
|-----------------|---------------|
| CPU             | AMD Ryzen 5 7600X - 6 C / 12 T, max 4.7 GHz |
| RAM             | 32 GB DDR5 |
| SSD 1 / 2       | KIOXIA EXCERIA PRO, je ≈ 931 GB |
| Betriebssystem  | Windows 11 (22H2) + Docker Engine 28.2.2 (build e6534b4) |

---

## 5 Ablage- & Benennungs­schema – *Wo liegt was?*

```
results/
├─ *_results.csv                         # Rohdaten pro Lauf: users, variant, round, repetitions, warmups
├─ summary_table.csv                     # Durchschnittswerte über alle Abfragen + Gesamtzeile ALL
├─ per_query_table.csv                   # Durchschnittswerte pro Einzel-Query (Ausreißer sichtbar)
├─ per_complexity_table.csv              # Durchschnittswerte pro Komplexitätsgruppe (easy…delete)
├─ constellation_stats.csv               # Mittelwert, Std-Abw. & Varianz je Konstellation (users, concurrency, variant)
├─ complexity_stats.csv                  # Mittelwert, Std-Abw. & Varianz je Komplexitätsgruppe
├─ ci_duration_by_complexity.csv         # 95 %-Konfidenzintervalle (duration_ms) je Komplexität
├─ p99_by_complexity.csv                 # 99 %-Perzentil (duration_ms) je Komplexität
├─ p55_by_complexity.csv                # P50 Mediane Latenz (duration_ms) je Komplexität
├─ significance_by_complexity.csv        # Paarweise Welch-t-Tests je Komplexität
├─ ci_duration_by_query.csv              # 95 %-Konfidenzintervalle (duration_ms) je Query
├─ p99_by_query.csv                      # 99 %-Perzentil (duration_ms) je Query
├─p55_by_query.csv                      # P50 Mediane Latenz(duration_ms) je Query
├─ significance_by_query.csv             # Paarweise Welch-t-Tests je Query
├─ volume_sizes.csv                      # Gemessene DB-Volumina (MB) je users & variant

plots/
└─ <prefix>_<variant>.png

logs/
└─ benchmark.log
```
`variant` ∈ `pg_normal`, `pg_opt`, `neo_normal`, `neo_opt`

---

## 6 Qualitäts­sicherung

| Schritt                     | Maßnahme |
|-----------------------------|----------|
| Reproduzierbare Umgebung    | feste Docker-Tags & `requirements.txt` |
| Datenvalidierung            | Plausi-Checks im Datengenerator |
| Laufzeit-Logging            | vollständige Logs in `logs/` |

---

## 7 Datenschutz

Es werden ausschließlich **synthetische Daten** verwendet → **keine DSGVO-Relevanz**.

---

## 8 Installation & Schnell­start

```bash
git clone https://github.com/KademEray/Bachelorarbeit.git
cd Bachelorarbeit

# Wenn Fehler kommt das product_dataset.csv fehlt dann Produktdatensatz downloaden und nach ./product_data/ entpacken und in product_dataset.csv umbennen
# https://doi.org/10.34740/kaggle/ds/3864183

Konfigurationsdatei anpassen an Hostsystem: neo4j.conf und postgres-tuning.conf

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# vollständigen Workflow starten
python main.py

```

*Ergebnisse landen in `results/`, Plots in `plots/`.*

---

## 9. Systemanforderungen & Fehlerbehebung

### Empfohlene Konfiguration
Der Benchmark ist für die unter Punkt 4 spezifizierte Hardware (6C/12T CPU, 32 GB RAM) optimiert. Für eine reibungslose Ausführung wird eine vergleichbare Konfiguration empfohlen.

### Fehlerbehebung: Import schlägt fehl (`out of memory`)
**Ursache:** Der initiale Datenimport ist sehr speicherintensiv, da eine hohe **Batch-Größe** verwendet wird, um die Laufzeit zu verkürzen. Auf Systemen mit weniger als 16 GB RAM kann dies zum Absturz des Datenbank-Containers führen.

**Lösung:** Reduzieren Sie die `BATCH_SIZE`-Variable in den folgenden Skripten manuell (z.B. von `500000` auf `100000`):
*   `src/database/postgresql_importer.py`
*   `src/database/neo4j_importer.py`

Diese Anpassung erhöht zwar die Importzeit, ermöglicht aber die Ausführung auf ressourcenschwächeren Systemen. Das eigentliche Performance-Benchmark-Skript muss nicht verändert werden, da die Parallelität Teil des Testdesigns ist.

---

## 10 Lizenzen & Nachnutzung

* **Code**: MIT  
* **Benchmark-Ergebnisse & Plots**: CC BY 4.0 – bitte Autor & Quelle nennen.  
* **Externe Rohdaten**: siehe jeweilige Ursprungs­lizenz (Kaggle-Link).

---

## 11 Weiterführende Ressourcen

* FitForFDM-Checkliste „Software‑Code‑Dokumentation / Abschlussarbeit“
* Offizielle Dokumentation: PostgreSQL 17 • Neo4j 5  

---

> Letzte Aktualisierung: 05.08.2025
