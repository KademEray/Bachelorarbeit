# Skript: export_sql_cypher.py
# Zweck:
# Dieses Skript konvertiert statische Daten aus einer generierten JSON-Datei in SQL- und Cypher-Skripte.
# Es werden vier separate Ausgabedateien erstellt:
# - static_products_data.sql (für PostgreSQL normal + optimized)
# - static_products_data_normal.cypher (für Neo4j normal)
# - static_products_data_optimized.cypher (für Neo4j optimized)

import argparse, json, random, shutil
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import html
import re
import csv

# === Helferfunktionen für JSON-Export ===

def stream_write(file, obj):
    # Schreibt ein JSON-Objekt als einzelne Zeile in die Datei (JSONL-Format)
    file.write(json.dumps(obj, ensure_ascii=False) + "\n")


def open_stream_files(base_dir: Path, tables: list[str]):
    # Öffnet für jede Tabelle eine eigene .jsonl-Datei zum Schreiben
    files = {}
    for name in tables:
        path = base_dir / f"{name}.jsonl"
        files[name] = open(path, "w", encoding="utf-8")
    return files


def close_stream_files(files: dict):
    # Schließt alle geöffneten Stream-Dateien
    for f in files.values():
        f.close()


def beautify_json_file(file_path: Path):
    # Liest die Datei ein und schreibt sie im eingerückten, lesbaren JSON-Format zurück
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def merge_jsonl_to_single_file(stream_dir: Path, final_path: Path):
    """
    Führt mehrere .jsonl-Dateien zu einer einzigen verschachtelten JSON-Datei zusammen.
    Die Dateien werden anschließend gelöscht.
    """
    tables = ["categories", "products", "product_categories"]
    final_path.parent.mkdir(parents=True, exist_ok=True)

    with open(final_path, "w", encoding="utf-8") as out:
        out.write("{\n")
        for i, table in enumerate(tables):
            out.write(f'"{table}": [\n')
            first = True
            with open(stream_dir / f"{table}.jsonl", "r", encoding="utf-8") as f:
                for line in f:
                    if not first:
                        out.write(",\n")
                    out.write(line.strip())
                    first = False
            out.write("\n]")
            if i < len(tables) - 1:
                out.write(",\n")
        out.write("\n}\n")

    shutil.rmtree(stream_dir)  # Löscht temporäres Verzeichnis mit den Einzeldateien
    beautify_json_file(final_path)  # Formatiert finale JSON-Datei
    print(f"✓ Datei erstellt unter: {final_path.resolve()}")


def generate_static_json(product_csv: Path, tmp_dir: Path, output_file: Path):
    """
    Konvertiert ein CSV mit Produktdaten in ein JSON-Format, das Kategorien, Produkte
    und ihre Zuordnungen enthält. Die Daten werden zunächst als JSONL gestreamt und
    anschließend zu einer finalen JSON-Datei zusammengeführt.
    """
    tmp_dir.mkdir(exist_ok=True)
    stream_files = open_stream_files(tmp_dir, ["categories", "products", "product_categories"])

    # CSV einlesen mit notwendigen Spalten
    df = pd.read_csv(
        product_csv,
        usecols=["title", "price", "categoryName", "reviews"],
        encoding="utf-8",
        converters={"reviews": lambda x: int(str(x).replace(",", "").strip()) if x else 0}
    ).dropna()

    products_raw = df.reset_index(drop=True)
    cat_name_to_id, categories = {}, []

    def get_cat_id(name: str) -> int:
        # Gibt eine eindeutige ID für jede Kategorie zurück (String wird bereinigt)
        name = name.strip()
        if name not in cat_name_to_id:
            cid = len(categories) + 1
            cat_name_to_id[name] = cid
            categories.append({"id": cid, "name": name})
        return cat_name_to_id[name]

    products, product_categories = [], []
    for idx, row in products_raw.iterrows():
        pid = idx + 1  # Produkt-IDs beginnen bei 1
        now = datetime.now()
        ten_years_ago = now - timedelta(days=365 * 10)

        # Zufällige Zeitstempel in den letzten 10 Jahren generieren
        created_dt = ten_years_ago + timedelta(seconds=random.randint(0, int((now - ten_years_ago).total_seconds())))
        created_at = created_dt.isoformat(timespec="seconds")
        updated_dt = created_dt + timedelta(seconds=random.randint(0, int((now - created_dt).total_seconds())))
        updated_at = updated_dt.isoformat(timespec="seconds")

        # Produkt-Objekt erstellen
        products.append({
            "id": pid,
            "name": str(row.title)[:255],
            "description": None,
            "price": float(row.price),
            "stock": random.randint(1, 100),
            "created_at": created_at,
            "updated_at": updated_at
        })

        # Kategorie-Zuordnungen auflösen (Mehrfachzuordnungen pro Produkt möglich)
        for cat in {c.strip() for c in row.categoryName.split(",") if c.strip()}:
            cid = get_cat_id(cat)
            product_categories.append({"product_id": pid, "category_id": cid})

    # Streaming-Dateien schreiben
    for c in categories:
        stream_write(stream_files["categories"], c)
    for p in products:
        stream_write(stream_files["products"], p)
    for pc in product_categories:
        stream_write(stream_files["product_categories"], pc)

    close_stream_files(stream_files)
    merge_jsonl_to_single_file(tmp_dir, output_file)


# === Exporter ===

def escape_sql_value(value):
    """
    Wandelt einen Wert in einen SQL-sicheren String um (z. B. Escaping von Hochkommas),
    oder gibt NULL zurück.
    """
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def escape_cypher_string(value: str) -> str:
    """
    Wandelt einen beliebigen String in ein für Cypher sicheres Format um,
    indem er problematische Zeichen escaped und Sonderzeichen bereinigt.
    """
    if not value:
        return ""
    s = html.unescape(str(value))                     # HTML-Entities (z. B. &quot; → ")
    s = s.replace("\\", "\\\\")                       # Backslashes escapen
    s = s.replace("'", "\\'")                         # Einfache Anführungszeichen escapen
    s = s.replace('"', '\\"')                         # Doppelte Anführungszeichen escapen
    s = s.replace("’", "\\'").replace("‘", "\\'")     # Unicode-Einzelzeichen
    s = s.replace("“", '\\"').replace("”", '\\"')     # Unicode-Doppelte Anführungszeichen
    s = s.replace("–", "-")                           # Gedankenstrich zu normalem Minus
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")  # Zeilenumbrüche
    s = s.replace(";", ",")
    s = re.sub(r"\s{2,}", " ", s)                     # Mehrfache Leerzeichen entfernen
    return s.strip()


def export_static_tables_to_sql_and_cypher(json_path: Path,
                                           sql_normal_path: Path,
                                           sql_optimized_path: Path,
                                           cypher_normal_path: Path,
                                           cypher_optimized_path: Path):
    """
    Exportiert die statischen Daten (Produkte, Kategorien und deren Verknüpfungen)
    aus der JSON-Datei in:
      - SQL INSERT-Befehle für PostgreSQL (normal + optimiert)
      - CSV-Dateien im Neo4j-Importformat (normal + optimiert)
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    static_tables = ["categories", "products", "product_categories"]

    for path in [sql_normal_path, sql_optimized_path, cypher_normal_path, cypher_optimized_path]:
        path.parent.mkdir(parents=True, exist_ok=True)

    # ────────── SQL-Export ──────────
    for path in [sql_normal_path, sql_optimized_path]:
        with open(path, "w", encoding="utf-8") as sql_file:
            for table in tqdm(static_tables, desc=f"SQL Export to {path.name}", ncols=80):
                rows = data.get(table, [])
                if not rows:
                    continue
                for row in rows:
                    columns = ", ".join(row.keys())
                    values = ", ".join(escape_sql_value(v) for v in row.values())
                    sql_file.write(f"INSERT INTO {table} ({columns}) VALUES ({values});\n")

    # ────────── Neo4j CSV-Export ──────────
    csv_tmp_dir = Path("tmp_csv_export")
    csv_tmp_dir.mkdir(parents=True, exist_ok=True)

    csv_tables = {
        "products": [
            "product_id:ID(Product)",
            "id:int",
            "name", "description",
            "price:float", "stock:int",
            "created_at:datetime", "updated_at:datetime"
        ],
        "categories": [
            "category_id:ID(Category)",
            "id:int",
            "name"
        ],
        "product_categories": [
            "product_id:START_ID(Product)",
            "category_id:END_ID(Category)",
            ":TYPE"
        ]
    }

    table_to_filename = {
        "products": "Product.csv",
        "categories": "Category.csv",
        "product_categories": "product_categories.csv"
    }

    for table, header in csv_tables.items():
        rows = data.get(table, [])
        if not rows:
            continue

        filename = table_to_filename[table]
        path     = csv_tmp_dir / filename

        with open(path, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=header)
            writer.writeheader()

            for row in rows:
                row_out = {}

                for col in header:
                    if col == ":TYPE":
                        continue

                    base = col.split(":")[0]

                    # Füllt ..._id-Spalten automatisch, wenn nötig
                    if base.endswith("_id") and base not in row and "id" in row:
                        row_out[col] = row["id"]
                    else:
                        row_out[col] = row.get(base) if row.get(base) is not None else ""

                if table == "product_categories":
                    row_out[":TYPE"] = "BELONGS_TO"

                writer.writerow(row_out)

    # Kopiere CSVs in beide Neo4j-Verzeichnisse
    for target_dir in ["neo4j_normal/import", "neo4j_optimized/import"]:
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)
        for file in csv_tmp_dir.glob("*.csv"):
            shutil.copy(file, target_path / file.name)
        print(f"📁 CSVs kopiert nach: {target_path.resolve()}")

    shutil.rmtree(csv_tmp_dir)

    print("\n✅ Export abgeschlossen.")


# === CLI-Wrapper ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generiert und exportiert statische Produktdaten als SQL- und Cypher-Dateien.")
    parser.add_argument("--product-csv", type=str, default="product_data/product_dataset.csv",
                        help="Pfad zur CSV-Datei mit Produktdaten")
    parser.add_argument("--tmp", type=str, default="tmp_static_gen",
                        help="Temporärer Ordner für Stream-Dateien")
    parser.add_argument("--static-json", type=str, default="static.json",
                        help="Zielpfad für temporäre JSON-Datei")

    parser.add_argument("--sql-normal", type=str, default="postgresql_normal/static_products_data.sql")
    parser.add_argument("--sql-optimized", type=str, default="postgresql_optimized/static_products_data.sql")
    parser.add_argument("--cypher-normal", type=str, default="neo4j_normal/static_products_data_normal.cypher")
    parser.add_argument("--cypher-optimized", type=str, default="neo4j_optimized/static_products_data_optimized.cypher")
    args = parser.parse_args()

    # 1. Generiere static.json aus CSV-Daten
    generate_static_json(Path(args.product_csv), Path(args.tmp), Path(args.static_json))

    # 2. Exportiere Inhalte der JSON-Datei in SQL- und Cypher-kompatible Formate
    export_static_tables_to_sql_and_cypher(
        Path(args.static_json),
        Path(args.sql_normal),
        Path(args.sql_optimized),
        Path(args.cypher_normal),
        Path(args.cypher_optimized)
    )

    # 3. Lösche temporäre JSON-Datei zur Bereinigung
    try:
        Path(args.static_json).unlink()
        print(f"🧹 static.json gelöscht.")
    except Exception as e:
        print(f"⚠ Fehler beim Löschen von static.json: {e}")