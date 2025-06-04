import argparse
import json
import csv
import subprocess
import time
from pathlib import Path
from tqdm import tqdm
import ast
import re

# === Konfiguration ===
IMPORT_DIR = Path("import")
CSV_DIR = IMPORT_DIR
STATIC_PATH = Path("static_products_data_normal.cypher")
NEO4J_BIN = "/var/lib/neo4j/bin/neo4j-admin"
CONTAINER_NAME = "neo5_test_normal"
IMAGE_NAME = "neo5-normal"

# === Tabellenstruktur ===
NODE_TABLES = {
    "users": ["id:ID", "name", "email", "created_at:datetime"],
    "products": ["id:ID", "name", "price:float", "stock:int", "created_at:datetime", "updated_at:datetime"],
    "categories": ["id:ID", "name"],
    "orders": ["id:ID", "user_id", "status", "total:float", "created_at:datetime", "updated_at:datetime"]
}

REL_TABLES = {
    "addresses": ["user_id:START_ID", "id:END_ID", "street", "city", "zip", "country", "is_primary"],
    "order_items": ["id:ID", "order_id:START_ID", "product_id:END_ID", "quantity:int", "price:float", ":TYPE"],
    "payments": ["id:ID", "order_id:START_ID", "payment_method", "payment_status", "paid_at:datetime", ":END_ID"],
    "shipments": ["id:ID", "order_id:START_ID", "tracking_number", "shipped_at:datetime", "delivered_at:datetime", "carrier", ":END_ID"],
    "reviews": ["id:ID", "user_id:START_ID", "product_id:END_ID", "rating:int", "comment", "created_at:datetime"],
    "cart_items": ["id:ID", "user_id:START_ID", "product_id:END_ID", "quantity:int", "added_at:datetime", ":TYPE"],
    "wishlists": ["user_id:START_ID", "product_id:END_ID", "created_at:datetime", ":TYPE"],
    "product_views": ["id:ID", "user_id:START_ID", "product_id:END_ID", "viewed_at:datetime", ":TYPE"],
    "product_purchases": ["id:ID", "user_id:START_ID", "product_id:END_ID", "purchased_at:datetime", ":TYPE"],
    "product_categories": ["product_id:START_ID", "category_id:END_ID", ":TYPE"]
}



# === Hilfsfunktionen ===
def stop_neo4j_container():
    print("🛑 Stoppe laufenden Neo4j-Container falls aktiv ...")
    try:
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True, stdout=subprocess.DEVNULL)
        for _ in range(10):
            result = subprocess.run(["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                                    capture_output=True, text=True)
            if not result.stdout.strip():
                print("✅ Container wurde vollständig gestoppt.")
                return
            time.sleep(1)
    except Exception as e:
        print(f"⚠️  Fehler beim Stoppen: {e}")

def start_neo4j_container():
    print("🚀 Starte Neo4j-Container neu ...")

    data_volume_path = str(Path("neo4j_data").resolve())

    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "-e", "NEO4J_AUTH=neo4j/superpassword55",
        "-p", "7474:7474",
        "-p", "7687:7687",
        "-v", f"{data_volume_path}:/data",  # Persistente DB
        IMAGE_NAME
    ], check=True)

    time.sleep(10)
    print("✅ Container läuft.")

def write_csv(table_name, rows, header):
    path = CSV_DIR / f"{table_name}.csv"

    # Nur der Feldname vor dem Doppelpunkt zählt
    actual_fieldnames = header  # ← Header direkt so wie in REL_TABLES/NODE_TABLES
    expected_fields_set = set(actual_fieldnames)

    print(f"📄 Schreibe Tabelle: {table_name}")
    print(f"🔑 Erwartete Header: {expected_fields_set}")

    with open(path, "w", encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=actual_fieldnames)
        writer.writeheader()
        for i, row in enumerate(rows):
            # Transformiere Keys wie ':TYPE' zu 'TYPE' (nur Anzeige und Vergleich, nicht für Datei)
            transformed_row = row
            actual_fields = set(transformed_row.keys())
            extra_fields = actual_fields - expected_fields_set
            if extra_fields:
                print(f"\n❌ Fehler in Tabelle '{table_name}', Zeile {i+1}")
                print(f"→ Erwartete Felder: {expected_fields_set}")
                print(f"→ Tatsächliche Felder: {actual_fields}")
                print(f"→ Zusätzliche Felder: {extra_fields}")
                print(f"→ Datenzeile: {json.dumps(row, indent=2)}")
                raise ValueError(f"Fehlende Felddefinition(en) in Header für Tabelle '{table_name}'")
            writer.writerow(transformed_row)


def fix_cypher_props(text):
    # Fügt Anführungszeichen um unquoted Keys und Values hinzu
    text = re.sub(r"(\w+):", r'"\1":', text)  # Keys
    text = re.sub(r':\s*([A-Za-z_][A-Za-z0-9_]*)', r': "\1"', text)  # Werte
    return text


def convert_json_to_csv(json_path: Path, static_cypher: Path):
    print("🔄 Konvertiere JSON + Cypher zu CSV-Dateien ...")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    CSV_DIR.mkdir(parents=True, exist_ok=True)

    for table, header in NODE_TABLES.items():
        if table in data:
            rows = data[table]
            new_rows = []
            for row in rows:
                new_row = {}
                for h in header:
                    key = h.split(":")[0]
                    new_row[h] = row[key]
                new_rows.append(new_row)
            write_csv(table, new_rows, header)


    products, categories, product_cats = [], [], []
    with open(static_cypher, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("CREATE (p:Product"):
                obj_text = line.split("{")[1].split("}")[0]
                fixed = fix_cypher_props(obj_text)
                obj = ast.literal_eval("{" + fixed + "}")
                products.append(obj)
            elif line.startswith("CREATE (c:Category"):
                obj_text = line.split("{")[1].split("}")[0]
                fixed = fix_cypher_props(obj_text)
                obj = ast.literal_eval("{" + fixed + "}")
                categories.append(obj)
            elif "CREATE (p)-[:BELONGS_TO]->(c)" in line:
                pid = line.split("p:Product {id:")[1].split("}")[0].strip()
                cid = line.split("c:Category {id:")[1].split("}")[0].strip()
                product_cats.append({
                    "product_id": pid,
                    "category_id": cid,
                    ":TYPE": "BELONGS_TO"
                })

    write_csv("products", products, NODE_TABLES["products"])
    write_csv("categories", categories, NODE_TABLES["categories"])
    print(f"🧪 Übergabe an write_csv für product_categories:")
    print(f"→ Header: {REL_TABLES['product_categories']}")
    print(f"→ Beispielzeile: {product_cats[0] if product_cats else 'LEER'}")
    write_csv("product_categories", product_cats, REL_TABLES["product_categories"])

    for table, header in REL_TABLES.items():
        if table in data:
            rows = data[table]
            for row in rows:
                if ":TYPE" in header:
                    row[":TYPE"] = table.upper() if table not in ["order_items", "product_categories"] else "CONTAINS" if table == "order_items" else "BELONGS_TO"
                if ":END_ID" in header:
                    if "user_id" in row:
                        row[":END_ID"] = row["user_id"]
                    elif "product_id" in row:
                        row[":END_ID"] = row["product_id"]
                    elif "order_id" in row:
                        row[":END_ID"] = row["order_id"]
            write_csv(table, rows, header)

def run_neo4j_import():
    print("📦 Importiere CSV-Dateien in Neo4j (Docker) ...")

    host_import_path = str((CSV_DIR).resolve())
    data_volume_path = str(Path("neo4j_data").resolve())

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{host_import_path}:/var/lib/neo4j/import",
        "-v", f"{data_volume_path}:/data",
        IMAGE_NAME,
        "neo4j-admin", "database", "import", "full",
        "--overwrite-destination=true",
        "--verbose"
    ]

    for table in NODE_TABLES:
        cmd.append(f"--nodes={table}=/var/lib/neo4j/import/{table}.csv")
    for table in REL_TABLES:
        cmd.append(f"--relationships={table}=/var/lib/neo4j/import/{table}.csv")

    cmd.append("--")       # ← ⛑ Stoppe weitere Optionen
    cmd.append("neo4j")    # ← 📛 Datenbankname

    subprocess.run(cmd, check=True)
    print("✅ Import abgeschlossen.")

def cleanup():
    print("🧹 Lösche CSV-Dateien ...")
    for file in CSV_DIR.glob("*.csv"):
        file.unlink()
    CSV_DIR.rmdir()

# === Hauptprogramm ===
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", type=int, required=True)
    parser.add_argument("--json-dir", type=str, default="../output")
    args = parser.parse_args()

    json_file = Path(args.json_dir) / f"users_{args.file_id}.json"

    stop_neo4j_container()
    convert_json_to_csv(json_file, STATIC_PATH)
    run_neo4j_import()
    cleanup()
    start_neo4j_container()


if __name__ == "__main__":
    main()
