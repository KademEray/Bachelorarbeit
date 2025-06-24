import argparse
import json
import csv
import subprocess
import time
from pathlib import Path
from tqdm import tqdm
import ast
import re
import shutil
import socket
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# === Konfiguration ===
IMPORT_DIR = Path(__file__).resolve().parent / "import"
CSV_DIR = IMPORT_DIR
NEO4J_BIN = "/var/lib/neo4j/bin/neo4j-admin"
CONTAINER_NAME = "neo5_test_optimized"
IMAGE_NAME = "neo5-optimized"

# === Tabellenstruktur für Optimized ===
NODE_TABLES = {
    "users": [
        "id:ID(User)",
        "name",
        "email",
        "created_at:datetime"
    ],
    "products": [
        "id:ID(Product)",
        "name",
        "description",
        "price:float",
        "stock:int",
        "created_at:datetime",
        "updated_at:datetime"
    ],
    "categories": [
        "id:ID(Category)",
        "name"
    ],
    "addresses": [
        "id:ID(Address)",
        "user_id:int",
        "street",
        "city",
        "zip",
        "country",
        "is_primary:boolean"
    ],
    "orders": [
        "id:ID(Order)",
        "user_id:int",
        "status",
        "total:float",
        "created_at:datetime",
        "updated_at:datetime"
    ],
    "payments": [
        "id:ID(Payment)",
        "order_id:int",
        "payment_method",
        "payment_status",
        "paid_at:datetime"
    ],
    "shipments": [
        "id:ID(Shipment)",
        "order_id:int",
        "tracking_number",
        "shipped_at:datetime",
        "delivered_at:datetime",
        "carrier"
    ]
}

# === Mapping Node-Table → Node-Typ ===
NODE_TYPES = {
    "users":       "User",
    "products":    "Product",
    "categories":  "Category",
    "addresses":   "Address",
    "orders":      "Order",
    "payments":    "Payment",
    "shipments":   "Shipment",
}

# === Relationship-Builder für direct Relationships ===
RELATION_BUILDERS = {
    "user_address": lambda row: {
        "user_id:START_ID(User)":     row["user_id"],
        "address_id:END_ID(Address)": row["id"],
        ":TYPE":                      "HAS_ADDRESS"
    },
    "user_order": lambda row: {
        "user_id:START_ID(User)": row["user_id"],
        "order_id:END_ID(Order)": row["id"],
        ":TYPE":                  "PLACED"
    },
    "order_payment": lambda row: {
        "order_id:START_ID(Order)":   row["order_id"],
        "payment_id:END_ID(Payment)": row["id"],
        ":TYPE":                      "PAID_WITH"
    },
    "order_shipment": lambda row: {
        "order_id:START_ID(Order)":    row["order_id"],
        "shipment_id:END_ID(Shipment)": row["id"],
        ":TYPE":                       "SHIPPED"
    },
    "user_wishlist": lambda row: {
    "user_id:START_ID(User)":   row["user_id"],
    "product_id:END_ID(Product)": row["product_id"],
    # fehlt der Key → None  ⇒  Property wird einfach weggelassen
    "created_at:datetime":      row.get("created_at"),
    ":TYPE":                    "WISHLISTED"
    },
    "order_contains": lambda row: {
        "order_id:START_ID(Order)":   row["order_id"],
        "product_id:END_ID(Product)": row["product_id"],
        "quantity:int":               row["quantity"],
        "price:float":                row["price"],
        ":TYPE":                      "CONTAINS"
    },
    "user_reviewed": lambda row: {
        "user_id:START_ID(User)":     row["user_id"],
        "product_id:END_ID(Product)": row["product_id"],
        "rating:int":                 row["rating"],
        "comment":                    row["comment"],
        "created_at:datetime":        row["created_at"],
        ":TYPE":                      "REVIEWED"
    },
    "user_cart": lambda row: {
        "user_id:START_ID(User)":     row["user_id"],
        "product_id:END_ID(Product)": row["product_id"],
        "quantity:int":               row["quantity"],
        "added_at:datetime":          row["added_at"],
        ":TYPE":                      "HAS_IN_CART"
    },
    "user_viewed": lambda row: {
        "user_id:START_ID(User)":     row["user_id"],
        "product_id:END_ID(Product)": row["product_id"],
        "viewed_at:datetime":         row["viewed_at"],
        ":TYPE":                      "VIEWED"
    },
    "user_purchased": lambda row: {
        "user_id:START_ID(User)":       row["user_id"],
        "product_id:END_ID(Product)":   row["product_id"],
        "purchased_at:datetime":        row["purchased_at"],
        ":TYPE":                        "PURCHASED"
    },
}

# === Aus welcher Tabelle je Relationship speisen ===
RELATION_TABLE_SOURCES = {
    "user_address":   "addresses",
    "user_order":     "orders",
    "order_payment":  "payments",
    "order_shipment": "shipments",
    "user_wishlist":  "wishlists",
    "order_contains": "order_items",
    "user_reviewed":  "reviews",
    "user_cart":      "cart_items",
    "user_viewed":    "product_views",
    "user_purchased": "product_purchases",
}


def stop_neo4j_container():
    print("🛑 Stoppe laufenden Neo4j-Container falls aktiv ...")
    try:
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True, stdout=subprocess.DEVNULL)
        for _ in range(10):
            result = subprocess.run(["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"], capture_output=True, text=True)
            if not result.stdout.strip():
                print("✅ Container wurde vollständig gestoppt.")
                return
            time.sleep(1)
    except Exception as e:
        print(f"⚠️  Fehler beim Stoppen: {e}")

def start_neo4j_container():
    print("🚀 Starte Neo4j-Container neu ...")
    data_volume_path = str((Path(__file__).resolve().parent / "neo4j_data").resolve())
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "-e", "NEO4J_AUTH=neo4j/superpassword55",
        "-p", "7474:7474", "-p", "7687:7687",
        "-v", f"{data_volume_path}:/data",
        IMAGE_NAME
    ], check=True)
    wait_for_bolt()
    print("✅ Container läuft.")

def fix_cypher_props(text):
    text = re.sub(r"(\w+):", r'"\1":', text)
    text = re.sub(r':\s*([A-Za-z_][A-Za-z0-9_]*)', r': "\1"', text)
    return text

def convert_json_to_csv_refactored(json_file: Path, out_dir: Path):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    out_dir.mkdir(parents=True, exist_ok=True)

    # === Nodes exportieren ===
    for table, label in NODE_TYPES.items():
        rows = data.get(table, [])
        if not rows:
            continue
        with open(out_dir / f"{table}.csv", "w", encoding="utf-8", newline="") as f_out:
            fieldnames = list(rows[0].keys())
            # Ersetze das erste Feld (angenommen: ID-Feld) mit :ID(Label)
            header_line = [f"{fieldnames[0]}:ID({label})"] + fieldnames[1:]
            writer = csv.writer(f_out)
            writer.writerow(header_line)
            for row in rows:
                writer.writerow([row.get(k) for k in fieldnames])

    # === Relationships vorbereiten ===
    rel_rows = {}

    # Nur Beziehungen erzeugen, wenn die Quelltabelle existiert
    for rel, source_table in RELATION_TABLE_SOURCES.items():
        if source_table not in data:
            continue
        rows = data[source_table]
        builder = RELATION_BUILDERS[rel]
        rel_rows[rel] = []
        for row in rows:
            try:
                rel_row = builder(row)
                rel_rows[rel].append(rel_row)
            except KeyError:
                continue

    # === Relationships exportieren ===
    for rel, rows in rel_rows.items():
        if not rows:
            continue
        with open(out_dir / f"{rel}.csv", "w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    return sorted(list(out_dir.glob("*.csv")))

def wait_for_bolt(uri="bolt://127.0.0.1:7687", auth=("neo4j","superpassword55"),
                  timeout=120, delay=2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with GraphDatabase.driver(uri, auth=auth) as drv:
                with drv.session() as s:
                    s.run("RETURN 1").consume()
            print("✅ Neo4j ist bereit.")
            return
        except ServiceUnavailable:
            time.sleep(delay)
    raise RuntimeError("❌ Neo4j kam nicht hoch – Timeout!")

def run_neo4j_import():
    print("📦 Importiere CSV-Dateien in Neo4j (Docker) ...")
    host_import_path = str(CSV_DIR.resolve())
    data_volume_path = str((Path(__file__).resolve().parent / "neo4j_data").resolve())

    cmd = [
        "docker", "run", "--rm", "--user", "7474:7474",
        "-v", f"{host_import_path}:/var/lib/neo4j/import",
        "-v", f"{data_volume_path}:/data",
        IMAGE_NAME,
        "neo4j-admin", "database", "import", "full",
        "--overwrite-destination=true", "--verbose",
        "--normalize-types=false"
    ]

    # 🔁 Manuelle statische Tabellen einfügen (wenn vorhanden)
    static_nodes = {"Product": "Product.csv", "Category": "Category.csv"}
    static_relationships = ["product_categories"]

    for label, file_name in static_nodes.items():
        node_file = CSV_DIR / file_name
        if node_file.exists():
            cmd.append(f"--nodes={label}=/var/lib/neo4j/import/{file_name}")

    for rel in static_relationships:
        rel_file = CSV_DIR / f"{rel}.csv"
        if rel_file.exists():
            cmd.append(f"--relationships={rel}=/var/lib/neo4j/import/{rel}.csv")

    # 🔁 Dynamisch generierte Nodes hinzufügen (mit korrektem Label)
    for table, label in NODE_TYPES.items():
        node_file = CSV_DIR / f"{table}.csv"
        if node_file.exists():
            cmd.append(f"--nodes={label}=/var/lib/neo4j/import/{table}.csv")

    # 🔁 Dynamisch generierte Beziehungen hinzufügen
    for rel in RELATION_BUILDERS:
        rel_file = CSV_DIR / f"{rel}.csv"
        if rel_file.exists():
            cmd.append(f"--relationships={rel}=/var/lib/neo4j/import/{rel}.csv")

    cmd += ["--", "neo4j"]
    subprocess.run(cmd, check=True)
    print("✅ Import abgeschlossen.")

def cleanup():
    print("🧹 Lösche CSV-Dateien ...")
    for file in CSV_DIR.glob("*.csv"):
        file.unlink()
    shutil.rmtree(CSV_DIR)

def reset_database_directory():
    db_path = Path(__file__).resolve().parent / "neo4j_data"
    if db_path.exists() and db_path.is_dir():
        print("🧨 Entferne bestehenden Neo4j-Datenbank-Ordner ...")
        shutil.rmtree(db_path)
        print("✅ Alter Datenbankordner entfernt.")
    db_path.mkdir(parents=True, exist_ok=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", type=int, required=True)
    parser.add_argument("--json-dir", type=str, default="../output")
    args = parser.parse_args()
    json_file = Path(args.json_dir) / f"users_{args.file_id}.json"
    reset_database_directory()
    stop_neo4j_container()
    convert_json_to_csv_refactored(json_file, CSV_DIR)
    run_neo4j_import()
    cleanup()
    start_neo4j_container()

if __name__ == "__main__":
    main()
