import argparse
import json
import csv
import subprocess
import time
from pathlib import Path
import re
import shutil
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# === Konfiguration ===
IMPORT_DIR = Path(__file__).resolve().parent / "import"
CSV_DIR = IMPORT_DIR
NEO4J_BIN = "/var/lib/neo4j/bin/neo4j-admin"
CONTAINER_NAME = "neo5_test_normal"
IMAGE_NAME = "neo5-normal"

# === Tabellenstruktur (Nodes) =============================================
NODE_TABLES = {
    "users": [
        "user_id:ID(User)",     # Import-ID
        "id:int",               # fachliche ID
        "name", "email", "created_at:datetime"
    ],
    "addresses": [
        "address_id:ID(Address)",
        "id:int",
        "user_id:int", "street", "city", "zip", "country", "is_primary:boolean"
    ],
    "orders": [
        "order_id:ID(Order)",
        "id:int",
        "user_id:int", "status",
        "total:float", "created_at:datetime", "updated_at:datetime"
    ],
    "order_items": [
        "orderitem_id:ID(OrderItem)",
        "id:int",
        "order_id:int", "product_id:int",
        "quantity:int", "price:float"
    ],
    "payments": [
        "payment_id:ID(Payment)",
        "id:int",
        "order_id:int", "payment_method",
        "payment_status", "paid_at:datetime"
    ],
    "shipments": [
        "shipment_id:ID(Shipment)",
        "id:int",
        "order_id:int", "tracking_number",
        "shipped_at:datetime", "delivered_at:datetime", "carrier"
    ],
    "reviews": [
        "review_id:ID(Review)",
        "id:int",
        "user_id:int", "product_id:int",
        "rating:int", "comment", "created_at:datetime"
    ],
    "cart_items": [
        "cartitem_id:ID(CartItem)",
        "id:int",
        "user_id:int", "product_id:int",
        "quantity:int", "added_at:datetime"
    ],
    "product_views": [
        "productview_id:ID(ProductView)",
        "id:int",
        "user_id:int", "product_id:int", "viewed_at:datetime"
    ],
    "product_purchases": [
        "productpurchase_id:ID(ProductPurchase)",
        "id:int",
        "user_id:int", "product_id:int", "purchased_at:datetime"
    ],
}

# === Label-Abbildung (unverändert) ========================================
NODE_TYPES = {
    "users": "User",
    "addresses": "Address",
    "orders": "Order",
    "order_items": "OrderItem",
    "payments": "Payment",
    "shipments": "Shipment",
    "cart_items": "CartItem",
    "product_views": "ProductView",
    "product_purchases": "ProductPurchase",
    "reviews": "Review",
}

# === Relationship-Builder ==================================================
RELATION_BUILDERS = {
    # User - Address
    "user_address": lambda row: {
        "user_id:START_ID(User)":   row["user_id"],
        "address_id:END_ID(Address)": row["id"],
        ":TYPE": "HAS_ADDRESS"
    },
    # User - Order
    "user_order": lambda row: {
        "user_id:START_ID(User)": row["user_id"],
        "order_id:END_ID(Order)": row["id"],
        ":TYPE": "PLACED"
    },
    # Order - OrderItem
    "order_item": lambda row: {
        "order_id:START_ID(Order)":       row["order_id"],
        "orderitem_id:END_ID(OrderItem)": row["id"],
        ":TYPE": "HAS_ITEM"
    },
    # OrderItem - Product
    "orderitem_product": lambda row: {
        "orderitem_id:START_ID(OrderItem)": row["id"],
        "product_id:END_ID(Product)":       row["product_id"],
        ":TYPE": "REFERS_TO"
    },
    # Order - Payment
    "order_payment": lambda row: {
        "order_id:START_ID(Order)":  row["order_id"],
        "payment_id:END_ID(Payment)": row["id"],
        ":TYPE": "PAID_WITH"
    },
    # Order - Shipment
    "order_shipment": lambda row: {
        "order_id:START_ID(Order)":   row["order_id"],
        "shipment_id:END_ID(Shipment)": row["id"],
        ":TYPE": "HAS_SHIPMENT"
    },
    # User - Review
    "user_review": lambda row: {
        "user_id:START_ID(User)":  row["user_id"],
        "review_id:END_ID(Review)": row["id"],
        ":TYPE": "WROTE"
    },
    # Review - Product
    "review_product": lambda row: {
        "review_id:START_ID(Review)": row["id"],
        "product_id:END_ID(Product)": row["product_id"],
        ":TYPE": "REVIEWS"
    },
    # User - CartItem
    "user_cartitem": lambda row: {
        "user_id:START_ID(User)":    row["user_id"],
        "cartitem_id:END_ID(CartItem)": row["id"],
        ":TYPE": "HAS_IN_CART"
    },
    # CartItem - Product
    "cartitem_product": lambda row: {
        "cartitem_id:START_ID(CartItem)": row["id"],
        "product_id:END_ID(Product)":     row["product_id"],
        ":TYPE": "CART_PRODUCT"
    },
    # User - ProductView
    "user_productview": lambda row: {
        "user_id:START_ID(User)":       row["user_id"],
        "productview_id:END_ID(ProductView)": row["id"],
        ":TYPE": "VIEWED"
    },
    # ProductView - Product
    "productview_product": lambda row: {
        "productview_id:START_ID(ProductView)": row["id"],
        "product_id:END_ID(Product)":           row["product_id"],
        ":TYPE": "VIEWED_PRODUCT"
    },
    # User - ProductPurchase
    "user_purchased": lambda row: {
        "user_id:START_ID(User)":            row["user_id"],
        "productpurchase_id:END_ID(ProductPurchase)": row["id"],
        ":TYPE": "PURCHASED"
    },
    # ProductPurchase - Product
    "productpurchase_product": lambda row: {
        "productpurchase_id:START_ID(ProductPurchase)": row["id"],
        "product_id:END_ID(Product)":                   row["product_id"],
        ":TYPE": "PURCHASED_PRODUCT"
    },
    # User - Wishlist (direkt zu Product)
    "user_wishlist": lambda row: {
        "user_id:START_ID(User)":  row["user_id"],
        "product_id:END_ID(Product)": row["product_id"],
        "created_at:datetime":     row["created_at"],
        ":TYPE": "WISHLISTED"
    },
}

# === Mapping Tabelle → Relationship-Quelle =================================
RELATION_TABLE_SOURCES = {
    "user_address":        "addresses",
    "user_order":          "orders",
    "order_item":          "order_items",
    "orderitem_product":   "order_items",
    "order_payment":       "payments",
    "order_shipment":      "shipments",
    "user_review":         "reviews",
    "review_product":      "reviews",
    "user_cartitem":       "cart_items",
    "cartitem_product":    "cart_items",
    "user_productview":    "product_views",
    "productview_product": "product_views",
    "user_purchased":      "product_purchases",
    "productpurchase_product": "product_purchases",
    "user_wishlist":       "wishlists"
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
    """Schreibt für jedes Node-CSV sowohl Import-ID als auch fachliche ID."""

    data = json.loads(Path(json_file).read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- Nodes ------------------------------------------------------
    for table, header in NODE_TABLES.items():
        rows = data.get(table, [])
        if not rows:
            continue

        # ➊ Map: Spalten­name  → Typangabe ('' | 'boolean' | 'int' …)
        type_by_key = {
            h.split(":")[0]: (h.split(":")[1] if ":" in h else "")
            for h in header
        }

        csv_path = out_dir / f"{table}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)

            def resolve_value(row, key):
                # A) Wert existiert im JSON    → direkt nehmen
                if key in row:
                    val = row[key]
                # B) Import-ID Spalte (…_id)   → fachliche id übernehmen
                elif key.endswith("_id") and "id" in row:
                    val = row["id"]
                # C) sonst                    → None (= fehlend)
                else:
                    val = None

                # ➋ Typ-spezifische Aufbereitung
                col_type = type_by_key.get(key, "")
                if col_type == "boolean":
                    # fehlender Wert      → false
                    # Python-Bool         → Literal in Kleinbuchstaben
                    return "true" if bool(val) else "false"
                if val is None:
                    return ""            # alle anderen Typen: leer lassen
                return val

            for row in rows:
                writer.writerow([resolve_value(row, k.split(":")[0])
                                for k in header])

    # ---------- Relationships ---------------------------------------------
    # (bleibt exakt wie zuvor)
    rel_rows = {}
    for rel, source_table in RELATION_TABLE_SOURCES.items():
        if source_table not in data:
            continue
        rows = data[source_table]
        builder = RELATION_BUILDERS[rel]
        rel_rows[rel] = [builder(r) for r in rows]

    for rel, rows in rel_rows.items():
        if not rows:
            continue
        with open(out_dir / f"{rel}.csv", "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    return sorted(out_dir.glob("*.csv"))


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
