import argparse, json
import psycopg2
from psycopg2 import sql
import json
import psycopg2
from pathlib import Path
from tqdm import tqdm
from typing import List

BATCH_SIZE = 10_000

def fix_sequences(conn):
    seq_map = {
        'users_id_seq'              : 'users',
        'addresses_id_seq'          : 'addresses',
        'products_id_seq'           : 'products',
        'categories_id_seq'         : 'categories',
        'orders_id_seq'             : 'orders',
        'order_items_id_seq'        : 'order_items',
        'payments_id_seq'           : 'payments',
        'reviews_id_seq'            : 'reviews',
        'cart_items_id_seq'         : 'cart_items',
        'shipments_id_seq'          : 'shipments',
        'product_views_id_seq'      : 'product_views',
        'product_purchases_id_seq'  : 'product_purchases'
    }

    with conn.cursor() as cur:
        for seq_name, table_name in seq_map.items():
            print(f"🔁 Setze Sequence {seq_name} für Tabelle {table_name} …")
            cur.execute(
                sql.SQL("SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {}), 0))")
                    .format(sql.Identifier(table_name)),
                [seq_name]
            )
    conn.commit()
    print("✅ Alle Sequences wurden angepasst.")

def insert_dynamic_with_executemany(cur, conn, table: str, rows: List[dict]):
    if not rows:
        return
    keys        = rows[0].keys()
    columns     = ", ".join(keys)
    placeholders= ", ".join(["%s"] * len(keys))
    query       = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    batch = []
    for row in tqdm(rows, desc=f"  ↳ {table}", unit="rows", ncols=80):
        batch.append(tuple(row[k] for k in keys))
        if len(batch) >= BATCH_SIZE:
            cur.executemany(query, batch)
            conn.commit()
            batch.clear()
    if batch:
        cur.executemany(query, batch)
        conn.commit()

def insert_data_to_optimized_postgres(file_id: int, json_dir: str = "../output"):
    print(f"\n📁 Lade Datei: users_{file_id}.json aus {json_dir}/ ...")
    json_path = Path(json_dir) / f"users_{file_id}.json"
    if not json_path.exists():
        print(f"❌ Datei nicht gefunden: {json_path}")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print("🔌 Stelle Verbindung zur PostgreSQL-Datenbank her ...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="pass",
            dbname="testdb"
        )
        cur = conn.cursor()
        print("✅ Verbindung erfolgreich.")
    except Exception as e:
        print(f"❌ Verbindungsfehler: {e}")
        return

    # 📦 Füge statische Daten ein (nur einmal)
    static_sql_path = Path(__file__).parent / "static_products_data.sql"
    if static_sql_path.exists():
        print(f"\n📄 Füge statische Produktdaten aus '{static_sql_path.name}' ein ...")
        try:
            with open(static_sql_path, "r", encoding="utf-8") as f:
                static_sql = f.read()
            cur.execute(static_sql)
            conn.commit()
            print("✅ Statische Daten erfolgreich eingefügt.")
        except Exception as e:
            print(f"❌ Fehler beim Einfügen der statischen Daten: {e}")
            conn.rollback()
    else:
        print(f"⚠️  Statische SQL-Datei nicht gefunden: {static_sql_path}")

    # Tabellen für dynamische Daten
    dynamic_tables = [
        "users", "addresses",
        "orders", "order_items", "payments", "shipments",
        "reviews", "cart_items", "wishlists",
        "product_views", "product_purchases"
    ]

    print("\n📥 Beginne mit dem Einfügen der dynamischen Daten ...\n")

    for table in dynamic_tables:
        print(f"➡️  {table} wird verarbeitet ...")
        if table not in data:
            print(f"⚠️  Tabelle '{table}' nicht in JSON enthalten, übersprungen.")
            continue

        rows = data[table]
        if not rows:
            print(f"⚠️  Keine Einträge in '{table}', übersprungen.")
            continue

        keys = rows[0].keys()
        columns = ", ".join(keys)
        placeholders = ", ".join(["%s"] * len(keys))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        insert_dynamic_with_executemany(cur, conn, table, rows)

    fix_sequences(conn)
    cur.close()
    conn.close()
    print(f"\n✅ Alle Daten aus Datei 'users_{file_id}.json' wurden erfolgreich eingefügt.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", type=int, required=True, help="Zahl X für Datei 'users_X.json'")
    parser.add_argument("--json-dir", type=str, default="../output", help="Ordnerpfad zur JSON-Datei")
    args = parser.parse_args()

    insert_data_to_optimized_postgres(args.file_id, args.json_dir)
