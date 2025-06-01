import argparse
import json
import psycopg2
from pathlib import Path
from tqdm import tqdm

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

        for row in tqdm(rows, desc=f"  ↳ {table}", unit="rows", ncols=80):
            values = tuple(row[key] for key in keys)
            try:
                cur.execute(query, values)
            except Exception as e:
                print(f"\n❌ Fehler beim Einfügen in '{table}': {e}\nWerte: {values}\n")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ Alle Daten aus Datei 'users_{file_id}.json' wurden erfolgreich eingefügt.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", type=int, required=True, help="Zahl X für Datei 'users_X.json'")
    parser.add_argument("--json-dir", type=str, default="../output", help="Ordnerpfad zur JSON-Datei")
    args = parser.parse_args()

    insert_data_to_optimized_postgres(args.file_id, args.json_dir)
