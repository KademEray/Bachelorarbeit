import argparse, json
import psycopg2
from psycopg2 import sql
import json
import psycopg2
from pathlib import Path
from tqdm import tqdm
from typing import List

BATCH_SIZE = 500_000 # Größe der Batches für den Datenimport


def fix_sequences(conn):
    """
    Hilfsfunktion zum Zurücksetzen aller ID-Sequenzen auf den korrekten Wert
    Hintergrund: Bei direktem Einfügen von Daten über SQL werden ID-Sequenzen
    nicht automatisch angepasst. Diese Funktion stellt sicher, dass neu
    eingefügte Datensätze keine Konflikte mit bestehenden IDs verursachen.
    """
    # Zuordnung von Sequenznamen zu ihren zugehörigen Tabellen
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
            # Setzt den aktuellen Wert der Sequenz auf das Maximum der ID-Spalte
            # oder auf 0, falls die Tabelle leer ist
            cur.execute(
                sql.SQL("SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {}), 0))")
                    .format(sql.Identifier(table_name)),
                [seq_name]
            )
    conn.commit()
    print("✅ Alle Sequences wurden angepasst.")


def insert_dynamic_with_executemany(cur, conn, table: str, rows: List[dict]):
    # Überspringt die Verarbeitung, wenn keine Daten vorhanden sind
    if not rows:
        return

    # Dynamisches Ermitteln der Spaltennamen und Platzhalter für das INSERT-Statement
    keys         = rows[0].keys()
    columns      = ", ".join(keys)
    placeholders = ", ".join(["%s"] * len(keys))
    query        = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    batch = []
    for row in tqdm(rows, desc=f"  ↳ {table}", unit="rows", ncols=80):
        # Umwandlung jedes Dictionaries in ein Tupel zur Verwendung mit psycopg2
        batch.append(tuple(row[k] for k in keys))
        # Wenn die definierte Batch-Größe erreicht ist, wird ein Block in die DB geschrieben
        if len(batch) >= BATCH_SIZE:
            cur.executemany(query, batch)
            conn.commit()
            batch.clear()
    # Restliche Daten nach der Schleife einfügen (falls < BATCH_SIZE)
    if batch:
        cur.executemany(query, batch)
        conn.commit()


def insert_data_to_optimized_postgres(file_id: int, json_dir: str = "../output"):
    # Gibt an, welche Datei geladen werden soll
    print(f"\n📁 Lade Datei: users_{file_id}.json aus {json_dir}/ ...")
    json_path = Path(json_dir) / f"users_{file_id}.json"
    if not json_path.exists():
        print(f"❌ Datei nicht gefunden: {json_path}")
        return

    # Öffnet und lädt die JSON-Datei mit den zu importierenden Daten
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print("🔌 Stelle Verbindung zur PostgreSQL-Datenbank her ...")
    try:
        # Verbindungsaufbau zur lokalen PostgreSQL-Datenbank
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

    # Einfügen statischer Produktdaten (einmalig erforderlich)
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

    # Reihenfolge der dynamischen Tabellen, deren Inhalte aus der JSON-Datei eingefügt werden
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

        # Vorbereitung des SQL-Befehls zur Datenübertragung
        keys = rows[0].keys()
        columns = ", ".join(keys)
        placeholders = ", ".join(["%s"] * len(keys))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        # Übergabe an Hilfsfunktion für batchweisen Import
        insert_dynamic_with_executemany(cur, conn, table, rows)

    # Nach dem Import werden die Sequenzen aktualisiert, um Konflikte mit zukünftigen Inserts zu vermeiden
    fix_sequences(conn)
    cur.close()
    conn.close()
    print(f"\n✅ Alle Daten aus Datei 'users_{file_id}.json' wurden erfolgreich eingefügt.")


if __name__ == "__main__":
    # Initialisiert Argumentparser zur Übergabe von Kommandozeilenargumenten
    parser = argparse.ArgumentParser()
    
    # Erwartet einen Integer-Parameter --file-id (z. B. 3 für 'users_3.json')
    parser.add_argument(
        "--file-id",
        type=int,
        required=True,
        help="Zahl X für Datei 'users_X.json'"
    )

    # Optionaler Parameter für das Verzeichnis, in dem sich die JSON-Dateien befinden
    parser.add_argument(
        "--json-dir",
        type=str,
        default="../output",
        help="Ordnerpfad zur JSON-Datei"
    )

    # Parsed die Argumente und übergibt sie an die Hauptfunktion
    args = parser.parse_args()
    insert_data_to_optimized_postgres(args.file_id, args.json_dir)