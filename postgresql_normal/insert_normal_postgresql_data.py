import argparse, json
import psycopg2
from psycopg2 import sql
import json
import psycopg2
from pathlib import Path
from tqdm import tqdm
from typing import List
import csv, math, subprocess
from pathlib import Path

BATCH_SIZE = 500_000
BASE_DIR = Path(__file__).resolve().parent            
RESULTS_DIR = (BASE_DIR / ".." / "results").resolve()


def fix_sequences(conn):
    # Enthält eine Zuordnung aller verwendeten Sequenznamen zu den zugehörigen Tabellen.
    # Diese Zuordnung ist notwendig, um die Sequenzen nach einem manuellen oder batchweisen
    # Datenimport korrekt auf den höchsten vorhandenen Primärschlüsselwert zu setzen.
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
            # Aktualisiert die Sequenz so, dass sie beim nächsten INSERT den korrekten,
            # fortlaufenden Wert vergibt. Andernfalls könnten Primärschlüsselkonflikte auftreten.
            # COALESCE stellt sicher, dass bei leeren Tabellen ein Startwert von 0 gesetzt wird.
            cur.execute(
                sql.SQL("SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {}), 0))")
                    .format(sql.Identifier(table_name)),
                [seq_name]
            )
    # Änderungen dauerhaft übernehmen
    conn.commit()
    print("✅ Alle Sequences wurden angepasst.")


def insert_dynamic_with_executemany(cur, conn, table: str, rows: List[dict]):
    # Prüft, ob überhaupt Daten übergeben wurden. Falls nicht, wird die Funktion beendet.
    if not rows:
        return
    # Extrahiere die Spaltennamen aus dem ersten Dictionary und erstelle SQL-Bestandteile.
    keys        = rows[0].keys()
    columns     = ", ".join(keys)
    placeholders= ", ".join(["%s"] * len(keys))
    query       = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    batch = []
    # Durchläuft alle Zeilen und fügt sie batchweise in die Datenbank ein.
    # Dies reduziert die Anzahl der Datenbanktransaktionen und verbessert die Performance.
    for row in tqdm(rows, desc=f"  ↳ {table}", unit="rows", ncols=80):
        batch.append(tuple(row[k] for k in keys))
        if len(batch) >= BATCH_SIZE:
            # Führt den Batch mit executemany effizient ein und committet anschließend.
            cur.executemany(query, batch)
            conn.commit()
            batch.clear()
    # Verbleibende Zeilen (Restmenge) nach dem letzten vollen Batch werden ebenfalls gespeichert.
    if batch:
        cur.executemany(query, batch)
        conn.commit()


def insert_data_to_normal_postgres(file_id: int, json_dir: str = "../output"):
    # Gibt den Pfad zur zu ladenden JSON-Datei aus
    print(f"\n📁 Lade Datei: users_{file_id}.json aus {json_dir}/ ...")
    json_path = Path(json_dir) / f"users_{file_id}.json"
    if not json_path.exists():
        print(f"❌ Datei nicht gefunden: {json_path}")
        return

    # Lädt und parst die JSON-Datei mit UTF-8-Encoding
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Baut eine Verbindung zur PostgreSQL-Datenbank auf
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

    # Prüft, ob eine SQL-Datei mit statischen Daten existiert und führt sie ggf. aus
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

    # Definiert alle Tabellen, die dynamisch aus dem JSON befüllt werden sollen
    dynamic_tables = [
        "users", "addresses",
        "orders", "order_items", "payments", "shipments",
        "reviews", "cart_items", "wishlists",
        "product_views", "product_purchases"
    ]

    print("\n📥 Beginne mit dem Einfügen der dynamischen Daten ...\n")

    # Iteriert über jede Tabelle und speist sie, falls vorhanden, in die Datenbank ein
    for table in dynamic_tables:
        print(f"➡️  {table} wird verarbeitet ...")
        if table not in data:
            print(f"⚠️  Tabelle '{table}' nicht in JSON enthalten, übersprungen.")
            continue

        rows = data[table]
        if not rows:
            print(f"⚠️  Keine Einträge in '{table}', übersprungen.")
            continue

        # Ermittelt Spaltennamen und bereitet SQL-Query für das Insert-Statement vor
        keys = rows[0].keys()
        columns = ", ".join(keys)
        placeholders = ", ".join(["%s"] * len(keys))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        # Übergibt die Daten an die vorbereitete Batch-Funktion
        insert_dynamic_with_executemany(cur, conn, table, rows)

    # Setzt alle Sequenzen korrekt auf den höchsten Primärschlüsselwert
    fix_sequences(conn)
    cur.close()
    conn.close()
    print(f"\n✅ Alle Daten aus Datei 'users_{file_id}.json' wurden erfolgreich eingefügt.")


def _pg_data_bytes(container: str, pg_datadir: str = "/var/lib/postgresql/data") -> int:
    """
    Liefert die belegten *Bytes* des PostgreSQL-Datenverzeichnisses im Container.

    - `du -sb` = Anzahl belegter Bytes (ohne Rundung, rekursiv)
    - Fällt auf SizeRootFs zurück, falls `du` scheitert
    """
    try:
        out = subprocess.check_output(
            ["docker", "exec", container, "du", "-sb", pg_datadir, "--apparent-size"],
            text=True
        ).split()[0]
        return int(out)
    except Exception as e:
        # Fallback: SizeRootFs (wie bisher bei Neo4j)
        try:
            out = subprocess.check_output(
                ["docker", "container", "inspect", "--size",
                 "--format", "{{.SizeRootFs}}", container],
                text=True
            ).strip()
            return int(out)
        except Exception:            # letzter Fallback: NaN
            print(f"⚠️  Konnte Volumen nicht ermitteln: {e}")
            return math.nan

def log_pg_volume(container: str,
                  variant: str,
                  n_users: int,
                  out_csv: Path = (BASE_DIR / ".." / "results" / "volume_sizes.csv")) -> None:
    """
    Misst das DB-Volumen (Bytes → MB) und hängt eine Zeile an die Ergebnis-CSV an.

    Spalten: variant | users | volume_mb
    """
    bytes_used = _pg_data_bytes(container)
    mb_used = None if math.isnan(bytes_used) else bytes_used / 1_000_000

    out_csv.parent.mkdir(exist_ok=True, parents=True)
    header = ("variant", "users", "volume_mb")

    # Datei evtl. anlegen und Kopf schreiben
    new_file = not out_csv.exists()
    with out_csv.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header)
        w.writerow([
                    variant, n_users, f"{mb_used:.1f}" if mb_used is not None else "nan"])

    print(f"💾  Volume-Größe protokolliert: {variant} | {n_users} | "
          f"{mb_used:.1f} MB" if mb_used is not None else "n/a")
    

if __name__ == "__main__":
    # Initialisiert den Argumentparser für Kommandozeilenparameter
    parser = argparse.ArgumentParser()
    # Übergibt die ID der Datei, z. B. bei 'users_3.json' wäre --file-id=3
    parser.add_argument("--file-id", type=int, required=True, help="Zahl X für Datei 'users_X.json'")
    # Optionaler Parameter: Verzeichnis, in dem sich die JSON-Dateien befinden
    parser.add_argument("--json-dir", type=str, default="../output", help="Ordnerpfad zur JSON-Datei")
    args = parser.parse_args()
    file_id = args.file_id
    # Startet den Datenimport mit den übergebenen Argumenten
    insert_data_to_normal_postgres(file_id, args.json_dir)
    log_pg_volume(
    container="pg_test_normal",   # Docker-Container-Name
    variant="pg_normal",          
    n_users=file_id               
)