import argparse          # Ermöglicht das Einlesen und Verarbeiten von Kommandozeilenargumenten
import json              # Zum Parsen von JSON-Dateien
import csv               # Für das Schreiben von CSV-Dateien (z. B. für den Neo4j-Import)
import subprocess        # Zur Ausführung externer Shell-Kommandos (z. B. Docker)
import time              # Zur Steuerung von Pausen und Wartezeiten
from pathlib import Path # Plattformunabhängiges Arbeiten mit Dateipfaden
import re                # Reguläre Ausdrücke, z. B. für Datenbereinigung
import shutil            # Dateisystemoperationen (z. B. Ordner löschen)
import os, math, sys
from neo4j import GraphDatabase                    # Offizieller Python-Treiber für Neo4j (Bolt-Verbindung)
from neo4j.exceptions import ServiceUnavailable    # Fehlerbehandlung bei nicht erreichbarem Neo4j-Service

# === Konfiguration der Arbeitsumgebung ===

CSV_DIR = Path(__file__).resolve().parent / "import"  # Verzeichnis für importierte CSV-Dateien

CONTAINER_NAME = "neo5_test_optimized"                   # Eindeutiger Name für den Docker-Container (Optimized-Version)
IMAGE_NAME = "neo5-optimized"                            # Name des zu verwendenden Docker-Images (Optimized-Version)
BASE_DIR = Path(__file__).resolve().parent            # Ordner, in dem DAS Skript liegt
RESULTS_DIR = (BASE_DIR / ".." / "results").resolve()


# === Tabellenstruktur für Optimized-Version ========================================
# Diese Struktur legt die CSV-Spaltennamen und -typen für alle Knoten fest,
# die im optimierten Graphmodell verwendet werden. Sie dient als Grundlage für
# den CSV-Export und anschließenden Import in Neo4j.

NODE_TABLES = {
    "users": [
        "user_id:ID(User)",          # ⇢ Neo4j-Import-ID für User-Knoten
        "id:int",                    # ⇢ Fachliche ID (zur Beziehungserzeugung)
        "name", "email",             # ⇢ Basisdaten des Nutzers
        "created_at:datetime"        # ⇢ Registrierungsdatum
    ],

    "products": [
        "product_id:ID(Product)",
        "id:int",
        "name", "description",       # ⇢ Produktbezeichnung und -beschreibung
        "price:float", "stock:int",  # ⇢ Preisangabe und Lagerbestand
        "created_at:datetime", "updated_at:datetime"
    ],

    "categories": [
        "category_id:ID(Category)",
        "id:int",
        "name"                       # ⇢ Bezeichnung der Produktkategorie
    ],

    "addresses": [
        "address_id:ID(Address)",
        "id:int",
        "user_id:int",               # ⇢ Fremdbezug zum User
        "street", "city", "zip", "country",
        "is_primary:boolean"         # ⇢ Angabe, ob Hauptadresse
    ],

    "orders": [
        "order_id:ID(Order)",
        "id:int",
        "user_id:int", "status",     # ⇢ Nutzerreferenz und Auftragsstatus
        "total:float",
        "created_at:datetime", "updated_at:datetime"
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
    ]
}


# === Mapping Node-Table → Node-Typ ===============================================
# Diese Zuordnung definiert, welches Label (Node-Typ) in Neo4j für die jeweilige
# JSON- oder CSV-Tabelle verwendet wird. Sie wird beim Import benötigt, um die
# Knoten mit semantisch korrekten Typbezeichnungen zu versehen.

NODE_TYPES = {
    "users":       "User",       # ⇢ Benutzerkonten
    "products":    "Product",    # ⇢ Produkte (z. B. Artikel im Shop)
    "categories":  "Category",   # ⇢ Produktkategorien
    "addresses":   "Address",    # ⇢ Nutzeradressen
    "orders":      "Order",      # ⇢ Kundenbestellungen
    "payments":    "Payment",    # ⇢ Zahlungsinformationen
    "shipments":   "Shipment"    # ⇢ Versandinformationen
}


# === Relationship-Builder (Optimized-Modell) ================================
# Diese Struktur definiert, wie Beziehungen aus den JSON-Zeilen generiert werden.
# Sie enthält sowohl klassische Referenzen (direkt aus Nodes) als auch umgewandelte
# frühere Join-Knoten, die nun als Beziehungen mit Properties modelliert sind.

RELATION_BUILDERS = {

    # ───────── 1) Beziehungen direkt aus verknüpften Knoten ─────────
    # Diese Beziehungen entstehen aus Feldern innerhalb der ursprünglichen Knoten,
    # z. B. user_id in Adressen oder Bestellungen.
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
        "order_id:START_ID(Order)":     row["order_id"],
        "shipment_id:END_ID(Shipment)": row["id"],
        ":TYPE":                        "SHIPPED"
    },

    # ───────── 2) Wunschliste ohne eigene Entität ─────────
    # Die Wishlist hat keinen eigenen Node, daher werden die Informationen direkt
    # als Beziehung mit Timestamp gespeichert.
    "user_wishlist": lambda row: {
        "user_id:START_ID(User)":     row["user_id"],
        "product_id:END_ID(Product)": row["product_id"],
        "created_at:datetime":        row.get("created_at"),
        ":TYPE":                      "WISHLISTED"
    },

    # ───────── 3) Ehemalige Join-Knoten als Relationship mit Properties ─────────
    # In der optimierten Modellierung werden Join-Tabellen (z. B. cart_items, reviews)
    # nicht mehr als Knoten, sondern als Beziehungen mit Metainformationen umgesetzt.
    "order_contains": lambda row: {
        "id:int":                    row["id"],                # frühere ID aus OrderItem
        "order_id:START_ID(Order)":  row["order_id"],
        "product_id:END_ID(Product)":row["product_id"],
        "quantity:int":              row["quantity"],
        "price:float":               row["price"],
        ":TYPE":                     "CONTAINS"
    },
    "user_reviewed": lambda row: {
        "id:int":                    row["id"],                # frühere ID aus Review
        "user_id:START_ID(User)":    row["user_id"],
        "product_id:END_ID(Product)":row["product_id"],
        "rating:int":                row["rating"],
        "comment":                   row["comment"],
        "created_at:datetime":       row["created_at"],
        ":TYPE":                     "REVIEWED"
    },
    "user_cart": lambda row: {
        "id:int":                    row["id"],                # frühere ID aus CartItem
        "user_id:START_ID(User)":    row["user_id"],
        "product_id:END_ID(Product)":row["product_id"],
        "quantity:int":              row["quantity"],
        "added_at:datetime":         row["added_at"],
        ":TYPE":                     "HAS_IN_CART"
    },
    "user_viewed": lambda row: {
        "id:int":                    row["id"],                # frühere ID aus ProductView
        "user_id:START_ID(User)":    row["user_id"],
        "product_id:END_ID(Product)":row["product_id"],
        "viewed_at:datetime":        row["viewed_at"],
        ":TYPE":                     "VIEWED"
    },
    "user_purchased": lambda row: {
        "id:int":                    row["id"],                # frühere ID aus ProductPurchase
        "user_id:START_ID(User)":    row["user_id"],
        "product_id:END_ID(Product)":row["product_id"],
        "purchased_at:datetime":     row["purchased_at"],
        ":TYPE":                     "PURCHASED"
    },
}


# === Datenquelle je Beziehungstyp ==========================================
# Diese Zuordnung definiert, aus welcher Tabelle die jeweiligen Beziehungen gespeist werden.
# Wichtig, um beim CSV-Export gezielt die richtigen JSON-Dateien für die Relationship-Generierung zu laden.

RELATION_TABLE_SOURCES = {
    "user_address":   "addresses",         # Adresse enthält user_id → User ↔ Address
    "user_order":     "orders",            # Order enthält user_id → User ↔ Order
    "order_payment":  "payments",          # Payment enthält order_id → Order ↔ Payment
    "order_shipment": "shipments",         # Shipment enthält order_id → Order ↔ Shipment
    "user_wishlist":  "wishlists",         # JSON-basierte Wunschliste ohne Knoten
    "order_contains": "order_items",       # OrderItem als Beziehung zw. Order & Product
    "user_reviewed":  "reviews",           # Review als bewertende Beziehung
    "user_cart":      "cart_items",        # CartItem enthält user_id & product_id
    "user_viewed":    "product_views",     # ProductView enthält user_id & product_id
    "user_purchased": "product_purchases", # ProductPurchase enthält user_id & product_id
}


def stop_neo4j_container():
    # Gibt eine Statusmeldung aus, dass versucht wird, den Container zu stoppen.
    print("🛑 Stoppe laufenden Neo4j-Container falls aktiv ...")
    try:
        # Führt den Docker-Befehl zum Stoppen des Containers aus.
        # Der Befehl gibt keine Ausgabe zurück, da stdout unterdrückt wird.
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True, stdout=subprocess.DEVNULL)

        # Wiederholt bis zu 10 Mal (mit jeweils 1 Sekunde Pause), ob der Container vollständig beendet wurde.
        for _ in range(10):
            # Prüft, ob ein Container mit dem angegebenen Namen noch vorhanden ist.
            result = subprocess.run(
                ["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                capture_output=True,
                text=True
            )
            # Wenn keine Container-ID zurückgegeben wird, wurde der Container erfolgreich gestoppt.
            if not result.stdout.strip():
                print("✅ Container wurde vollständig gestoppt.")
                return
            time.sleep(1)  # Wartezeit vor dem nächsten Versuch

    except Exception as e:
        # Gibt eine Fehlermeldung aus, falls beim Stoppen des Containers ein Problem auftritt.
        print(f"⚠️  Fehler beim Stoppen: {e}")


def start_neo4j_container():
    # Gibt eine Statusmeldung aus, dass der Neo4j-Container gestartet wird.
    print("🚀 Starte Neo4j-Container neu ...")

    # Ermittelt den absoluten Pfad zum lokalen Verzeichnis 'neo4j_data', 
    # in dem die persistente Datenhaltung erfolgen soll.
    data_volume_path = str((Path(__file__).resolve().parent / "neo4j_data").resolve())

    # Startet einen neuen Docker-Container mit den folgenden Parametern:
    # -d: im Hintergrund (detached mode)
    # --rm: Container wird nach dem Stoppen automatisch gelöscht
    # --name: setzt einen festen Containernamen
    # -e: übergibt die Authentifizierungsdaten als Umgebungsvariable
    # -p: leitet Ports für HTTP (7474) und Bolt (7687) weiter
    # -v: bindet das Datenverzeichnis als Volume ein
    # IMAGE_NAME: definiertes Neo4j-Image
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "-e", "NEO4J_AUTH=neo4j/superpassword55",
        "-p", "7474:7474", "-p", "7687:7687",
        "-v", f"{data_volume_path}:/data",
        IMAGE_NAME
    ], check=True)

    # Wartet darauf, dass der Bolt-Endpunkt (Standardprotokoll von Neo4j) erreichbar ist.
    wait_for_bolt()

    # Gibt eine Erfolgsmeldung aus, sobald der Container aktiv ist.
    print("✅ Container läuft.")


def fix_cypher_props(text):
    # Sucht nach Schlüsselbezeichnern in Cypher-Notation (z. B. name: ...) 
    # und wandelt diese in gültige JSON-Schlüssel um (z. B. "name": ...).
    text = re.sub(r"(\w+):", r'"\1":', text)

    # Sucht nach nicht in Anführungszeichen gesetzten String-Werten in Eigenschaftszuweisungen
    # (z. B. : admin) und ergänzt automatisch doppelte Anführungszeichen (→ : "admin").
    text = re.sub(r':\s*([A-Za-z_][A-Za-z0-9_]*)', r': "\1"', text)

    # Gibt den bereinigten Text zurück, der nun syntaktisch korrekt ist für JSON oder Cypher-Mapping.
    return text


def convert_json_to_csv_refactored(json_file: Path, out_dir: Path):
    """Konvertiert ein gegebenes JSON-Datenobjekt in CSV-Dateien für Knoten und Relationen gemäß der vorgegebenen Tabellenstruktur."""

    # Lädt die JSON-Datei und stellt sicher, dass das Ausgabeverzeichnis existiert
    data = json.loads(Path(json_file).read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # --------------------- Verarbeitung der Knoten-Tabellen ---------------------
    for table, header in NODE_TABLES.items():
        rows = data.get(table, [])
        if not rows:
            continue  # Tabelle ist im JSON nicht vorhanden oder leer

        # ➊ Erstellt eine Zuordnung zwischen Attributnamen und deren Typdefinitionen (z. B. 'boolean', 'int', '')
        type_by_key = {
            h.split(":")[0]: (h.split(":")[1] if ":" in h else "")
            for h in header
        }

        # Legt den Pfad zur Ausgabedatei fest und öffnet sie im Schreibmodus
        csv_path = out_dir / f"{table}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)  # schreibt die Kopfzeile mit Typdefinitionen

            def resolve_value(row, key):
                # A) Direkter Zugriff: Wert ist im Datensatz vorhanden
                if key in row:
                    val = row[key]
                # B) Falls nur eine generische 'id' existiert, wird diese für Import-ID-Felder übernommen
                elif key.endswith("_id") and "id" in row:
                    val = row["id"]
                # C) Andernfalls wird der Wert als fehlend markiert
                else:
                    val = None

                # ➋ Typabhängige Umwandlung der Werte
                col_type = type_by_key.get(key, "")
                if col_type == "boolean":
                    # Fehlende Werte werden als 'false' interpretiert,
                    # vorhandene Booleans als lowercase-String zurückgegeben
                    return "true" if bool(val) else "false"
                if val is None:
                    return ""  # fehlende Werte (nicht-boolean) werden als leere Zelle geschrieben
                return val  # alle anderen Typen: unbearbeitet zurückgeben

            # Schreibt die aufbereiteten Datenzeilen in die CSV-Datei
            for row in rows:
                writer.writerow([resolve_value(row, k.split(":")[0])
                                 for k in header])

    # --------------------- Verarbeitung der Relationen-Tabellen ---------------------
    # Baut zunächst eine Zwischenstruktur auf, um Beziehungen zu generieren
    rel_rows = {}
    for rel, source_table in RELATION_TABLE_SOURCES.items():
        if source_table not in data:
            continue  # Quelltabelle fehlt → keine Beziehungen erzeugbar
        rows = data[source_table]
        builder = RELATION_BUILDERS[rel]  # verwendet vordefinierte Builder-Funktion
        rel_rows[rel] = [builder(r) for r in rows]

    # Schreibt die Beziehungsdaten in separate CSV-Dateien
    for rel, rows in rel_rows.items():
        if not rows:
            continue
        with open(out_dir / f"{rel}.csv", "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # Gibt alle erstellten CSV-Dateien als Liste zurück
    return sorted(out_dir.glob("*.csv"))


def wait_for_bolt(uri="bolt://127.0.0.1:7687", auth=("neo4j","superpassword55"),
                  timeout=120, delay=2):
    """
    Wartet auf die erfolgreiche Erreichbarkeit des Neo4j-Bolt-Endpunkts.

    Parameter:
        uri (str): Bolt-Verbindungs-URI (Standard: localhost mit Port 7687)
        auth (tuple): Tuple aus Benutzername und Passwort für den Login
        timeout (int): Maximale Wartezeit in Sekunden
        delay (int): Wartezeit zwischen den Verbindungsversuchen in Sekunden
    """

    t0 = time.time()  # Startzeit zur Berechnung des Timeout

    # Wiederholt Verbindungsversuche bis zum Ablauf der maximalen Wartezeit
    while time.time() - t0 < timeout:
        try:
            # Baut eine Verbindung zur Neo4j-Instanz über den Bolt-Protokolltreiber auf
            with GraphDatabase.driver(uri, auth=auth) as drv:
                with drv.session() as s:
                    # Führt eine einfache Testabfrage aus, um die Betriebsbereitschaft zu prüfen
                    s.run("RETURN 1").consume()
            
            # Gibt eine Erfolgsmeldung aus, wenn Neo4j bereit ist
            print("✅ Neo4j ist bereit.")
            return

        except ServiceUnavailable:
            # Wenn Neo4j noch nicht erreichbar ist, wird kurz gewartet und erneut versucht
            time.sleep(delay)

    # Wird nach Ablauf des Timeouts ausgelöst, falls Neo4j nicht verfügbar ist
    raise RuntimeError("❌ Neo4j kam nicht hoch – Timeout!")


def run_neo4j_import():
    # Gibt eine Statusmeldung zum Start des Importvorgangs aus
    print("📦 Importiere CSV-Dateien in Neo4j (Docker) ...")

    # Ermittelt die absoluten Pfade für das lokale CSV-Verzeichnis und das persistente Datenverzeichnis
    host_import_path = str(CSV_DIR.resolve())
    data_volume_path = str((Path(__file__).resolve().parent / "neo4j_data").resolve())

    # Basis-Befehl zum Starten des Neo4j-Admin-Importprozesses im Docker-Container
    cmd = [
        "docker", "run", "--rm", "--user", "7474:7474",  # Ausführung unter dem Neo4j-User (UID/GID)
        "-v", f"{host_import_path}:/var/lib/neo4j/import",  # Bindet Importverzeichnis ins Container-Dateisystem ein
        "-v", f"{data_volume_path}:/data",                  # Bindet das Datenverzeichnis zur Speicherung ein
        IMAGE_NAME,                                         # Verwendetes Neo4j-Docker-Image
        "neo4j-admin", "database", "import", "full",        # Vollständiger Datenbankimport über Admin-Tool
        "--overwrite-destination=true",                     # Überschreibt bestehende Datenbank (falls vorhanden)
        "--verbose",                                        # Aktiviert detaillierte Konsolenausgabe
        "--normalize-types=false"                           # Deaktiviert automatische Typanpassung
    ]

    # 🔁 Fügt ggf. vorhandene statische CSV-Dateien für definierte Knoten (Nodes) hinzu
    static_nodes = {"Product": "Product.csv", "Category": "Category.csv"}
    static_relationships = ["product_categories"]

    for label, file_name in static_nodes.items():
        node_file = CSV_DIR / file_name
        if node_file.exists():
            cmd.append(f"--nodes={label}=/var/lib/neo4j/import/{file_name}")

    # 🔁 Fügt ggf. vorhandene statische CSV-Dateien für Beziehungen hinzu
    for rel in static_relationships:
        rel_file = CSV_DIR / f"{rel}.csv"
        if rel_file.exists():
            cmd.append(f"--relationships={rel}=/var/lib/neo4j/import/{rel}.csv")

    # 🔁 Dynamisch generierte Knotendateien aus dem vorherigen JSON-Konvertierungsprozess einbinden
    for table, label in NODE_TYPES.items():
        node_file = CSV_DIR / f"{table}.csv"
        if node_file.exists():
            cmd.append(f"--nodes={label}=/var/lib/neo4j/import/{table}.csv")

    # 🔁 Dynamisch generierte Beziehungsdateien hinzufügen
    for rel in RELATION_BUILDERS:
        rel_file = CSV_DIR / f"{rel}.csv"
        if rel_file.exists():
            cmd.append(f"--relationships={rel}=/var/lib/neo4j/import/{rel}.csv")

    # Beendet den Befehl mit dem Namen der zu erstellenden Datenbank ("neo4j")
    cmd += ["--", "neo4j"]

    # Führt den vollständigen Importbefehl aus; bricht bei Fehler ab (check=True)
    subprocess.run(cmd, check=True)

    # Gibt eine Bestätigung über den erfolgreichen Abschluss aus
    print("✅ Import abgeschlossen.")


def cleanup():
    # Gibt eine Statusmeldung aus, dass die temporären CSV-Dateien gelöscht werden
    print("🧹 Lösche CSV-Dateien ...")

    # Durchsucht das Zielverzeichnis nach allen CSV-Dateien und löscht sie einzeln
    for file in CSV_DIR.glob("*.csv"):
        file.unlink()  # entfernt die Datei vom Dateisystem

    # Löscht anschließend das gesamte Verzeichnis, in dem die CSV-Dateien lagen
    shutil.rmtree(CSV_DIR)


def reset_database_directory():
    # Bestimmt den Pfad zum lokalen Datenverzeichnis der Neo4j-Instanz
    db_path = Path(__file__).resolve().parent / "neo4j_data"

    # Prüft, ob der Ordner bereits existiert und ein Verzeichnis ist
    if db_path.exists() and db_path.is_dir():
        print("🧨 Entferne bestehenden Neo4j-Datenbank-Ordner ...")
        shutil.rmtree(db_path)  # Löscht das gesamte Verzeichnis rekursiv
        print("✅ Alter Datenbankordner entfernt.")

    # Erstellt ein neues, leeres Verzeichnis für die Datenbank
    db_path.mkdir(parents=True, exist_ok=True)


def _folder_size_mb(path: Path) -> float:
    """
    Liefert die Größe eines Ordners in MB.
    1️⃣  Versuch via `du -sb`, weil es bei Docker-Setups praktisch immer vorhanden ist.
    2️⃣  Fallback: rekursiv per os.walk – funktioniert auch auf Windows, ist aber langsamer.
    """
    try:
        size_bytes = int(subprocess.check_output(["du", "-sb", str(path)]).split()[0])
    except Exception:                                      # z. B. 'du' nicht verfügbar
        size_bytes = 0
        for root, _, files in os.walk(path):
            size_bytes += sum((Path(root) / f).stat().st_size for f in files)
    return round(size_bytes / (1024 * 1024), 1)            # eine Nachkommastelle

def log_volume_size(variant: str, users: int,
                    volume_path: Path,
                    out_csv: Path = (BASE_DIR / ".." / "results" / "volume_sizes.csv")) -> None:
    """
    Hängt eine Zeile  variant,users,volume_mb  an die Ergebnis-CSV an.

    Parameters
    ----------
    variant      : Kurzbezeichnung (z. B. 'pg_normal', 'neo_optimized')
    users        : Zahl der in dieser Runde importierten Users
    volume_path  : Host-Pfad des gemounteten Volumes (Ordner, nicht Container-ID!)
    out_csv      : Zieldatei; wird angelegt, falls sie noch nicht existiert
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    volume_mb = _folder_size_mb(volume_path)

    # Datei neu anlegen → Header schreiben; sonst anhängen
    write_header = not out_csv.exists()
    with out_csv.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["variant", "users", "volume_mb"])
        w.writerow([variant, users, volume_mb])
    print(f"💾  Volume-Größe protokolliert: {variant} | {users} | {volume_mb} MB")


def main():
    # Initialisiert einen Argumentparser für Kommandozeilenargumente
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", type=int, required=True, 
                        help="Numerischer Suffix der zu importierenden JSON-Datei (z. B. 'users_3.json')")
    parser.add_argument("--json-dir", type=str, default="../output",
                        help="Pfad zum Verzeichnis mit den vorbereiteten JSON-Dateien")
    
    args = parser.parse_args()  # Parst die übergebenen Argumente
    
    # Zusammensetzen des vollständigen Dateipfads basierend auf der übergebenen file-id
    json_file = Path(args.json_dir) / f"users_{args.file_id}.json"

    # 1. Entfernt ggf. vorhandene Datenbankdaten und legt ein frisches Verzeichnis an
    reset_database_directory()

    # 2. Beendet laufende Neo4j-Container (falls vorhanden), um Konflikte zu vermeiden
    stop_neo4j_container()

    # 3. Konvertiert die JSON-Daten in tabellenbasierte CSV-Dateien für den Import
    convert_json_to_csv_refactored(json_file, CSV_DIR)

    # 4. Führt den vollständigen CSV-Import in die Neo4j-Datenbank durch
    run_neo4j_import()

    # 5. Entfernt temporäre CSV-Dateien, um die Arbeitsumgebung aufzuräumen
    cleanup()

    # 6. Startet die Neo4j-Datenbank im Docker-Container und wartet auf vollständige Verfügbarkeit
    start_neo4j_container()

    # 7. Disk-Footprint des frischen Volumes festhalten
    data_volume_path = Path(__file__).resolve().parent / "neo4j_data"
    log_volume_size(variant="neo_optimized",
                    users=args.file_id,           
                    volume_path=data_volume_path)


# Stellt sicher, dass die main()-Funktion nur ausgeführt wird,
# wenn das Skript direkt gestartet wird (nicht bei Modulimport)
if __name__ == "__main__":
    main()