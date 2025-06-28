import argparse          # Ermöglicht das Einlesen und Verarbeiten von Kommandozeilenargumenten
import json              # Zum Parsen von JSON-Dateien
import csv               # Für das Schreiben von CSV-Dateien (z. B. für den Neo4j-Import)
import subprocess        # Zur Ausführung externer Shell-Kommandos (z. B. Docker)
import time              # Zur Steuerung von Pausen und Wartezeiten
from pathlib import Path # Plattformunabhängiges Arbeiten mit Dateipfaden
from tqdm import tqdm    # Fortschrittsbalken für lange Operationen (z. B. Dateioperationen)
import ast               # Abstrakter Syntaxbaum – hier ggf. für sichere Auswertung von Literal-Ausdrücken
import re                # Reguläre Ausdrücke, z. B. für Datenbereinigung
import shutil            # Dateisystemoperationen (z. B. Ordner löschen)
import socket            # Für Netzwerkprüfungen (z. B. ob Bolt-Port erreichbar ist)

from neo4j import GraphDatabase                    # Offizieller Python-Treiber für Neo4j (Bolt-Verbindung)
from neo4j.exceptions import ServiceUnavailable    # Fehlerbehandlung bei nicht erreichbarem Neo4j-Service

# === Konfiguration der Arbeitsumgebung ===

IMPORT_DIR = Path(__file__).resolve().parent / "import"  # Verzeichnis für importierte CSV-Dateien
CSV_DIR = IMPORT_DIR                                     # Alias zur Vereinfachung im Code

NEO4J_BIN = "/var/lib/neo4j/bin/neo4j-admin"             # (Optional) Pfad zum Admin-Tool innerhalb des Containers
CONTAINER_NAME = "neo5_test_optimized"                   # Eindeutiger Name für den Docker-Container (Optimized-Version)
IMAGE_NAME = "neo5-optimized"                            # Name des zu verwendenden Docker-Images (Optimized-Version)


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
    """
    Stoppt den laufenden Docker-Container für Neo4j (falls aktiv).
    Wartet anschließend auf die vollständige Entfernung aus dem System.
    """
    print("🛑 Stoppe laufenden Neo4j-Container falls aktiv ...")
    try:
        subprocess.run(
            ["docker", "stop", CONTAINER_NAME],
            check=True,
            stdout=subprocess.DEVNULL
        )
        for _ in range(10):
            result = subprocess.run(
                ["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                capture_output=True, text=True
            )
            if not result.stdout.strip():
                print("✅ Container wurde vollständig gestoppt.")
                return
            time.sleep(1)
    except Exception as e:
        print(f"⚠️  Fehler beim Stoppen: {e}")


def start_neo4j_container():
    """
    Startet den optimierten Neo4j-Container im Docker.
    - Mountet ein lokales Volume unter ./neo4j_data nach /data im Container (für persistente Datenhaltung).
    - Setzt Standard-Ports für HTTP (7474) und Bolt (7687).
    - Verwendet die Umgebungsvariable zur Authentifizierung.
    - Wartet nach dem Start auf Erreichbarkeit des Bolt-Endpunkts.
    """
    print("🚀 Starte Neo4j-Container neu ...")

    # Absoluter Pfad zum lokalen Datenverzeichnis für /data-Mount
    data_volume_path = str((Path(__file__).resolve().parent / "neo4j_data").resolve())

    # Docker-Container starten
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "-e", "NEO4J_AUTH=neo4j/superpassword55",  # Zugangsdaten
        "-p", "7474:7474", "-p", "7687:7687",       # Ports veröffentlichen
        "-v", f"{data_volume_path}:/data",          # Datenverzeichnis mounten
        IMAGE_NAME
    ], check=True)

    # Auf Erreichbarkeit des Neo4j-Bolt-Protokolls warten
    wait_for_bolt()
    print("✅ Container läuft.")


def fix_cypher_props(text):
    """
    Hilfsfunktion zur Formatkorrektur von Property-Zuweisungen in Cypher-Zeilen (Text-Ebene).
    - Wandelt z. B. name:Max → "name":"Max" um, um gültige JSON/Cypher-Syntax zu gewährleisten.
    - Wichtig für dynamisch erzeugte CSV-Dateien mit Properties in Neo4j-Importen.
    """
    # Erster Schritt: Schlüssel in Anführungszeichen setzen → z.B. name: → "name":
    text = re.sub(r"(\w+):", r'"\1":', text)

    # Zweiter Schritt: unquoted Werte in Anführungszeichen setzen → z.B. :abc → : "abc"
    text = re.sub(r':\s*([A-Za-z_][A-Za-z0-9_]*)', r': "\1"', text)

    return text


def convert_json_to_csv_refactored(json_file: Path, out_dir: Path):
    """
    Konvertiert eine strukturierte JSON-Datei in CSV-Dateien für den Neo4j-Import.

    - Jeder JSON-Abschnitt (z. B. 'users', 'orders') wird basierend auf der NODE_TABLES-Definition
      in eine eigene CSV-Datei umgewandelt.
    - Sowohl die technische Import-ID (z. B. user_id:ID(User)) als auch die fachliche ID (id:int)
      werden berücksichtigt.
    - Felder mit booleschen Werten werden korrekt in "true"/"false" übersetzt.
    - Beziehungen (z. B. :PLACED, :HAS_ADDRESS) werden mithilfe vordefinierter Builder generiert
      und in separate CSV-Dateien geschrieben.

    Parameter:
    ----------
    json_file : Path
        Pfad zur JSON-Datei mit den exportierten Daten.
    out_dir : Path
        Zielverzeichnis für die generierten CSV-Dateien.

    Rückgabe:
    ---------
    List[Path]
        Alphabetisch sortierte Liste aller erzeugten CSV-Dateien.
    """

    data = json.loads(Path(json_file).read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------- Verarbeitung der Knoten-Tabellen -------------------
    for table, header in NODE_TABLES.items():
        rows = data.get(table, [])
        if not rows:
            continue

        # Zuordnung der Spaltennamen zu ihren Typangaben (z. B. int, boolean)
        type_by_key = {
            h.split(":")[0]: (h.split(":")[1] if ":" in h else "")
            for h in header
        }

        csv_path = out_dir / f"{table}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)

            def resolve_value(row, key):
                # A) Wert im JSON vorhanden → direkt übernehmen
                if key in row:
                    val = row[key]
                # B) Import-ID-Spalte vorhanden → setze auf fachliche ID
                elif key.endswith("_id") and "id" in row:
                    val = row["id"]
                else:
                    val = None  # fehlender Wert

                # Typ-spezifische Formatierung
                col_type = type_by_key.get(key, "")
                if col_type == "boolean":
                    return "true" if bool(val) else "false"
                if val is None:
                    return ""
                return val

            # Zeilenweise schreiben
            for row in rows:
                writer.writerow([resolve_value(row, k.split(":")[0])
                                 for k in header])

    # ------------------- Verarbeitung der Beziehungen -----------------------
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

    # Rückgabe der generierten CSV-Dateien
    return sorted(out_dir.glob("*.csv"))


def wait_for_bolt(uri="bolt://127.0.0.1:7687", auth=("neo4j", "superpassword55"),
                  timeout=120, delay=2):
    """
    Wartet auf die Verfügbarkeit der Neo4j-Bolt-Schnittstelle.

    Diese Funktion versucht wiederholt, eine Verbindung zur Bolt-API des Neo4j-Datenbankservers
    herzustellen. Sie wird typischerweise nach dem Start eines Docker-Containers verwendet, um
    sicherzustellen, dass der Dienst vollständig initialisiert wurde, bevor weitere Operationen
    wie das Ausführen von Cypher-Skripten beginnen.

    Parameter:
    ----------
    uri : str
        Bolt-URL, unter der Neo4j erreichbar sein soll (Standard: "bolt://127.0.0.1:7687").

    auth : Tuple[str, str]
        Zugangsdaten (Benutzername, Passwort) zur Authentifizierung bei Neo4j.

    timeout : int
        Maximale Wartezeit in Sekunden, bevor ein Fehler ausgelöst wird (Standard: 120 s).

    delay : int
        Wartezeit in Sekunden zwischen zwei Verbindungsversuchen (Standard: 2 s).

    Raises:
    -------
    RuntimeError
        Wenn die Datenbank nach Ablauf des Timeouts nicht erreichbar ist.

    Beispiel:
    ---------
    wait_for_bolt()  # wartet maximal 2 Minuten, bis Neo4j verfügbar ist
    """
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
    """
    Führt den vollständigen Datenimport in eine Neo4j-Datenbank durch.

    Diese Funktion verwendet den `neo4j-admin import`-Befehl innerhalb eines Docker-Containers,
    um eine vollständig neue Datenbankinstanz mit CSV-Dateien aus der lokalen Umgebung
    zu befüllen. Die Daten bestehen sowohl aus statischen Dateien (z. B. Produkte, Kategorien)
    als auch dynamisch generierten CSV-Dateien für Nodes und Beziehungen.

    Importiert werden:
    - Statische Knoten wie Produkte und Kategorien
    - Statische Beziehungen wie `product_categories`
    - Dynamisch generierte Nodes gemäß `NODE_TYPES`
    - Dynamisch generierte Beziehungen gemäß `RELATION_BUILDERS`

    Die Option `--overwrite-destination=true` sorgt dafür, dass bei jedem Import
    die bestehende Datenbank überschrieben wird. Die Pfade zur Import- und Datenbankstruktur
    werden mithilfe von Docker-Volumes bereitgestellt, sodass der Container temporär 
    ausgeführt werden kann (`--rm`).

    Voraussetzungen:
    ----------------
    - CSV-Dateien müssen im Verzeichnis `CSV_DIR` vorhanden sein.
    - Docker-Image muss unter dem Namen `IMAGE_NAME` gebaut worden sein.
    - Das lokale Volume `neo4j_data/` enthält das Neo4j-Datenverzeichnis.

    Ablauf:
    -------
    1. Statische und dynamische Dateien werden dem Kommando als `--nodes` und `--relationships` übergeben.
    2. Der Container führt den Import in ein neues Datenbankverzeichnis aus.
    3. Nach Abschluss wird der Container automatisch gelöscht.

    Hinweis:
    --------
    Diese Methode funktioniert nur mit ausgeschaltetem Neo4j-Container, da der Import
    exklusiven Zugriff auf das Datenverzeichnis benötigt.

    Beispiel:
    ---------
    run_neo4j_import()  # führt den Import auf Basis der vorbereiteten CSV-Dateien aus
    """

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
    """
    Entfernt alle temporär erstellten CSV-Dateien im Importverzeichnis.

    Nach erfolgreichem Import in die Neo4j-Datenbank werden die erzeugten CSV-Dateien
    aus dem `CSV_DIR` gelöscht. Zusätzlich wird das gesamte Verzeichnis rekursiv entfernt,
    um Speicherplatz freizugeben und eine saubere Arbeitsumgebung zu gewährleisten.
    """
    print("🧹 Lösche CSV-Dateien ...")
    for file in CSV_DIR.glob("*.csv"):
        file.unlink()
    shutil.rmtree(CSV_DIR)


def reset_database_directory():
    """
    Setzt das lokale Datenbankverzeichnis (`neo4j_data`) zurück.

    Für den `neo4j-admin import` ist ein leerer Datenbankordner notwendig.
    Falls bereits ein Ordner mit dem Namen `neo4j_data` existiert, wird dieser
    vollständig gelöscht und anschließend neu erstellt.
    """
    db_path = Path(__file__).resolve().parent / "neo4j_data"
    if db_path.exists() and db_path.is_dir():
        print("🧨 Entferne bestehenden Neo4j-Datenbank-Ordner ...")
        shutil.rmtree(db_path)
        print("✅ Alter Datenbankordner entfernt.")
    db_path.mkdir(parents=True, exist_ok=True)


def main():
    """
    Hauptfunktion für den vollständigen Import einer JSON-Datei in Neo4j.

    Diese Funktion stellt die zentrale Ablaufsteuerung für die datenbankseitige Verarbeitung dar.
    Sie akzeptiert ein JSON-Datei-Argument via Kommandozeile (`--file-id`) und führt folgende Schritte aus:

    1. Setzt das Datenbankverzeichnis zurück.
    2. Stoppt ggf. einen laufenden Neo4j-Container.
    3. Konvertiert die übergebene JSON-Datei in das CSV-Importformat.
    4. Führt einen vollständigen Datenbankimport mit `neo4j-admin` durch.
    5. Entfernt temporäre CSV-Dateien.
    6. Startet den Neo4j-Container mit der frisch importierten Datenbank.

    Hinweis:
    --------
    Diese Methode eignet sich insbesondere für Performancevergleiche mit wachsender Datenmenge,
    da sie jedes Mal eine frische Datenbank mit konsistenter Struktur erstellt.

    Kommandozeilenargumente:
    ------------------------
    --file-id     : Gibt die ID der zu verarbeitenden JSON-Datei an (z. B. `users_1.json`).
    --json-dir    : Pfad zum Verzeichnis, in dem die JSON-Dateien gespeichert sind (optional).
    """

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
