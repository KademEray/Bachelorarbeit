import subprocess
import time
from pathlib import Path
import psycopg2

# ----------------------------- Konfiguration
IMAGE_NAME = "pg17-optimized"                       # Name des Docker-Images
CONTAINER_NAME = "pg_test_optimized"                # Eindeutiger Containername zur Referenzierung
POSTGRES_PORT = 5432                                # Port, auf dem PostgreSQL im Container läuft
DOCKERFILE_DIR = Path("./")                         # Pfad zum Verzeichnis, das das Dockerfile enthält
sql_file = Path("./setup_postgres_optimized.sql")   # Pfad zur SQL-Datei mit Strukturdefinitionen

# ----------------------------- Funktionen

def build_optimized_postgres_image(DOCKERFILE_DIR: Path = DOCKERFILE_DIR):
    """Baut das Docker-Image aus dem Dockerfile für die optimierte PostgreSQL-Version."""
    print(f"🛠 Baue Image '{IMAGE_NAME}' aus {DOCKERFILE_DIR} ...")
    subprocess.run([
        "docker", "build", "-t", IMAGE_NAME, "."
    ], cwd=str(DOCKERFILE_DIR), check=True)
    print("✅ Image erfolgreich gebaut.")


def start_optimized_postgres_container():
    """Startet den optimierten PostgreSQL-Container aus dem zuvor gebauten Docker-Image."""
    print(f"🚀 Starte Container '{CONTAINER_NAME}' aus Image '{IMAGE_NAME}' ...")
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "--shm-size", "10g",
        "-e", "POSTGRES_PASSWORD=pass",
        "-e", "POSTGRES_DB=testdb",
        "-p", f"{POSTGRES_PORT}:5432",
        IMAGE_NAME
    ], check=True)
    print("⏳ Warte auf Initialisierung...")
    time.sleep(15)
    print("✅ Container läuft.")


def apply_optimized_sql_structure(sql_file: Path = sql_file):
    """Spielt die SQL-Strukturdatei in die optimierte PostgreSQL-Datenbank ein."""
    print(f"📄 Spiele SQL-Struktur aus {sql_file} ein...")

    try:
        # Öffnet die SQL-Datei mit der definierten Tabellenstruktur
        with open(sql_file, "r", encoding="utf-8") as file:
            sql = file.read()

        # Verbindet sich zur lokalen PostgreSQL-Datenbank
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="pass",
            dbname="testdb"
        )

        # Führt den SQL-Befehl (bzw. die Befehle) zur Erstellung der Tabellen aus
        with conn.cursor() as cur:
            cur.execute(sql)

        # Bestätigt alle Änderungen dauerhaft in der Datenbank
        conn.commit()
        conn.close()
        print("✅ Struktur erfolgreich eingespielt.")

    except Exception as e:
        # Fehlerbehandlung bei Verbindungsproblemen oder fehlerhaftem SQL-Code
        print(f"❌ Fehler beim Einspielen der Struktur: {e}")
        return

        
def stop_optimized_postgres_container():
    """
    Stoppt den laufenden optimierten PostgreSQL-Docker-Container
    und wartet, bis dieser vollständig entfernt wurde.
    """
    print(f"🛑 Versuche Container '{CONTAINER_NAME}' zu stoppen...")
    try:
        # Versucht, den Container über Docker zu stoppen
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True)
        print("🧹 Container gestoppt. Warte auf vollständige Entfernung...")

        # Überprüft für maximal 10 Sekunden, ob der Container auch wirklich entfernt wurde
        for i in range(10):
            result = subprocess.run(
                ["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                capture_output=True, text=True
            )
            if not result.stdout.strip():
                print("✅ Container wurde vollständig entfernt.")
                return
            time.sleep(1)

        # Hinweis, wenn Container nach Wartezeit noch vorhanden ist
        print("⚠️  Container noch nicht entfernt nach Timeout.")

    except subprocess.CalledProcessError:
        # Fall: Container war bereits gestoppt oder nicht vorhanden
        print("⚠️  Container war nicht aktiv oder konnte nicht gestoppt werden.")
    except Exception as e:
        # Allgemeine Fehlerbehandlung bei unerwarteten Ausnahmen
        print(f"❗ Fehler beim Stoppen des Containers: {e}")


def delete_optimized_postgres_image():
    """
    Löscht das zuvor gebaute Docker-Image für die optimierte PostgreSQL-Konfiguration.
    Diese Funktion wird typischerweise zur Bereinigung nach Tests verwendet.
    """
    print(f"🗑️  Versuche Image '{IMAGE_NAME}' zu löschen...")
    try:
        # Löscht das angegebene Docker-Image
        subprocess.run(["docker", "rmi", IMAGE_NAME], check=True)
        print("✅ Image gelöscht.")
    except subprocess.CalledProcessError:
        # Fall: Image konnte nicht gelöscht werden (z. B. weil Container noch aktiv ist)
        print("⚠️  Image konnte nicht gelöscht werden (evtl. Container läuft noch?).")
    except Exception as e:
        # Allgemeine Fehlerbehandlung
        print(f"❗ Fehler beim Löschen des Images: {e}")


def main():
    """
    Hauptfunktion zur Initialisierung des optimierten PostgreSQL-Containers:
    - Baut das Docker-Image basierend auf der optimierten Konfiguration
    - Startet den Container
    - Spielt die vorbereitete Datenbankstruktur ein
    Weitere Schritte wie Datenimport oder Performance-Messungen können hier ergänzt werden.
    """
    try:
        # Initialisierungsschritte
        build_optimized_postgres_image()
        start_optimized_postgres_container()
        apply_optimized_sql_structure()

        # Platzhalter für erweiterbare Aufgaben wie Datenimport oder Tests
        # z. B. insert_data_to_optimized_postgres(file_id)

    except Exception as e:
        # Abfangen aller Fehler, die während der Initialisierung auftreten
        print(f"❗ Ein Fehler ist aufgetreten: {e}")


if __name__ == "__main__":
    main()