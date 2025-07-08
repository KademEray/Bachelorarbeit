import subprocess
import time
from pathlib import Path
import psycopg2

# ----------------------------- Konfiguration
IMAGE_NAME = "pg17-normal"               # Name des zu erstellenden Docker-Images
CONTAINER_NAME = "pg_test_normal"        # Eindeutiger Name für den Container
POSTGRES_PORT = 5432                     # Port, über den PostgreSQL erreichbar ist
DOCKERFILE_DIR = Path("./")              # Pfad zum Verzeichnis mit dem Dockerfile
sql_file = Path("./setup_postgres_normal.sql")  # SQL-Skript mit der Strukturdefinition


# ----------------------------- Funktionen

def build_normal_postgres_image(DOCKERFILE_DIR: Path = DOCKERFILE_DIR):
    """
    Baut ein Docker-Image mit einem angepassten PostgreSQL-Setup.

    Das Image basiert auf dem in DOCKERFILE_DIR befindlichen Dockerfile
    und enthält bereits die initiale Konfiguration (z. B. Tuning-Parameter).
    """
    print(f"🛠 Baue Image '{IMAGE_NAME}' aus {DOCKERFILE_DIR} ...")
    subprocess.run([
        "docker", "build", "-t", IMAGE_NAME, "."
    ], cwd=str(DOCKERFILE_DIR), check=True)
    print("✅ Image erfolgreich gebaut.")


def start_normal_postgres_container():
    """
    Startet einen neuen PostgreSQL-Container auf Basis des zuvor erstellten Images.

    Der Container wird im Hintergrund ausgeführt und nach dem Stoppen automatisch gelöscht (--rm).
    Es werden Umgebungsvariablen für das Passwort und die Datenbank gesetzt.
    """
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


def apply_normal_sql_structure(sql_file: Path = sql_file):
    """
    Spielt das Datenbankschema aus einer SQL-Datei in die laufende PostgreSQL-Datenbank ein.

    Die SQL-Datei enthält typischerweise die Definitionen für Tabellen, Indizes und Constraints.
    Die Verbindung wird direkt zum lokal laufenden Container aufgebaut.
    """
    print(f"📄 Spiele SQL-Struktur aus {sql_file} ein...")

    try:
        with open(sql_file, "r", encoding="utf-8") as file:
            sql = file.read()

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="pass",
            dbname="testdb"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()
        print("✅ Struktur erfolgreich eingespielt.")
    except Exception as e:
        print(f"❌ Fehler beim Einspielen der Struktur: {e}")
        return


def stop_normal_postgres_container():
    """
    Stoppt den laufenden PostgreSQL-Container und wartet auf dessen vollständige Entfernung.

    Diese Funktion ist hilfreich, um sicherzustellen, dass keine Altinstanzen beim Wiederaufbau stören.
    Es wird überprüft, ob der Container nach dem Stoppen tatsächlich nicht mehr existiert.
    """
    print(f"🛑 Versuche Container '{CONTAINER_NAME}' zu stoppen...")
    try:
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True)
        print("🧹 Container gestoppt. Warte auf vollständige Entfernung...")

        # Warte, bis Docker den Container intern wirklich entfernt hat
        for i in range(10):  # max. 10 Sekunden
            result = subprocess.run(["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                                    capture_output=True, text=True)
            if not result.stdout.strip():
                print("✅ Container wurde vollständig entfernt.")
                return
            time.sleep(1)

        print("⚠️  Container noch nicht entfernt nach Timeout.")
    except subprocess.CalledProcessError:
        print("⚠️  Container war nicht aktiv oder konnte nicht gestoppt werden.")
    except Exception as e:
        print(f"❗ Fehler beim Stoppen des Containers: {e}")


def delete_normal_postgres_image():
    """
    Entfernt das erstellte Docker-Image für PostgreSQL.

    Diese Funktion ist nützlich zur Bereinigung nach einem Testlauf oder vor dem erneuten Aufbau eines Images.
    """
    print(f"🗑️  Versuche Image '{IMAGE_NAME}' zu löschen...")
    try:
        subprocess.run(["docker", "rmi", IMAGE_NAME], check=True)
        print("✅ Image gelöscht.")
    except subprocess.CalledProcessError:
        print("⚠️  Image konnte nicht gelöscht werden (evtl. Container läuft noch?).")
    except Exception as e:
        print(f"❗ Fehler beim Löschen des Images: {e}")

def main():
    """
    Hauptablauf zur Ausführung der vorbereitenden Schritte:
    - Bauen des Docker-Images
    - Starten des Containers
    - Einspielen der Datenbankstruktur

    Kann durch weitere Schritte wie Datenimport oder Performance-Messungen ergänzt werden.
    """
    try:
        # 1) Setup
        build_normal_postgres_image()
        start_normal_postgres_container()
        apply_normal_sql_structure()

        # Hier kannst du weitere Schritte hinzufügen, z.B. Daten einfügen oder Benchmarks durchführen

    except Exception as e:
        print(f"❗ Ein Fehler ist aufgetreten: {e}")


if __name__ == "__main__":
    main()