import subprocess
import time
from pathlib import Path
import psycopg2

# ----------------------------- Konfiguration
IMAGE_NAME = "pg17-normal"
CONTAINER_NAME = "pg_test_normal"
POSTGRES_PORT = 5432
DOCKERFILE_DIR = Path("")  # Hier liegt dein Dockerfile
sql_file = Path("setup_postgres_normal.sql")

# ----------------------------- Funktionen

def build_postgres_image():
    """Baut das Docker-Image aus dem Dockerfile."""
    print(f"🛠 Baue Image '{IMAGE_NAME}' aus {DOCKERFILE_DIR} ...")
    subprocess.run([
        "docker", "build", "-t", IMAGE_NAME, "."
    ], cwd=str(DOCKERFILE_DIR), check=True)
    print("✅ Image erfolgreich gebaut.")


def start_postgres_container():
    """Startet den PostgreSQL-Container aus dem Image."""
    print(f"🚀 Starte Container '{CONTAINER_NAME}' aus Image '{IMAGE_NAME}' ...")
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "-e", "POSTGRES_PASSWORD=pass",
        "-e", "POSTGRES_DB=testdb",
        "-p", f"{POSTGRES_PORT}:5432",
        IMAGE_NAME
    ], check=True)
    print("⏳ Warte auf Initialisierung...")
    time.sleep(5)
    print("✅ Container läuft.")


def apply_sql_structure(sql_path: Path):
    """Spielt die SQL-Strukturdatei in die Datenbank ein."""
    print(f"📄 Spiele SQL-Struktur aus {sql_path} ein...")

    try:
        with open(sql_path, "r", encoding="utf-8") as file:
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


def stop_postgres_container():
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


def delete_postgres_image():
    """Löscht das erstellte Docker-Image."""
    print(f"🗑️  Versuche Image '{IMAGE_NAME}' zu löschen...")
    try:
        subprocess.run(["docker", "rmi", IMAGE_NAME], check=True)
        print("✅ Image gelöscht.")
    except subprocess.CalledProcessError:
        print("⚠️  Image konnte nicht gelöscht werden (evtl. Container läuft noch?).")
    except Exception as e:
        print(f"❗ Fehler beim Löschen des Images: {e}")

