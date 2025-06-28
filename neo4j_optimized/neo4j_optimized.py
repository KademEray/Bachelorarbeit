import subprocess
import time
from pathlib import Path
from neo4j import GraphDatabase

# ----------------------------- Konfiguration
IMAGE_NAME = "neo5-optimized"
CONTAINER_NAME = "neo5_test_optimized"
NEO4J_HTTP_PORT = 7474
NEO4J_BOLT_PORT = 7687
DOCKERFILE_DIR = Path("./")  # Hier liegt dein Dockerfile
cypher_file = Path("./setup_neo4j_optimized.cypher")

# ----------------------------- Funktionen

def build_optimized_neo4j_image(DOCKERFILE_DIR: Path = DOCKERFILE_DIR):
    """Baut das Docker-Image aus dem Dockerfile."""
    print(f"🛠 Baue Image '{IMAGE_NAME}' aus {DOCKERFILE_DIR} ...")
    subprocess.run([
        "docker", "build", "-t", IMAGE_NAME, "."
    ], cwd=str(DOCKERFILE_DIR), check=True)
    print("✅ Image erfolgreich gebaut.")


def start_optimized_neo4j_container():
    """Startet den Neo4j-Container aus dem Image."""
    print(f"🚀 Starte Container '{CONTAINER_NAME}' aus Image '{IMAGE_NAME}' ...")
    subprocess.run([
        "docker", "run", "-d", "--rm",
        "--name", CONTAINER_NAME,
        "-e", "NEO4J_AUTH=neo4j/superpassword55",
        "-p", f"{NEO4J_HTTP_PORT}:7474",
        "-p", f"{NEO4J_BOLT_PORT}:7687",
        IMAGE_NAME
    ], check=True)
    print("⏳ Warte auf Initialisierung...")
    time.sleep(10)
    print("✅ Container läuft.")


def apply_optimized_cypher_structure(cypher_file: Path = cypher_file):
    print("📡 Verbinde mit Neo4j über Bolt...")

    for attempt in range(10):  # 10 Versuche, 3 Sekunden Abstand = max 30 Sekunden
        try:
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "superpassword55"))
            with driver.session() as session:
                with open(cypher_file, encoding="utf-8") as f:
                    script = f.read()
                statements = [stmt.strip() for stmt in script.split(";") if stmt.strip()]
                for stmt in statements:
                    session.run(stmt)
            driver.close()
            print("✅ Cypher-Script erfolgreich eingespielt.")
            return
        except Exception as e:
            print(f"⏳ Versuch {attempt+1}/10 fehlgeschlagen: {e}")
            time.sleep(3)

    raise RuntimeError("❌ Verbindung zu Neo4j konnte nicht hergestellt werden.")


def stop_optimized_neo4j_container():
    print(f"🛑 Versuche Container '{CONTAINER_NAME}' zu stoppen...")
    try:
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True)
        print("🧹 Container gestoppt. Warte auf vollständige Entfernung...")

        for i in range(10):
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


def delete_optimized_neo4j_image():
    """Löscht das erstellte Docker-Image."""
    print(f"🗑️  Versuche Image '{IMAGE_NAME}' zu löschen...")
    try:
        subprocess.run(["docker", "rmi", IMAGE_NAME], check=True)
        print("✅ Image gelöscht.")
    except subprocess.CalledProcessError:
        print("⚠️  Image konnte nicht gelöscht werden (evtl. Container läuft noch?).")
    except Exception as e:
        print(f"❗ Fehler beim Löschen des Images: {e}")

def main():
    try:
        # 1) Setup
        build_optimized_neo4j_image()
        start_optimized_neo4j_container()
        apply_optimized_cypher_structure()

        # Hier kannst du weitere Schritte hinzufügen, z.B. Daten einfügen oder Abfragen durchführen

    except Exception as e:
        print(f"❗ Ein Fehler ist aufgetreten: {e}")

if __name__ == "__main__":
    main()