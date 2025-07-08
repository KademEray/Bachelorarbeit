import subprocess
import time
from pathlib import Path
from neo4j import GraphDatabase

# ----------------------------- Konfiguration
# Diese Konstanten definieren die grundlegenden Einstellungen für den Aufbau und
# die Steuerung des optimierten Neo4j-Dockercontainers. Sie werden im gesamten
# Setup-Prozess wiederverwendet, um das Builden, Starten und Initialisieren 
# des Containers zu automatisieren.

IMAGE_NAME = "neo5-optimized"                  # Name des Docker-Images für die optimierte Version
CONTAINER_NAME = "neo5_test_optimized"         # Name des Containers für Testzwecke
NEO4J_HTTP_PORT = 7474                         # Port für das Webinterface von Neo4j
NEO4J_BOLT_PORT = 7687                         # Port für den Zugriff per Bolt-Protokoll
DOCKERFILE_DIR = Path("./")                    # Pfad zum Verzeichnis mit dem Dockerfile
cypher_file = Path("./setup_neo4j_optimized.cypher")  # Pfad zur Datei mit Setup-Befehlen (Constraints, Indexe, Relationen)


# ----------------------------- Funktionen
# Diese Funktionen automatisieren die Erstellung und das Starten eines Docker-Containers 
# für eine optimierte Neo4j-Instanz. Sie dienen der Reproduzierbarkeit und vereinfachen
# den manuellen Aufwand bei Setup und Testläufen.

def build_optimized_neo4j_image(DOCKERFILE_DIR: Path = DOCKERFILE_DIR):
    """
    Baut ein Docker-Image für die optimierte Neo4j-Version basierend auf dem angegebenen Dockerfile-Verzeichnis.

    Args:
        DOCKERFILE_DIR (Path): Verzeichnis, das das Dockerfile enthält.
    """
    print(f"🛠 Baue Image '{IMAGE_NAME}' aus {DOCKERFILE_DIR} ...")
    subprocess.run([
        "docker", "build", "-t", IMAGE_NAME, "."
    ], cwd=str(DOCKERFILE_DIR), check=True)
    print("✅ Image erfolgreich gebaut.")


def start_optimized_neo4j_container():
    """
    Startet einen Docker-Container für die optimierte Neo4j-Version.
    Der Container wird mit Umgebungsvariablen für die Authentifizierung
    und den notwendigen Portweiterleitungen gestartet.

    # Startet den Docker-Container im Hintergrund mit folgenden Eigenschaften:
    # -d: detached mode
    # --rm: Container wird automatisch entfernt, wenn er gestoppt wird
    # --name: eindeutiger Containername
    # -e: Übergibt Authentifizierungskonfiguration an Neo4j
    # -p: leitet lokale Ports an Container-Ports weiter (HTTP + Bolt)
    """
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
    time.sleep(15)
    print("✅ Container läuft.")


def apply_optimized_cypher_structure(cypher_file: Path = cypher_file):
    """
    Verbindet sich mit Neo4j über das Bolt-Protokoll und führt die Befehle aus der
    übergebenen Cypher-Datei aus. Mehrere Versuche werden unternommen, um Verzögerungen
    beim Container-Startup abzufangen.

    Args:
        cypher_file (Path): Pfad zur Cypher-Datei mit den Setup-Befehlen.
    """
    print("📡 Verbinde mit Neo4j über Bolt...")

    for attempt in range(10):  # Maximal 10 Verbindungsversuche (insgesamt ca. 30 Sekunden)
        try:
            # Verbindungsaufbau zum lokalen Neo4j über Bolt-Protokoll
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "superpassword55"))
            with driver.session() as session:
                # Cypher-Skript einlesen und in Einzelsätze trennen
                with open(cypher_file, encoding="utf-8") as f:
                    script = f.read()
                statements = [stmt.strip() for stmt in script.split(";") if stmt.strip()]

                # Ausführung jedes Einzelskripts
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
    """
    Stoppt den laufenden Neo4j-Container mit dem Namen CONTAINER_NAME.
    Wartet anschließend, bis der Container vollständig entfernt wurde.
    """
    print(f"🛑 Versuche Container '{CONTAINER_NAME}' zu stoppen...")
    try:
        # Versuche, den Container normal zu stoppen
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True)
        print("🧹 Container gestoppt. Warte auf vollständige Entfernung...")

        # Wiederhole 10x im Abstand von 1 Sekunde, ob der Container entfernt wurde
        for i in range(10):
            result = subprocess.run(
                ["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                capture_output=True, text=True
            )
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
        build_optimized_neo4j_image()
        start_optimized_neo4j_container()
        apply_optimized_cypher_structure()
        # Optional: weitere Logik oder Testdaten einfügen
    except Exception as e:
        print(f"❗ Ein Fehler ist aufgetreten: {e}")

if __name__ == "__main__":
    main()