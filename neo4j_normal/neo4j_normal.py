import subprocess         # Für die Ausführung externer Shell-Kommandos (z. B. Docker-Befehle)
import time               # Steuerung von Wartezeiten (z. B. nach dem Start des Containers)
from pathlib import Path  # Plattformunabhängige Arbeit mit Dateipfaden
from neo4j import GraphDatabase  # Offizieller Python-Treiber zur Verbindung mit Neo4j über Bolt

# ----------------------------- Konfiguration -------------------------------------
# Diese Variablen definieren zentrale Parameter für den Betrieb eines Neo4j-Containers
# sowie den Zugriff auf zugehörige Ressourcen wie Cypher-Skripte und Portfreigaben.

IMAGE_NAME = "neo5-normal"                     # Name des zu verwendenden Docker-Images
CONTAINER_NAME = "neo5_test_normal"            # Eindeutiger Name für den Neo4j-Container
NEO4J_HTTP_PORT = 7474                         # Port für HTTP-Zugriff auf die Neo4j-Oberfläche
NEO4J_BOLT_PORT = 7687                         # Port für Bolt-Protokoll (für Cypher-Queries über API)

DOCKERFILE_DIR = Path("./")                    # Verzeichnis, in dem sich das Dockerfile befindet
cypher_file = Path("./setup_neo4j_normal.cypher")  # Pfad zur Cypher-Datei mit Setup-Befehlen für die Datenbank


# ----------------------------- Funktionen ----------------------------------------

def build_normal_neo4j_image(DOCKERFILE_DIR: Path = DOCKERFILE_DIR):
    """
    Baut ein Docker-Image auf Basis des im angegebenen Verzeichnis liegenden Dockerfiles.

    Parameter:
        DOCKERFILE_DIR (Path): Pfad zum Verzeichnis, in dem sich das Dockerfile befindet.
                               Standardmäßig wird das aktuelle Arbeitsverzeichnis verwendet.
    """

    # Gibt eine Statusmeldung zum Start des Build-Prozesses aus
    print(f"🛠 Baue Image '{IMAGE_NAME}' aus {DOCKERFILE_DIR} ...")

    # Führt den Docker-Build-Befehl aus und weist ihm das angegebene Verzeichnis als Build-Kontext zu
    subprocess.run([
        "docker", "build", "-t", IMAGE_NAME, "."  # -t: Tag (Name) des Images, .: Kontextverzeichnis
    ], cwd=str(DOCKERFILE_DIR), check=True)       # cwd gibt an, in welchem Verzeichnis der Befehl ausgeführt wird

    # Erfolgsmeldung nach erfolgreichem Build
    print("✅ Image erfolgreich gebaut.")


def start_normal_neo4j_container():
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


def apply_normal_cypher_structure(cypher_file: Path = cypher_file):
    """
    Verbindet sich mit der laufenden Neo4j-Instanz über das Bolt-Protokoll und führt
    ein vorbereitetes Cypher-Skript zur Strukturdefinition (z. B. Constraints, Indizes)
    aus.

    Parameter:
        cypher_file (Path): Pfad zur .cypher-Datei mit den auszuführenden Anweisungen.
    """

    print("📡 Verbinde mit Neo4j über Bolt...")

    # Führt bis zu 10 Verbindungsversuche mit 3 Sekunden Pause durch (z. B. bei verzögerter Initialisierung)
    for attempt in range(10):  # 10 Versuche, 3 Sekunden Abstand = max. 30 Sekunden Wartezeit
        try:
            # Öffnet eine Verbindung zur lokalen Neo4j-Instanz (Bolt-Protokoll)
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "superpassword55"))

            # Führt das Skript zeilenweise im Rahmen einer Session aus
            with driver.session() as session:
                with open(cypher_file, encoding="utf-8") as f:
                    script = f.read()

                # Zerlegt das Skript anhand von Semikolons in Einzelsätze, filtert Leerzeilen
                statements = [stmt.strip() for stmt in script.split(";") if stmt.strip()]
                
                # Führt jeden Cypher-Befehl einzeln aus
                for stmt in statements:
                    session.run(stmt)

            driver.close()
            print("✅ Cypher-Script erfolgreich eingespielt.")
            return

        except Exception as e:
            print(f"⏳ Versuch {attempt+1}/10 fehlgeschlagen: {e}")
            time.sleep(3)  # kurze Pause vor erneutem Versuch

    # Wenn nach allen Versuchen keine Verbindung möglich ist, wird der Prozess abgebrochen
    raise RuntimeError("❌ Verbindung zu Neo4j konnte nicht hergestellt werden.")


def stop_normal_neo4j_container():
    """
    Versucht, den laufenden Neo4j-Docker-Container kontrolliert zu stoppen.
    Nach dem Stopp wird überprüft, ob der Container vollständig entfernt wurde.
    """

    print(f"🛑 Versuche Container '{CONTAINER_NAME}' zu stoppen...")

    try:
        # Führt den Docker-Befehl zum Stoppen des Containers aus
        subprocess.run(["docker", "stop", CONTAINER_NAME], check=True)
        print("🧹 Container gestoppt. Warte auf vollständige Entfernung...")

        # Wiederholt bis zu 10 Mal die Überprüfung, ob der Container nicht mehr gelistet ist
        for i in range(10):
            result = subprocess.run(
                ["docker", "ps", "-a", "-q", "-f", f"name={CONTAINER_NAME}"],
                capture_output=True, text=True
            )

            # Wenn der Container nicht mehr vorhanden ist (leere Ausgabe), Abbruch der Schleife
            if not result.stdout.strip():
                print("✅ Container wurde vollständig entfernt.")
                return

            # Andernfalls 1 Sekunde warten und erneut prüfen
            time.sleep(1)

        print("⚠️  Container noch nicht entfernt nach Timeout.")

    except subprocess.CalledProcessError:
        # Container war bereits gestoppt oder nicht existent
        print("⚠️  Container war nicht aktiv oder konnte nicht gestoppt werden.")

    except Exception as e:
        # Unerwarteter Fehler beim Stoppen
        print(f"❗ Fehler beim Stoppen des Containers: {e}")


def delete_normal_neo4j_image():
    """
    Löscht das zuvor erstellte Docker-Image (z. B. zur Bereinigung nach Tests).
    Hinweis: Das Image kann nur gelöscht werden, wenn kein aktiver Container darauf basiert.
    """

    print(f"🗑️  Versuche Image '{IMAGE_NAME}' zu löschen...")
    try:
        # Führt den Docker-Befehl zum Entfernen des Images aus
        subprocess.run(["docker", "rmi", IMAGE_NAME], check=True)
        print("✅ Image gelöscht.")

    except subprocess.CalledProcessError:
        # Fehler bei der Ausführung des Befehls, z. B. weil das Image noch in Benutzung ist
        print("⚠️  Image konnte nicht gelöscht werden (evtl. Container läuft noch?).")

    except Exception as e:
        # Allgemeiner Fehlerfall (z. B. Pfad- oder Systemfehler)
        print(f"❗ Fehler beim Löschen des Images: {e}")


def main():
    """
    Hauptfunktion zur schrittweisen Ausführung des Aufbaus der Neo4j-Datenbankumgebung:
    1) Docker-Image bauen
    2) Container starten
    3) Cypher-Struktur anwenden
    """

    try:
        # 1) Erzeugt das Docker-Image aus dem Dockerfile
        build_normal_neo4j_image()

        # 2) Startet einen neuen Container auf Basis des erzeugten Images
        start_normal_neo4j_container()

        # 3) Spielt ein vorbereitetes Cypher-Skript zur Definition von Indizes, Constraints etc. ein
        apply_normal_cypher_structure()

    except Exception as e:
        # Globaler Fehlerhandler für alle Schritte
        print(f"❌ Fehler während des Prozesses: {e}")


# Führt das Hauptprogramm nur aus, wenn das Skript direkt gestartet wurde
if __name__ == "__main__":
    main()
