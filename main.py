# === Hauptskript main.py ===
# Dieses Skript orchestriert den vollständigen Benchmark-Ablauf für PostgreSQL und Neo4j
# (jeweils in normaler und optimierter Variante). Es steuert die Datengenerierung, den
# Aufbau und Abbau der Datenbankcontainer, das Einfügen von Daten sowie die Benchmark- und Analysephasen.

import subprocess, sys, time, logging
from pathlib import Path
from contextlib import contextmanager

# === Import der Helper-Module für die vier Datenbankvarianten ===
from postgresql_normal.postgresql_normal import (
    build_normal_postgres_image, start_normal_postgres_container,
    apply_normal_sql_structure, stop_normal_postgres_container, delete_normal_postgres_image
)
from postgresql_optimized.postgresql_optimized import (
    build_optimized_postgres_image, start_optimized_postgres_container,
    apply_optimized_sql_structure, stop_optimized_postgres_container, delete_optimized_postgres_image
)
from neo4j_normal.neo4j_normal import (
    build_normal_neo4j_image, start_normal_neo4j_container,
    apply_normal_cypher_structure, stop_normal_neo4j_container, delete_normal_neo4j_image
)
from neo4j_optimized.neo4j_optimized import (
    build_optimized_neo4j_image, start_optimized_neo4j_container,
    apply_optimized_cypher_structure, stop_optimized_neo4j_container, delete_optimized_neo4j_image
)

# === Globale Pfade und Einstellungen ===
BASE_DIR = Path(__file__).parent


# Pfade zu den wichtigsten Skripten
GEN     = BASE_DIR / "generate_data.py"
EXPORT  = BASE_DIR / "export_sql_cypher.py"
INSERT_POSTGRESQL_NORMAL     = BASE_DIR / "postgresql_normal" / "insert_normal_postgresql_data.py"
INSERT_POSTGRESQL_OPTIMIZED  = BASE_DIR / "postgresql_optimized" / "insert_optimized_postgresql_data.py"
INSERT_NEO4J_NORMAL          = BASE_DIR / "neo4j_normal" / "insert_normal_neo4j_data.py"
INSERT_NEO4J_OPTIMIZED       = BASE_DIR / "neo4j_optimized" / "insert_optimized_neo4j_data.py"
BENCH   = BASE_DIR / "performance_benchmark.py"
ANALYSE = BASE_DIR / "analyse.py"


# Liste von Nutzerzahlen für die Simulation (z. B. 100, 1000 usw.)
USER_STEPS = [1000, 10000, 100000]


# Maximale Anzahl an Benchmark-Runden
MAX_ROUNDS = 5


# Anzahl der Wiederholungen für jeden Benchmarklauf
repetitions = 10


# Warmup-Runden zur Stabilisierung der Umgebung
warmups = 10


# === Logging-Konfiguration für eine übersichtliche Konsolenausgabe ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)


@contextmanager
def timeit(msg: str):
    # Kontextmanager zum Messen und Protokollieren der Laufzeit eines Codeblocks
    logging.info("⚙️  %s", msg)  # Beginn-Logeintrag mit Beschreibung
    t0 = time.perf_counter()    # Startzeitpunkt erfassen
    try:
        yield                   # Übergibt die Kontrolle an den umschlossenen Codeblock
    finally:
        # Nach Beendigung des Blocks: Endzeit ermitteln und Dauer ausgeben
        logging.info("✅ %s – %.1fs", msg, time.perf_counter() - t0)


def run_once(n_users: int, rounds: int) -> None:
    try:
        print(f"Starte Benchmark für {n_users} Nutzer (Runde {rounds})")
        # 🔁 Führt einen vollständigen Durchlauf mit allen vier Datenbankvarianten durch (normal & optimiert, PostgreSQL & Neo4j)

        # 1) Datengenerierung mit n_users
        with timeit(f"generate_data.py ({n_users})"):
            subprocess.run([sys.executable, "-u", str(GEN),
                            "--users", str(n_users)],
                           check=True)

        #2) Export statischer Produktdaten in SQL- und Cypher-Dateien
        with timeit("export_sql_cypher.py"):
            subprocess.run([sys.executable, "-u", str(EXPORT)], check=True)

        # ============ Normal PostgreSQL ============
        logging.info("Starte Normal PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        build_normal_postgres_image("./postgresql_normal")
        start_normal_postgres_container()
        apply_normal_sql_structure("./postgresql_normal/setup_postgres_normal.sql")

        with timeit("insert_normal_postgresql_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_POSTGRESQL_NORMAL),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "pg_normal",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)

        stop_normal_postgres_container()
        delete_normal_postgres_image()
        logging.info("Beendet Normal PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        # ============ Optimized PostgreSQL ============
        logging.info("Starte Optimized PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        build_optimized_postgres_image("./postgresql_optimized")
        start_optimized_postgres_container()
        apply_optimized_sql_structure("./postgresql_optimized/setup_postgres_optimized.sql")

        with timeit("insert_optimized_postgresql_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_POSTGRESQL_OPTIMIZED),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "pg_opt",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)


        stop_optimized_postgres_container()
        delete_optimized_postgres_image()
        logging.info("Beendet Optimized PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        # ============ Normal Neo4j ============
        logging.info("Starte Normal Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        build_normal_neo4j_image("./neo4j_normal")
        start_normal_neo4j_container()
        apply_normal_cypher_structure("./neo4j_normal/setup_neo4j_normal.cypher")

        with timeit("insert_normal_neo4j_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_NEO4J_NORMAL),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "neo_normal",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)
            

        stop_normal_neo4j_container()
        delete_normal_neo4j_image()
        logging.info("Beendet Normal Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        # ============ Optimized Neo4j ============
        logging.info("Starte Optimized Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        build_optimized_neo4j_image("./neo4j_optimized")
        start_optimized_neo4j_container()
        apply_optimized_cypher_structure("./neo4j_optimized/setup_neo4j_optimized.cypher")

        with timeit("insert_optimized_neo4j_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_NEO4J_OPTIMIZED),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "neo_opt",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)
            

        stop_optimized_neo4j_container()
        delete_optimized_neo4j_image()
        logging.info("Beendet Optimized Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        logging.info("Alle Schritte für %d Nutzer (Runde %d) erfolgreich abgeschlossen", n_users, rounds)

    finally:
        # ❗ Sicherheitsnetz – stellt sicher, dass alle Container auch bei Fehlern gestoppt und gelöscht werden
        stop_normal_postgres_container()
        delete_normal_postgres_image()
        stop_optimized_postgres_container()
        delete_optimized_postgres_image()
        stop_normal_neo4j_container()
        delete_normal_neo4j_image()
        stop_optimized_neo4j_container()
        delete_optimized_neo4j_image()


def main():
    # Hauptschleife: Führt den Benchmark mehrfach mit steigender Nutzeranzahl durch
    for rnd in range(1, MAX_ROUNDS + 1):
        for n_users in USER_STEPS:
            run_once(n_users, rnd)

    # Analyse der gesammelten Ergebnisse
    with timeit("analyse.py"):
        subprocess.run([sys.executable, "-u", str(ANALYSE)], check=True)
        logging.info("Analyse abgeschlossen. Ergebnisse in 'results/' und 'plots/' gespeichert.")


if __name__ == "__main__":
    main()
