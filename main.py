# main.py
import subprocess, sys, time, logging
from pathlib import Path
from contextlib import contextmanager
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

BASE_DIR = Path(__file__).parent
GEN     = BASE_DIR / "generate_data.py"
EXPORT  = BASE_DIR / "export_sql_cypher.py"
INSERT_POSTGRESQL_NORMAL  = BASE_DIR / "postgresql_normal" / "insert_normal_postgresql_data.py"
INSERT_POSTGRESQL_OPTIMIZED  = BASE_DIR / "postgresql_optimized" / "insert_optimized_postgresql_data.py"
INSERT_NEO4J_NORMAL  = BASE_DIR / "neo4j_normal" / "insert_normal_neo4j_data.py"
INSERT_NEO4J_OPTIMIZED  = BASE_DIR / "neo4j_optimized" / "insert_optimized_neo4j_data.py"
BENCH   = BASE_DIR / "performance_benchmark.py"

USER_STEPS = [1000]

MAX_ROUNDS = 1

repetitions = 1  # Anzahl der Wiederholungen für den Benchmark

warmups = 0  # Anzahl der Warmup-Runden vor dem eigentlichen Benchmark am besten 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)

@contextmanager
def timeit(msg: str):
    logging.info("⚙️  %s", msg)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        logging.info("✅ %s – %.1fs", msg, time.perf_counter() - t0)

def run_once(n_users: int, rounds: int) -> None:
    try:
        
        # 1) Datensatz
        with timeit(f"generate_data.py ({n_users})"):
            subprocess.run([sys.executable, "-u", str(GEN),
                            "--users", str(n_users)],
                           check=True)

        # 2) Export
        with timeit("export_sql_cypher.py"):
            subprocess.run([sys.executable, "-u", str(EXPORT)], check=True)

        #---Normal PostgreSQL---
        logging.info("Starte Normal PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        # 1) Postgres-Setup
        build_normal_postgres_image("./postgresql_normal")
        start_normal_postgres_container()
        apply_normal_sql_structure("./postgresql_normal/setup_postgres_normal.sql")

        # 2) Insert
        with timeit("insert_normal_postgresql_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_POSTGRESQL_NORMAL),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        # 3) Benchmark
        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "pg_normal",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)
        # 4) Stop und Cleanup
        stop_normal_postgres_container()
        delete_normal_postgres_image()    
        logging.info("Beendet Normal PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        #---Optimized PostgreSQL---
        logging.info("Starte Optimized PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)
        
        # 1) Postgres-Setup
        build_optimized_postgres_image("./postgresql_optimized")
        start_optimized_postgres_container()
        apply_optimized_sql_structure("./postgresql_optimized/setup_postgres_optimized.sql")

        # 2) Insert
        with timeit("insert_optimized_postgresql_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_POSTGRESQL_OPTIMIZED),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        # 3) Benchmark
        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "pg_opt",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)
        # 4) Stop und Cleanup
        stop_optimized_postgres_container()
        delete_optimized_postgres_image()  
        logging.info("Beendet Optimized PostgreSQL Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        #---Normal Neo4j---
        logging.info("Starte Normal Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        # 1) Setup
        build_normal_neo4j_image("./neo4j_normal")
        start_normal_neo4j_container()
        apply_normal_cypher_structure("./neo4j_normal/setup_neo4j_normal.cypher")

        # 2) Insert
        with timeit("insert_normal_neo4j_data.py"):
            subprocess.run([sys.executable, "-u", str(INSERT_NEO4J_NORMAL),
                            "--file-id", str(n_users),
                            "--json-dir", "./output"],
                           check=True)

        # 3) Benchmark
        with timeit("performance_benchmark.py"):
            subprocess.run([sys.executable, "-u", str(BENCH),
                            "--variant", "neo_normal",
                            "--users", str(n_users),
                            "--round", str(rounds),
                            "--repetitions", str(repetitions),
                            "--warmups", str(warmups)],
                           check=True)
        # 4) Stop und Cleanup
        stop_normal_neo4j_container()
        delete_normal_neo4j_image()   
        logging.info("Beendet Normal Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        #---Optimized Neo4j---
        #logging.info("Starte Optimized Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        # 1) Setup
        #build_optimized_neo4j_image()
        #start_optimized_neo4j_container()
        #apply_optimized_cypher_structure()

        # 2) Insert
        #with timeit("insert_optimized_neo4j_data.py"):
        #    subprocess.run([sys.executable, "-u", str(INSERT_NEO4J_OPTIMIZED),
        #                    "--file-id", str(n_users),
        #                    "--json-dir", "./output"],
        #                   check=True)

        # 3) Benchmark
        #with timeit("performance_benchmark.py"):
        #    subprocess.run([sys.executable, "-u", str(BENCH),
        #                    "--variant", "neo_opt",
        #                    "--users", str(n_users),
        #                    "--round", str(rounds),
        #                    "--repetitions", str(repetitions),
        #                    "--warmups", str(warmups)],
        #                   check=True)
        # 4) Stop und Cleanup
        #stop_optimized_neo4j_container()
        #delete_optimized_neo4j_image()   
        #logging.info("Beendet Optimized Neo4j Benchmark für %d Nutzer (Runde %d)", n_users, rounds)

        logging.info("Alle Schritte für %d Nutzer (Runde %d) erfolgreich abgeschlossen", n_users, rounds)
    
    finally:
        # Sicherstellen, dass alle Container gestoppt werden
        stop_normal_postgres_container()
        delete_normal_postgres_image()
        stop_optimized_postgres_container()
        delete_optimized_postgres_image()
        stop_normal_neo4j_container()
        delete_normal_neo4j_image()
        stop_optimized_neo4j_container()
        delete_optimized_neo4j_image()


def main():
    for rnd in range(1, MAX_ROUNDS + 1):
        for n_users in USER_STEPS:
            run_once(n_users, rnd)

if __name__ == "__main__":
    main()
