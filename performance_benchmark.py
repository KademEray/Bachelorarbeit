"""performance_benchmark.py
============================================================
Vergleicht vier Docker‑Datenbanken (PostgreSQL & Neo4j) anhand
12 Queries x 4 Laststufen (1 / 3 / 5 / 10 parallele Aufrufe).

Erfasst pro Durchlauf:
* Dauer (ms) - Gesamtzeit bis **alle** parallelen Aufrufe fertig sind
* CPU-Last des Containers (%),
* RAM-Belegung (MB)
* belegter Plattenspeicher (MB, SizeRootFs),
* Komplexitätsstufe der Query.

Speichert alles in eine CSV-Datei <variant>_results.csv.
"""

from __future__ import annotations
import csv
import time
import subprocess
from pathlib import Path
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from psycopg2.pool import ThreadedConnectionPool
from neo4j import GraphDatabase
from tqdm import tqdm
import math
import uuid, random, string
import argparse
import json
import os
import logging
import multiprocessing, pathlib


CG_PATH = pathlib.Path("/sys/fs/cgroup")        # cgroup v2 angenommen
CPU_CORES = multiprocessing.cpu_count()

# ───── Logging-Setup ─────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/benchmark.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


###############################################################################
# Hilfsfunktionen: Docker‑Statistik -----------------------------------------
###############################################################################
_CLK_TCK_CACHE: dict[str, int] = {}

def _clk_tck_for_container(cid: str) -> int:
    """Liefert den CLK_TCK-Wert (Jiffies pro Sekunde) des Containers."""
    if cid in _CLK_TCK_CACHE:
        return _CLK_TCK_CACHE[cid]

    try:
        out = subprocess.check_output(
            ["docker", "exec", cid, "getconf", "CLK_TCK"],
            text=True, stderr=subprocess.DEVNULL
        ).strip()
        val = int(out)
    except Exception:
        val = 100          # praktisch alle x86-64-Kerne
    _CLK_TCK_CACHE[cid] = val
    return val

def _read_iowait_jiffies(cid: str) -> int:
    """
    Liest iowait-Jiffies aus /proc/stat des Containers (nicht des Hosts).
    Das funktioniert auch unter Docker Desktop auf Windows/Mac.
    """
    try:
        txt = subprocess.check_output(
            ["docker", "exec", cid, "cat", "/proc/stat"],
            text=True, stderr=subprocess.DEVNULL
        )
        for line in txt.splitlines():
            if line.startswith("cpu "):
                fields = line.split()
                return int(fields[5])  # Spalte 6 = iowait
    except Exception as e:
        print(f"[warn] iowait konnte nicht gelesen werden: {e}")
        return 0

def _read_cgroup_stats(cid: str) -> dict[str, int]:
    """
    Liefert kumulative CPU-, Block-I/O-, Netz-I/O- und Speicher-Statistiken
    eines Containers.  Fällt bei Docker-Desktop automatisch auf `docker exec`
    zurück.
    """
    cdir = CG_PATH / cid
    use_exec = not cdir.exists()

    # ---------- Dateien lesen ----------
    def _cat_inside(path: str) -> str:
        return subprocess.check_output(
            ["docker", "exec", cid, "cat", path],
            text=True, stderr=subprocess.DEVNULL
        )

    def _read_file(rel_path: str) -> str:
        if use_exec:
            return _cat_inside(f"/sys/fs/cgroup/{rel_path}")
        return (cdir / rel_path).read_text()

    # ---------- CPU ----------
    cpu_usec = 0
    for ln in _read_file("cpu.stat").splitlines():
        if ln.startswith("usage_usec"):
            cpu_usec = int(ln.split()[1])
            break

    # ---------- Block-I/O ----------
    io_txt = _read_file("io.stat")

    def _sum_io(kind: str) -> int:
        total = 0
        for ln in io_txt.splitlines():
            for kv in ln.split()[1:]:
                k, v = kv.split("=", 1)
                if k == kind or k == f"{kind}_recursive":
                    total += int(v)
        return total

    rbytes = _sum_io("rbytes")
    wbytes = _sum_io("wbytes")

    # ---------- Network-I/O ----------
    # Wir lesen innerhalb des Containers das *erste* "eth*"-Interface.
    # (Bei Docker ist das üblicherweise eth0.)
    net_rx = net_tx = 0
    try:
        iface = "eth0"
        base = f"/sys/class/net/{iface}/statistics"
        if use_exec:
            net_rx = int(_cat_inside(f"{base}/rx_bytes").strip())
            net_tx = int(_cat_inside(f"{base}/tx_bytes").strip())
        else:
            net_rx = int(Path(base, "rx_bytes").read_text().strip())
            net_tx = int(Path(base, "tx_bytes").read_text().strip())
    except Exception:
        # Fallback – z. B. wenn Interface anders heißt
        pass

    # ---------- Memory ----------
    mem_now  = int(_read_file("memory.current").strip())

    return {
        "cpu_usec":  cpu_usec,
        "io_rbytes": rbytes,
        "io_wbytes": wbytes,
        "net_rbytes": net_rx,
        "net_tbytes": net_tx,
        "mem_now":   mem_now,
    }


def _delta(before: dict, after: dict) -> dict:
    return {k: after[k] - before[k] for k in before}


def get_docker_disk_mb(container: str) -> float:
    """Gibt die Größe des beschreibbaren Layers (SizeRootFs) in MB zurück."""
    try:
        out = subprocess.run(
            ["docker", "container", "inspect", "--size", "--format", "{{.SizeRootFs}}", container],
            capture_output=True, text=True, check=True
        ).stdout.strip()

        bytes_val = int(out)
        mb_val = bytes_val / (1024 * 1024)
        logger.debug(f"Disk usage für Container '{container}': {mb_val:.2f} MB")
        return mb_val

    except ValueError:
        logger.warning(f"Kann Disk-Wert nicht in Integer umwandeln: '{out}'")
        return math.nan

    except subprocess.CalledProcessError as e:
        logger.error(f"Fehler beim Abrufen der Docker-Disk-Größe für '{container}': {e}")
        return math.nan


###############################################################################
# Query‑Definitionen ---------------------------------------------------------
###############################################################################

class Complexity(Enum):
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"

def make_new_ids() -> dict[str, str | int]:
    new_ids = {
        "new_big_id": random.randint(10_000_000, 99_999_999),
        "new_cat_name": f"NewCat_{uuid.uuid4().hex[:6]}",
        "new_prod_name": f"NewProd_{uuid.uuid4().hex[:6]}",
        "new_track_no": "TRK" + ''.join(random.choices(
            string.ascii_uppercase + string.digits, k=6)),
    }
    logger.debug(f"Generierte neue IDs: {new_ids}")
    return new_ids



# ==========================================================
# PostgreSQL-Benchmark-Queries (Normal & Optimised)
# ==========================================================
PG_NORMAL_QUERIES: Dict[Complexity, List[str]] = {

    # ───────────── SIMPLE ─────────────
    Complexity.SIMPLE: [
        "SELECT * FROM products LIMIT 10000;",
        "SELECT id, name FROM categories LIMIT 1000;",
        "SELECT * FROM addresses LIMIT 1;"
    ],

    # ───────────── MEDIUM ─────────────
    Complexity.MEDIUM: [
        # alle Produkte, die überhaupt Kategorien besitzen
        """SELECT p.* FROM products p
           WHERE EXISTS (SELECT 1 FROM product_categories pc
                         WHERE pc.product_id = p.id)
           LIMIT 100;""",

        # letzte Bestellung (irgendeines Users) + Positionen
        """SELECT p.*, oi.quantity
           FROM orders o
           JOIN order_items oi ON oi.order_id = o.id
           JOIN products    p  ON p.id = oi.product_id
           ORDER BY o.created_at DESC
           LIMIT 1;""",

        # fünf neueste Reviews (egal zu welchem Produkt)
        """SELECT * FROM reviews
           ORDER BY created_at DESC
           LIMIT 5;"""
    ],

    # ───────────── COMPLEX ─────────────
    Complexity.COMPLEX: [
        # Bestell-Summen pro Order
        """SELECT o.id, o.created_at,
                 SUM(oi.quantity * oi.price) AS total
           FROM orders o
           JOIN order_items oi ON oi.order_id = o.id
           GROUP BY o.id
           LIMIT 50;""",

        # Produkte mit Ø-Rating > 4
        """SELECT p.id, p.name, AVG(r.rating) AS avg_rating
           FROM products p
           JOIN reviews r ON r.product_id = p.id
           GROUP BY p.id, p.name
           HAVING AVG(r.rating) > 4;""",

        # Bestellungen letztes Monat pro User (> 0)
        """SELECT u.id, COUNT(*) AS orders_last_30d
           FROM users u
           JOIN orders o ON o.user_id = u.id
           WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
           GROUP BY u.id
           HAVING COUNT(*) > 0
           LIMIT 50;"""
    ],

    # ───────────── VERY COMPLEX ─────────────
    Complexity.VERY_COMPLEX: [
        # Cross-Sell: Top-gekauftes Produkt ermitteln,
        # Kunden dieses Produkts kaufen etwas anderes …
        """
        WITH top_prod AS (
            SELECT product_id
            FROM order_items
            GROUP BY product_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ),
        buyers AS (
            SELECT DISTINCT o.user_id
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE oi.product_id = (SELECT product_id FROM top_prod)
        )
        SELECT oi2.product_id AS rec_id, COUNT(*) AS freq
        FROM orders o
        JOIN order_items oi2 ON oi2.order_id = o.id
        WHERE o.user_id IN (SELECT user_id FROM buyers)
          AND oi2.product_id <> (SELECT product_id FROM top_prod)
        GROUP BY oi2.product_id
        ORDER BY freq DESC
        LIMIT 10;""",

        # View + Purchase beliebiger User
        """
        SELECT DISTINCT p.id, p.name
        FROM product_views  v
        JOIN orders         o  ON o.user_id = v.user_id
        JOIN order_items    oi ON oi.order_id = o.id
        JOIN products       p  ON p.id = v.product_id
                               AND p.id = oi.product_id
        LIMIT 25;""",

        # Zwei-Hop Netz um dasselbe top-Produkt
        """
        WITH top_prod AS (
            SELECT product_id
            FROM order_items
            GROUP BY product_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ),
        buyers AS (
            SELECT DISTINCT user_id
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE oi.product_id = (SELECT product_id FROM top_prod)
        )
        SELECT oi2.product_id, COUNT(*) AS freq
        FROM orders o
        JOIN order_items oi2 ON oi2.order_id = o.id
        WHERE o.user_id IN (SELECT user_id FROM buyers)
          AND oi2.product_id <> (SELECT product_id FROM top_prod)
        GROUP BY oi2.product_id
        ORDER BY freq DESC
        LIMIT 20;"""
    ],

    # ───────────── CREATE ─────────────
    Complexity.CREATE: [
        # neue Adresse für irgendeinen User
        """
        INSERT INTO addresses (user_id, street, city, zip, country, is_primary)
        VALUES (
            (SELECT id FROM users LIMIT 1),
            'Foo-' || gen_random_uuid()::text,
            'Bar City', '12345', 'DE', FALSE
        );""",

        # neue Bestellung
        """
        INSERT INTO orders (user_id, status, total, created_at)
        VALUES (
            (SELECT id FROM users LIMIT 1),
            'pending', 0.0, CURRENT_TIMESTAMP
        );""",

        # Cart-Item
        """
        INSERT INTO cart_items (user_id, product_id, quantity, added_at)
        VALUES (
            (SELECT id FROM users LIMIT 1),
            (SELECT id FROM products LIMIT 1),
            2, CURRENT_TIMESTAMP
        );""",

        # Produkt-View
        """
        INSERT INTO product_views (user_id, product_id, viewed_at)
        VALUES (
            (SELECT id FROM users LIMIT 1),
            (SELECT id FROM products LIMIT 1),
            CURRENT_TIMESTAMP
        );"""
    ],

    # ───────────── UPDATE ─────────────
    Complexity.UPDATE: [
        "UPDATE products  SET stock = stock + 1 WHERE id = (SELECT id FROM products LIMIT 1);",
        "UPDATE reviews   SET rating = GREATEST(rating - 1, 1) WHERE id = (SELECT id FROM reviews LIMIT 1);",
        "UPDATE cart_items SET quantity = quantity + 3 WHERE id = (SELECT id FROM cart_items LIMIT 1);",
        "UPDATE users     SET email = email || '.tmp' WHERE id = (SELECT id FROM users LIMIT 1);"
    ],

    # ───────────── DELETE ─────────────
    Complexity.DELETE: [
        "DELETE FROM addresses WHERE id = (SELECT id FROM addresses LIMIT 1);",
        "DELETE FROM reviews   WHERE id = (SELECT id FROM reviews   LIMIT 1);",
        "DELETE FROM cart_items WHERE id = (SELECT id FROM cart_items LIMIT 1);",
        "DELETE FROM product_purchases WHERE id = (SELECT id FROM product_purchases LIMIT 1);"
    ]
}


PG_OPT_QUERIES = PG_NORMAL_QUERIES   # gleiche SQL-Syntax


# ==========================================================
#  Neo4j  (NORMAL & OPTIMISED)
# ==========================================================
# - Die inhaltliche Reihenfolge/Anzahl ist identisch zu PG.
# - Optimised-Variante nimmt die kürzeren Relationen (CONTAINS, REVIEWED …)
#   – ansonsten exakt dieselbe Logik & Zählweise.
NEO_NORMAL_QUERIES: Dict[Complexity, List[str]] = {

    # ───────────── SIMPLE ─────────────
    Complexity.SIMPLE: [
        "MATCH (p:Product) RETURN p LIMIT 10000;",
        "MATCH (c:Category) RETURN c LIMIT 1000;",
        "MATCH (u:User)-[:HAS_ADDRESS]->(a) RETURN a LIMIT 1;"
    ],

    # ───────────── MEDIUM ─────────────
    Complexity.MEDIUM: [
        # Produkte, die mindestens einer Kategorie zugeordnet sind
        "MATCH (p:Product)-[:BELONGS_TO]->(:Category) RETURN p LIMIT 100;",

        # Letzte Bestellung + Positionen
        """
        MATCH (o:Order)<-[:PLACED]-(:User)
        WITH o ORDER BY o.created_at DESC LIMIT 1
        MATCH (o)-[:HAS_ITEM]->(oi:OrderItem)-[:REFERS_TO]->(p:Product)
        RETURN p, oi.quantity;
        """,

        # Neueste Reviews
        """
        MATCH (r:Review)-[:REVIEWS]->(p:Product)
        RETURN r ORDER BY r.created_at DESC LIMIT 5;
        """
    ],

    # ───────────── COMPLEX ─────────────
    Complexity.COMPLEX: [
        # Summen pro Order
        """
        MATCH (o:Order)-[:HAS_ITEM]->(oi:OrderItem)
        WITH o, SUM(toInteger(oi.quantity)*toFloat(oi.price)) AS total
        RETURN o.id AS orderId, o.created_at AS date, total LIMIT 50;
        """,

        # Produkte mit Ø-Rating > 4
        """
        MATCH (p:Product)<-[:REVIEWS]-(r:Review)
        WITH p, AVG(toFloat(r.rating)) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id, p.name, avg_rating;
        """,

        # Bestellungen der letzten 30 Tage pro User
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE o.created_at >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS cnt
        RETURN u.id, cnt LIMIT 50;
        """
    ],

    # ───────────── VERY COMPLEX ─────────────
    Complexity.VERY_COMPLEX: [
        # Cross-Sell auf Basis des meist­verkauften Produkts
        """
        // Top-Produkt bestimmen
        MATCH (:Order)-[:HAS_ITEM]->(oi1:OrderItem)
        WITH oi1.product_id AS topProd, COUNT(*) AS freq
        ORDER BY freq DESC LIMIT 1
        // Käufer dieses Produkts
        MATCH (u:User)-[:PLACED]->(o:Order)-[:HAS_ITEM]->(:OrderItem {product_id:topProd})
        WITH DISTINCT u, topProd
        // andere gekaufte Produkte
        MATCH (u)-[:PLACED]->(o2:Order)-[:HAS_ITEM]->(oi2:OrderItem)
        WHERE oi2.product_id <> topProd
        RETURN oi2.product_id AS rec_id, COUNT(*) AS freq
        ORDER BY freq DESC LIMIT 10;
        """,

        # View + Purchase-Schnittmenge
        """
        MATCH (u:User)-[:VIEWED]->(:ProductView)-[:VIEWED_PRODUCT]->(p:Product)
        MATCH (u)-[:PLACED]->(:Order)-[:HAS_ITEM]->(:OrderItem)-[:REFERS_TO]->(p)
        RETURN DISTINCT p.id, p.name LIMIT 25;
        """,

        # Zwei-Hop-Netz um oben­stehendes Top-Produkt
        """
        MATCH (:Order)-[:HAS_ITEM]->(oi1:OrderItem)
        WITH oi1.product_id AS topProd, COUNT(*) AS freq
        ORDER BY freq DESC LIMIT 1
        MATCH (u:User)-[:PLACED]->(:Order)-[:HAS_ITEM]->(:OrderItem {product_id:topProd})
        WITH DISTINCT u, topProd
        MATCH (u)-[:PLACED]->(:Order)-[:HAS_ITEM]->(oi2:OrderItem)
        WHERE oi2.product_id <> topProd
        RETURN oi2.product_id AS prod_id, COUNT(*) AS freq
        ORDER BY freq DESC LIMIT 20;
        """
    ],

    # ───────────── CREATE ─────────────
    Complexity.CREATE: [
        # Adresse
        """
        MATCH (u:User) WITH u LIMIT 1
        CREATE (u)-[:HAS_ADDRESS]->(:Address {
            id: randomUUID(), street:'Foo', city:'Bar City',
            zip:'12345', country:'DE', is_primary:false});
        """,

        # Order
        """
        MATCH (u:User) WITH u LIMIT 1
        CREATE (u)-[:PLACED]->(:Order {
            status:'pending', total:0.0, created_at:datetime()});
        """,

        # Cart-Item
        """
        MATCH (u:User) WITH u LIMIT 1
        MATCH (p:Product) WITH u,p LIMIT 1
        CREATE (u)-[:HAS_IN_CART {quantity:2, added_at:datetime()}]->(p);
        """,

        # Produkt-View
        """
        MATCH (u:User) WITH u LIMIT 1
        MATCH (p:Product) WITH u,p LIMIT 1
        CREATE (u)-[:VIEWED]->(:ProductView {id:randomUUID(), viewed_at:datetime()})
               -[:VIEWED_PRODUCT]->(p);
        """
    ],

    # ───────────── UPDATE ─────────────
    Complexity.UPDATE: [
        "MATCH (p:Product) WITH p LIMIT 1 SET p.stock = COALESCE(p.stock,0) + 1;",
        "MATCH (r:Review)  WITH r LIMIT 1 SET r.rating = CASE WHEN r.rating > 1 THEN r.rating-1 ELSE r.rating END;",
        "MATCH ()-[c:HAS_IN_CART]->() WITH c LIMIT 1 SET c.quantity = c.quantity + 3;",
        "MATCH (u:User)    WITH u LIMIT 1 SET u.email = u.email + '.tmp';"
    ],

    # ───────────── DELETE ─────────────
    Complexity.DELETE: [
        "MATCH (a:Address) WITH a LIMIT 1 DETACH DELETE a;",
        "MATCH ()-[r:REVIEWS]-() WITH r LIMIT 1 DELETE r;",
        "MATCH ()-[rel:HAS_IN_CART]-() WITH rel LIMIT 1 DELETE rel;",
        "MATCH ()-[c:HAS_IN_CART]->() WITH c LIMIT 1 SET c.quantity = toInteger(c.quantity) + 3;"
    ]
}


# --- Neo4j optimiert --------------------------------------------------------
NEO_OPT_QUERIES: Dict[Complexity, List[str]] = {

    Complexity.SIMPLE: [
        "MATCH (p:Product) RETURN p LIMIT 10000;",
        "MATCH (c:Category) RETURN c LIMIT 1000;",
        "MATCH (u:User)-[:HAS_ADDRESS]->(a) RETURN a LIMIT 1;"
    ],

    Complexity.MEDIUM: [
        "MATCH (p:Product)-[:BELONGS_TO]->(:Category) RETURN p LIMIT 100;",

        """
        MATCH (o:Order)<-[:PLACED]-(:User)
        WITH o ORDER BY o.created_at DESC LIMIT 1
        MATCH (o)-[:CONTAINS]->(p:Product)
        RETURN p, o;""",

        "MATCH ()-[rev:REVIEWED]->(p:Product) RETURN rev ORDER BY rev.created_at DESC LIMIT 5;"
    ],

    Complexity.COMPLEX: [
        """
        MATCH (o:Order)-[:CONTAINS]->(p:Product)<-[rev:REVIEWED]-()
        WITH o, SUM(rev.quantity * rev.price) AS total
        RETURN o.id, o.created_at, total LIMIT 50;
        """,

        """
        MATCH (p:Product)<-[rev:REVIEWED]-()
        WITH p, AVG(rev.rating) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id, p.name, avg_rating;
        """,

        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE o.created_at >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS cnt
        RETURN u.id, cnt LIMIT 50;
        """
    ],

    Complexity.VERY_COMPLEX: [
        # Cross-Sell (CONTAINS)
        """
        MATCH (:Order)-[:CONTAINS]->(p1:Product)
        WITH p1, COUNT(*) AS freq ORDER BY freq DESC LIMIT 1
        MATCH (u:User)-[:PLACED]->(:Order)-[:CONTAINS]->(p1)
        WITH u, p1
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p2:Product)
        WHERE p2 <> p1
        RETURN p2.id, p2.name, COUNT(*) AS freq
        ORDER BY freq DESC LIMIT 10;
        """,

        # View + Purchase
        """
        MATCH (u:User)-[:VIEWED]->(:ProductView)-[:VIEWED_PRODUCT]->(p:Product)
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p)
        RETURN DISTINCT p.id, p.name LIMIT 25;
        """,

        # Zwei-Hop
        """
        MATCH (:Order)-[:CONTAINS]->(p1:Product)
        WITH p1, COUNT(*) AS freq ORDER BY freq DESC LIMIT 1
        MATCH (u:User)-[:PLACED]->(:Order)-[:CONTAINS]->(p1)
        WITH u, p1
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p2:Product)
        WHERE p2 <> p1
        RETURN p2.id, p2.name, COUNT(*) AS freq
        ORDER BY freq DESC LIMIT 20;
        """
    ],

    # ───────────── CREATE ─────────────
    Complexity.CREATE: [
        # Adresse
        """
        MATCH (u:User) WITH u LIMIT 1
        CREATE (u)-[:HAS_ADDRESS]->(:Address {
            id: randomUUID(), street:'Foo', city:'Bar City',
            zip:'12345', country:'DE', is_primary:false});
        """,

        # Order
        """
        MATCH (u:User) WITH u LIMIT 1
        CREATE (u)-[:PLACED]->(:Order {
            status:'pending', total:0.0, created_at:datetime()});
        """,

        # Cart-Item
        """
        MATCH (u:User) WITH u LIMIT 1
        MATCH (p:Product) WITH u,p LIMIT 1
        CREATE (u)-[:HAS_IN_CART {quantity:2, added_at:datetime()}]->(p);
        """,

        # Produkt-View
        """
        MATCH (u:User) WITH u LIMIT 1
        MATCH (p:Product) WITH u,p LIMIT 1
        CREATE (u)-[:VIEWED]->(:ProductView {id:randomUUID(), viewed_at:datetime()})
               -[:VIEWED_PRODUCT]->(p);
        """
    ],

    # ───────────── UPDATE ─────────────
    Complexity.UPDATE: [
        "MATCH (p:Product) WITH p LIMIT 1 SET p.stock = COALESCE(p.stock,0) + 1;",
        "MATCH (r:Review)  WITH r LIMIT 1 SET r.rating = CASE WHEN r.rating > 1 THEN r.rating-1 ELSE r.rating END;",
        "MATCH ()-[c:HAS_IN_CART]->() WITH c LIMIT 1 SET c.quantity = c.quantity + 3;",
        "MATCH (u:User)    WITH u LIMIT 1 SET u.email = u.email + '.tmp';"
    ],

    # ───────────── DELETE ─────────────
    Complexity.DELETE: [
        "MATCH (a:Address) WITH a LIMIT 1 DETACH DELETE a;",
        "MATCH ()-[r:REVIEWED]-() WITH r LIMIT 1 DELETE r;",
        "MATCH ()-[c:HAS_IN_CART]-() WITH c LIMIT 1 DELETE c;",
        "MATCH ()-[p:PURCHASED]-() WITH p LIMIT 1 DELETE p;"
    ]
}


###############################################################################
# Benchmark‑Runner -----------------------------------------------------------
###############################################################################

CONCURRENCY_LEVELS = [1, 3, 5, 10]

WARMUP_SLEEP = 0.05 

CSV_HEADER = [
    "db", "mode", "phase", "concurrency", "query_no", "repeat", "complexity",
    "duration_ms","per_query_ms","qps", "avg_cpu", "iowait_pct", "avg_mem",
    "disk_mb", "total_read_mb", "total_write_mb", "net_recv_mb", "net_send_mb",
    "statement", "result"
]


def _warmup_parallel(func, query: str, concurrency: int):
    """
    Führt die Query WARMUP_RUNS-mal parallel aus - ohne Mess- und Logik-Overhead.
    """
    if WARMUP_RUNS <= 0:
        logger.debug("Überspringe Warm-up, da WARMUP_RUNS <= 0.")
        return

    total_runs = concurrency * WARMUP_RUNS
    logger.debug(f"Starte Warm-up: {total_runs} Durchläufe mit concurrency={concurrency}")
    logger.debug(f"Warm-up Query: {query.replace(chr(10), ' ')}")

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(func, query) for _ in range(total_runs)]
        for ft in as_completed(futs):
            _ = ft.result()  # Fehler werden hier geworfen und nicht unterdrückt

    logger.debug(f"Warm-up abgeschlossen. Warte {WARMUP_SLEEP} Sekunden...")
    time.sleep(WARMUP_SLEEP)

_CID_CACHE: dict[str, str] = {}

def _cid_of(name: str) -> str:
    """Liefert die volle Container-ID für einen Docker-Namen (mit Cache)."""
    if name in _CID_CACHE:
        return _CID_CACHE[name]
    cid = subprocess.check_output(
        ["docker", "inspect", "--format", "{{.Id}}", name],
        text=True, stderr=subprocess.DEVNULL).strip()
    _CID_CACHE[name] = cid
    return cid

def _run_and_time(func, *a, **kw) -> float:
    """
    Führt func mit Argumenten aus und misst die Laufzeit in Millisekunden.
    """
    logger.debug(f"Starte Zeitmessung für Funktion {func.__name__} mit args={a}, kwargs={kw}")
    t0 = time.perf_counter_ns()
    func(*a, **kw)
    duration_ms = (time.perf_counter_ns() - t0) / 1_000_000
    logger.debug(f"Laufzeit für {func.__name__}: {duration_ms:.2f} ms")
    return duration_ms


def _log_csv(writer, *, phase, db, mode, conc, idx, repeat,
             comp, dur, per_q_ms, qps, avg_cpu, iowait_pct, avg_mem,
             disk_mb, total_read_mb, total_write_mb, net_recv_mb, net_send_mb, stmt, res):
    row = [
        db, mode, phase, conc, idx, repeat, comp.value,
        f"{dur:.2f}", f"{per_q_ms:.2f}", f"{qps:.2f}", 
        f"{avg_cpu:.2f}", f"{iowait_pct:.2f}", f"{avg_mem:.2f}",
        f"{disk_mb:.2f}", f"{total_read_mb:.2f}", f"{total_write_mb:.2f}",
        f"{net_recv_mb:.2f}", f"{net_send_mb:.2f}",
        stmt.replace("\n", " "), json.dumps(res, ensure_ascii=False, default=str)
    ]
    writer.writerow(row)
    logger.info(
        f"[{db.upper()}] {phase} | Mode: {mode} | Query #{idx} | "
        f"Conc: {conc} | Time: {dur:.2f}ms | per Query: {per_q_ms:.2f}ms | qps: {qps:.2f} | "
        f"AVG CPU: {avg_cpu:.2f}% | iowait: {iowait_pct:.2f}% | "
        f"AVG Mem: {avg_mem:.2f}MB |"
        f"Disk: {disk_mb:.2f}MB | Read Δ: {total_read_mb:.2f}MB | Write Δ: {total_write_mb:.2f}MB |"
        f"Net RX: {net_recv_mb:.2f}MB | Net TX: {net_send_mb:.2f}MB | "
    )


def _serialize_pg(cur, rows):
    if cur.description:  # SELECT
        result = {
            "rows": len(rows),
            "first": rows[0] if rows else None
        }
        logger.debug(f"[PG_SERIALIZE] SELECT-Ergebnis: {result}")
        return result
    result = {"rowcount": cur.rowcount}
    logger.debug(f"[PG_SERIALIZE] Änderungsergebnis: {result}")
    return result


def _serialize_neo(records, summary):
    if records:  # MATCH … RETURN
        result = {
            "rows": len(records),
            "first": records[0].data()
        }
        logger.debug(f"[NEO_SERIALIZE] MATCH-Ergebnis: {result}")
        return result
    cnt = summary.counters
    result = {
        "nodes_created": cnt.nodes_created,
        "nodes_deleted": cnt.nodes_deleted,
        "properties_set": getattr(cnt, "properties_set", 0)
    }
    logger.debug(f"[NEO_SERIALIZE] Veränderungsstatistik: {result}")
    return result


###############################################################################
# PostgreSQL helpers ---------------------------------------------------------
###############################################################################

PG_CONN_KWARGS = dict(host="localhost", port=5432,
                      user="postgres", password="pass", dbname="testdb")

# >>> globaler Pool; wird in _pg_benchmark() initialisiert
PG_POOL: ThreadedConnectionPool | None = None

def _run_pg_query(query: str):
    """
    Führt genau **eine** SQL-Anweisung aus.
    Die Verbindung stammt aus dem globalen Thread-Pool und hat Autocommit aktiv.
    """
    conn = PG_POOL.getconn()           # ➊ Connection leihen
    try:
        conn.autocommit = True         # spart explizites COMMIT
        with conn.cursor() as cur:
            logger.debug(f"[PG_QUERY] Start: {query.strip()}")
            cur.execute(query)
            rows = cur.fetchall() if cur.description else []
            result = _serialize_pg(cur, rows)
            logger.debug(f"[PG_QUERY] Ergebnis: {result}")
            return result
    except Exception as e:
        logger.error(f"[PG_QUERY] Fehler bei Query: {query.strip()} | Fehler: {e}")
        raise
    finally:
        PG_POOL.putconn(conn)          # ➋ Connection zurückgeben
        logger.debug("[PG_QUERY] Verbindung zurückgegeben an Pool")


def _pg_benchmark(queries: Dict[Complexity, List[str]],
                  container: str, mode: str, output: Path) -> None:
    logger.info("[PG_BENCHMARK] starte, container=%s", container)
    cid = _cid_of(container)                 # ➊ einmalig Container-ID
    clk_tck = _clk_tck_for_container(cid)
    # Connection-Pool initialisieren
    global PG_POOL
    PG_POOL = ThreadedConnectionPool(
        minconn=1, maxconn=max(CONCURRENCY_LEVELS), **PG_CONN_KWARGS)

    with open(output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(CSV_HEADER)

        q_iter = [(c, q) for c, lst in queries.items() for q in lst]
        for conc in CONCURRENCY_LEVELS:
            pbar = tqdm(q_iter, desc=f"PostgreSQL {mode} x{conc}")
            for idx, (comp, query) in enumerate(pbar, 1):

                # ---------- WARM-UP ----------
                if WARMUP_RUNS > 0:
                    logger.debug(f"[PG_BENCHMARK] Starte Warm-up für Query #{idx}")
                    warm_ms = _run_and_time(
                        _warmup_parallel,        # <- neue Signatur
                        _run_pg_query,           # Query-Runner
                        query,                   # SQL-String
                        conc                     # concurrency
                    )
                    _log_csv(w, phase="warmup", db="postgres", mode=mode,
                            conc=conc, idx=idx, repeat=0, comp=comp,
                            dur=warm_ms, avg_cpu=math.nan, iowait_pct=math.nan,
                            avg_mem=math.nan, per_q_ms=math.nan, qps=math.nan,
                            disk_mb=get_docker_disk_mb(container),
                            total_read_mb=0, total_write_mb=0,
                            net_recv_mb=0, net_send_mb=0,
                            stmt=query, res={"note": "warmup"})

                # ---------- STEADY-RUNS ----------
                for rep in range(1, REPETITIONS+1):
                    io0 = _read_iowait_jiffies(cid)
                    start_stats = _read_cgroup_stats(cid)      # ➋ Start-Snapshot
                    t0 = time.perf_counter_ns()
                    with ThreadPoolExecutor(max_workers=conc) as ex:
                        futs = [ex.submit(_run_pg_query, query)
                                for _ in range(conc)]
                        first_result = futs[0].result()
                    duration_ms = (time.perf_counter_ns()-t0)/1_000_000
                    io1 = _read_iowait_jiffies(cid)
                    end_stats = _read_cgroup_stats(cid)        # ➌ End-Snapshot
                    d = _delta(start_stats, end_stats) 
                    per_query_ms = duration_ms / conc               # Schnitt pro Request
                    qps          = conc * 1000 / duration_ms        # Concurrency / ms → / s

                    # ─── Kennzahlen berechnen ─────────────────────────────
                    elapsed_sec = duration_ms / 1000          # Messintervall
                    cpu_sec     = d["cpu_usec"] / 1_000_000    # Δ CPU-Zeit
                    delta_iowait_sec = (io1 - io0) / clk_tck
                    iowait_pct       = (delta_iowait_sec / (elapsed_sec * CPU_CORES)) * 100
                    avg_cpu = (cpu_sec / (elapsed_sec * CPU_CORES)) * 100     # kann >100% sein
                    avg_mem  = (start_stats["mem_now"] +
                                end_stats["mem_now"]) / 2 / 1024**2
                    total_read_mb  = d["io_rbytes"] / 1_048_576
                    total_write_mb = d["io_wbytes"] / 1_048_576
                    net_recv_mb     = d["net_rbytes"] / 1_048_576
                    net_send_mb     = d["net_tbytes"] / 1_048_576
                    disk_mb = get_docker_disk_mb(container)

                    _log_csv(w, phase="steady", db="postgres", mode=mode,
                             conc=conc, idx=idx, repeat=rep, comp=comp,
                             dur=duration_ms, per_q_ms=per_query_ms, qps=qps,
                             avg_cpu=avg_cpu, iowait_pct=iowait_pct, avg_mem=avg_mem, disk_mb=disk_mb,
                             total_read_mb=total_read_mb,
                             total_write_mb=total_write_mb,
                             net_recv_mb=net_recv_mb, net_send_mb=net_send_mb,
                             stmt=query, res=first_result)
        logger.info(f"[PG_BENCHMARK] Benchmark abgeschlossen: {output.name}")

    if PG_POOL:
        PG_POOL.closeall()


###############################################################################
# Neo4j helpers --------------------------------------------------------------
###############################################################################

NEO_BOLT_URI = "bolt://localhost:7687"

def _run_neo_query(query: str, driver) -> dict:
    logger.debug(f"[NEO_QUERY] Starte Query")
    try:
        with driver.session() as sess:
            res = sess.run(query)
            records = list(res)  # Records erst lesen
            result = _serialize_neo(records, res.consume())
            logger.debug(f"[NEO_QUERY] Query erfolgreich abgeschlossen. Ergebnis: {result}")
            return result
    except Exception as e:
        logger.exception(f"[NEO_QUERY] Fehler beim Ausführen der Query: {query}")
        raise


def _neo_benchmark(queries: Dict[Complexity, List[str]],
                   container: str, mode: str, output: Path) -> None:
    logger.info("[NEO_BENCHMARK] starte, container=%s", container)
    cid = _cid_of(container)
    clk_tck = _clk_tck_for_container(cid)
    driver = GraphDatabase.driver(
        NEO_BOLT_URI, auth=("neo4j", "superpassword55"))

    with driver, open(output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(CSV_HEADER)

        q_iter = [(c, q) for c, lst in queries.items() for q in lst]
        for conc in CONCURRENCY_LEVELS:
            pbar = tqdm(q_iter, desc=f"Neo4j {mode} x{conc}")
            for idx, (comp, query) in enumerate(pbar, 1):

                # ---------- WARM-UP ----------
                if(WARMUP_RUNS > 0):
                    logger.debug(f"[NEO_BENCHMARK] Starte Warm-up für Query #{idx}")
                    warm_ms = _run_and_time(
                        _warmup_parallel,        # <- neue Signatur
                        lambda q: _run_neo_query(q, driver),          # Query-Runner
                        query,                   # SQL-String
                        conc                     # concurrency
                    )
                    _log_csv(w, phase="warmup", db="neo4j", mode=mode,
                            conc=conc, idx=idx, repeat=0, comp=comp,
                            dur=warm_ms, avg_cpu=math.nan, iowait_pct=math.nan,
                            avg_mem=math.nan, per_q_ms=math.nan, qps=math.nan,
                            disk_mb=get_docker_disk_mb(container),
                            total_read_mb=0, total_write_mb=0,
                            net_recv_mb=0, net_send_mb=0,
                            stmt=query, res={"note": "warmup"})

                # ---------- STEADY-RUNS ----------
                for rep in range(1, REPETITIONS+1):
                    io0 = _read_iowait_jiffies(cid)
                    s0 = _read_cgroup_stats(cid)
                    t0 = time.perf_counter_ns()
                    with ThreadPoolExecutor(max_workers=conc) as ex:
                        futs = [ex.submit(_run_neo_query, query, driver)
                                for _ in range(conc)]
                        first_result = futs[0].result()
                    duration_ms = (time.perf_counter_ns()-t0)/1_000_000
                    io1 = _read_iowait_jiffies(cid)
                    s1 = _read_cgroup_stats(cid)
                    d  = _delta(s0, s1)
                    per_query_ms = duration_ms / conc               # Schnitt pro Request
                    qps          = conc * 1000 / duration_ms        # Concurrency / ms → / s
                    # ─── Kennzahlen berechnen ─────────────────────────────
                    elapsed_sec = duration_ms / 1000          # Messintervall
                    cpu_sec     = d["cpu_usec"] / 1_000_000    # Δ CPU-Zeit
                    delta_iowait_sec = (io1 - io0) / clk_tck
                    iowait_pct       = (delta_iowait_sec / (elapsed_sec * CPU_CORES)) * 100
                    avg_cpu = (cpu_sec / (elapsed_sec * CPU_CORES)) * 100     # kann >100% sein
                    avg_mem  = (s0["mem_now"] + s1["mem_now"]) / 2 / 1024**2
                    total_read_mb  = d["io_rbytes"] / 1_048_576
                    total_write_mb = d["io_wbytes"] / 1_048_576
                    net_recv_mb     = d["net_rbytes"] / 1_048_576
                    net_send_mb     = d["net_tbytes"] / 1_048_576
                    disk_mb = get_docker_disk_mb(container)

                    _log_csv(w, phase="steady", db="neo4j", mode=mode,
                             conc=conc, idx=idx, repeat=rep, comp=comp,
                             dur=duration_ms, per_q_ms=per_query_ms, qps=qps,
                             avg_cpu=avg_cpu, iowait_pct=iowait_pct, avg_mem=avg_mem, disk_mb=disk_mb,
                             total_read_mb=total_read_mb,
                             total_write_mb=total_write_mb,
                             net_recv_mb=net_recv_mb, net_send_mb=net_send_mb,
                             stmt=query, res=first_result)

    logger.info(f"[NEO_BENCHMARK] Benchmark abgeschlossen: {output.name}")

###############################################################################
# Öffentliche Funktionen -----------------------------------------------------
###############################################################################

def run_pg_normal(output_csv: str = "pg_normal_results.csv"):
    _pg_benchmark(PG_NORMAL_QUERIES, "pg_test_normal", "normal", Path("results") / output_csv)

def run_pg_optimized(output_csv: str = "pg_opt_results.csv"):
    _pg_benchmark(PG_OPT_QUERIES, "pg_test_optimized", "optimized", Path("results") / output_csv)

def run_neo_normal(output_csv: str = "neo_normal_results.csv"):
    _neo_benchmark(NEO_NORMAL_QUERIES, "neo5_test_normal", "normal", Path("results") / output_csv)

def run_neo_optimized(output_csv: str = "neo_opt_results.csv"):
    _neo_benchmark(NEO_OPT_QUERIES, "neo5_test_optimized", "optimized", Path("results") / output_csv)

###############################################################################
# CLI Entry‑Point ------------------------------------------------------------
###############################################################################

if __name__ == "__main__":

    parser = argparse.ArgumentParser("Benchmark Runner")
    parser.add_argument(
        "--variant", required=True, choices=[
            "pg_normal", "pg_opt", "neo_normal", "neo_opt"
        ],
        help="Welche DB getestet werden soll (z. B. --variant pg_opt)"
    )
    parser.add_argument("--users", type=int, required=True, help="Anzahl der Benutzer im Testlauf")
    parser.add_argument("--round", type=int, default=1, help="Rundenzähler für den Testlauf (default: 1)")
    parser.add_argument("--repetitions", type=int, default=3, help="Anzahl Wiederholungen für Messung (default: 3)")
    parser.add_argument("--warmups", type=int, default=2, help="Anzahl Warm-up-Runden (default: 1)")

    args = parser.parse_args()

    WARMUP_RUNS = args.warmups
    REPETITIONS = args.repetitions

    RESULTS_DIR = Path("results")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    csv_name = f"{args.users}_{args.variant}_{args.round}_{REPETITIONS}_{WARMUP_RUNS}_results.csv"
    csv_path = RESULTS_DIR / csv_name

    if args.variant == "pg_normal":
        run_pg_normal(csv_name)
    elif args.variant == "pg_opt":
        run_pg_optimized(csv_name)
    elif args.variant == "neo_normal":
        run_neo_normal(csv_name)
    elif args.variant == "neo_opt":
        run_neo_optimized(csv_name)