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


# ==========================================================
# PostgreSQL-Benchmark-Queries (Normal & Optimised)
# ==========================================================
PG_NORMAL_QUERIES: Dict[Complexity, List[str]] = {

    # ───────── SIMPLE ─────────
    Complexity.SIMPLE: [
        "SELECT id, name, price, stock, created_at, updated_at FROM products ORDER BY id LIMIT 10000;",
        "SELECT id, name FROM categories ORDER BY id LIMIT 1000;",
        "SELECT * FROM addresses ORDER BY id LIMIT 25;",
    ],

    # ───────── MEDIUM ─────────
    Complexity.MEDIUM: [
        # Produkte mit mindestens einer Kategorie
        """
        SELECT p.id, p.name, p.price, p.stock, p.created_at, p.updated_at
          FROM products p
         WHERE EXISTS ( SELECT 1
                          FROM product_categories pc
                         WHERE pc.product_id = p.id )
         ORDER BY p.id
         LIMIT 100;
        """,

        # 20 Positionen aus den zuletzt angelegten Bestellungen
        """
        SELECT p.id,
               p.name,
               p.price,
               p.stock,
               p.created_at,
               p.updated_at,
               oi.quantity
          FROM orders       o
          JOIN order_items  oi ON oi.order_id = o.id
          JOIN products     p  ON p.id        = oi.product_id
         ORDER BY o.created_at DESC, o.id DESC, p.id
         LIMIT 20;
        """,

        # fünf neueste Reviews (beliebige Produkte)
        """
        SELECT id,
               user_id,
               product_id,
               rating,
               created_at
          FROM reviews
         ORDER BY created_at DESC, id DESC
         LIMIT 5;
        """,
    ],

    # ───────── COMPLEX ─────────
    Complexity.COMPLEX: [
        # Bestellsummen pro Bestellung
        """
        SELECT o.id,
               o.created_at,
               SUM(oi.quantity * oi.price) AS total
          FROM orders       o
          JOIN order_items  oi ON oi.order_id = o.id
         GROUP BY o.id, o.created_at
         ORDER BY o.id
         LIMIT 50;
        """,

        # Produkte mit Ø-Rating > 4
        """
        SELECT p.id,
               p.name,
               AVG(r.rating) AS avg_rating
          FROM products p
          JOIN reviews  r ON r.product_id = p.id
         GROUP BY p.id, p.name
        HAVING AVG(r.rating) > 4
         ORDER BY avg_rating DESC, p.id LIMIT 200;
        """,

        # Bestellungen der letzten 30 Tage pro User (>0)
        """
        SELECT u.id,
               COUNT(*) AS orders_last_30d
          FROM users  u
          JOIN orders o ON o.user_id = u.id
         WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
         GROUP BY u.id
       HAVING COUNT(*) > 0
         ORDER BY u.id
         LIMIT 50;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Sell: meistgekauftes Produkt & zugehörige Empfehlungen
        """
        WITH top_prod AS (
                SELECT product_id
                  FROM order_items
                 GROUP BY product_id
                 ORDER BY COUNT(*) DESC, product_id
                 LIMIT 1
        ),
        buyers AS (
                SELECT DISTINCT o.user_id
                  FROM orders       o
                  JOIN order_items oi ON oi.order_id = o.id
                 WHERE oi.product_id = (SELECT product_id FROM top_prod)
        )
        SELECT oi2.product_id AS rec_id,
               COUNT(*)       AS freq
          FROM orders       o
          JOIN order_items  oi2 ON oi2.order_id = o.id
         WHERE o.user_id IN (SELECT user_id FROM buyers)
           AND oi2.product_id <> (SELECT product_id FROM top_prod)
         GROUP BY oi2.product_id
         ORDER BY freq DESC, oi2.product_id
         LIMIT 20;
        """,

        # Produkte, die ein User sowohl angesehen als auch gekauft hat
        """
        SELECT DISTINCT p.id,
                        p.name
          FROM product_views v
          JOIN orders        o  ON o.user_id    = v.user_id
          JOIN order_items   oi ON oi.order_id  = o.id
          JOIN products      p  ON p.id         = v.product_id
                               AND p.id         = oi.product_id
         ORDER BY p.id
         LIMIT 25;
        """,

        # Zwei-Hop-Netz rund um dasselbe Top-Produkt
        """
        WITH top_prod AS (
                SELECT product_id
                  FROM order_items
                 GROUP BY product_id
                 ORDER BY COUNT(*) DESC, product_id
                 LIMIT 1
        ),
        buyers AS (
                SELECT DISTINCT user_id
                  FROM orders       o
                  JOIN order_items oi ON oi.order_id = o.id
                 WHERE oi.product_id = (SELECT product_id FROM top_prod)
        )
        SELECT oi2.product_id,
               COUNT(*) AS freq
          FROM orders       o
          JOIN order_items  oi2 ON oi2.order_id = o.id
         WHERE o.user_id IN (SELECT user_id FROM buyers)
           AND oi2.product_id <> (SELECT product_id FROM top_prod)
         GROUP BY oi2.product_id
         ORDER BY freq DESC, oi2.product_id
         LIMIT 20;
        """,
    ],

    # ───────── CREATE ─────────
    Complexity.CREATE: [
        # neue Adresse
        """
        INSERT INTO addresses (user_id, street, city, zip, country, is_primary)
        VALUES (
            (SELECT id FROM users LIMIT 1),
            'Foo-' || gen_random_uuid()::text,
            'Bar City',
            '12345',
            'DE',
            FALSE
        )
        RETURNING id AS address_id;
        """,

        # neue Bestellung
        """
        INSERT INTO orders (user_id, status, total, created_at)
        VALUES (
            (SELECT id FROM users LIMIT 1),
            'pending',
            0.0,
            CURRENT_TIMESTAMP
        )
        RETURNING id AS order_id;
        """,

        # Cart-Item
        """
        INSERT INTO cart_items (user_id, product_id, quantity, added_at)
        VALUES (
            (SELECT id FROM users    LIMIT 1),
            (SELECT id FROM products LIMIT 1),
            2,
            CURRENT_TIMESTAMP
        )
        RETURNING id AS cart_item_id;
        """,

        # Produkt-View
        """
        INSERT INTO product_views (user_id, product_id, viewed_at)
        VALUES (
            (SELECT id FROM users    LIMIT 1),
            (SELECT id FROM products LIMIT 1),
            CURRENT_TIMESTAMP
        )
        RETURNING id AS product_view_id;
        """
    ],

    # ───────── UPDATE ─────────
    Complexity.UPDATE: [
        """
        UPDATE products
        SET stock = stock + 1
        WHERE id = (SELECT id FROM products LIMIT 1)
        RETURNING id AS product_id, stock AS new_stock;
        """,

        """
        UPDATE reviews
        SET rating = GREATEST(rating - 1, 1)
        WHERE id = (SELECT id FROM reviews LIMIT 1)
        RETURNING id AS review_id, rating AS new_rating;
        """,

        """
        UPDATE cart_items
        SET quantity = quantity + 3
        WHERE id = (SELECT id FROM cart_items LIMIT 1)
        RETURNING id AS cart_item_id, quantity AS new_quantity;
        """,

        """
        UPDATE users
        SET email = email || '.tmp'
        WHERE id = (SELECT id FROM users LIMIT 1)
        RETURNING id AS user_id, email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        """
        WITH victim AS (
            SELECT id
            FROM   addresses
            ORDER  BY id
            LIMIT  1
        )
        DELETE FROM addresses a
        USING victim
        WHERE a.id = victim.id
        RETURNING a.id AS deleted_address_id;
        """,

        # 2) Review löschen
        """
        WITH victim AS (
            SELECT id
            FROM   reviews
            ORDER  BY id
            LIMIT  1
        )
        DELETE FROM reviews r
        USING victim
        WHERE r.id = victim.id
        RETURNING r.id AS deleted_review_id;
        """,

        # 3) Cart-Item löschen
        """
        WITH victim AS (
            SELECT id
            FROM   cart_items
            ORDER  BY id
            LIMIT  1
        )
        DELETE FROM cart_items c
        USING victim
        WHERE c.id = victim.id
        RETURNING c.id AS deleted_cart_item_id;
        """,

        # 4) Product-Purchase löschen
        """
        WITH victim AS (
            SELECT id
            FROM   product_purchases
            ORDER  BY id
            LIMIT  1
        )
        DELETE FROM product_purchases pp
        USING victim
        WHERE pp.id = victim.id
        RETURNING pp.id AS deleted_purchase_id;
        """
    ],
}


PG_OPT_QUERIES = PG_NORMAL_QUERIES   # gleiche SQL-Syntax


# ==========================================================
#  Neo4j  (NORMAL & OPTIMISED)
# ==========================================================
# - Die inhaltliche Reihenfolge/Anzahl ist identisch zu PG.
# - Optimised-Variante nimmt die kürzeren Relationen (CONTAINS, REVIEWED …)
#   – ansonsten exakt dieselbe Logik & Zählweise.
NEO_NORMAL_QUERIES = {

    # ───────── SIMPLE ─────────
    Complexity.SIMPLE: [
        """
        MATCH (p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 10000;
        """,

        """
        MATCH (c:Category)
        RETURN c.id   AS id,
               c.name AS name
        ORDER BY id
        LIMIT 1000;
        """,

        """
        MATCH (a:Address)
        RETURN a.id         AS id,
               a.user_id    AS user_id,
               a.street     AS street,
               a.city       AS city,
               a.zip        AS zip,
               a.country    AS country,
               a.is_primary AS is_primary
        ORDER BY id
        LIMIT 25;
        """,
    ],

    # ───────── MEDIUM ─────────
    Complexity.MEDIUM: [
        # Produkte mit mindestens einer Kategorie
        """
        MATCH (p:Product)-[:BELONGS_TO]->(:Category)
        WITH DISTINCT p
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 100;
        """,

        # letzte 20 Bestellungen + Positionen
        """
        MATCH (o:Order)
        WITH o ORDER BY o.created_at DESC, o.id DESC LIMIT 20
        MATCH (o)-[:HAS_ITEM]->(oi:OrderItem)-[:REFERS_TO]->(p:Product)
        RETURN 
               p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at,
               oi.quantity  AS quantity
        ORDER BY o.created_at DESC, o.id DESC, id
        LIMIT 20;
        """,

        # fünf neueste Reviews (Knoten)
        """
        MATCH (r:Review)
        RETURN r.id         AS id,
               r.user_id    AS user_id,
               r.product_id AS product_id,
               r.rating     AS rating,
               r.created_at AS created_at
        ORDER BY created_at DESC, id DESC
        LIMIT 5;
        """,
    ],

    # ───────── COMPLEX ─────────
    Complexity.COMPLEX: [
        # Bestellsummen pro Order
        """
        MATCH (o:Order)-[:HAS_ITEM]->(oi:OrderItem)
        WITH o, SUM(oi.quantity * oi.price) AS total
        RETURN o.id         AS id,
               o.created_at AS created_at,
               total        AS total
        ORDER BY id
        LIMIT 50;
        """,

        # Produkte mit Ø-Rating > 4
        """
        MATCH (p:Product)<-[:REVIEWS]-(r:Review)
        WITH p, AVG(r.rating) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id        AS id,
               p.name      AS name,
               avg_rating  AS avg_rating
        ORDER BY avg_rating DESC, id
        LIMIT 200;
        """,

        # Bestellungen der letzten 30 Tage pro User
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE datetime(o.created_at) >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS orders_last_30d
        WHERE orders_last_30d > 0
        RETURN u.id            AS id,
               orders_last_30d AS orders_last_30d
        ORDER BY id
        LIMIT 50;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Sell basierend auf meistverkauftem Produkt
        """
        MATCH (:Order)-[:HAS_ITEM]->(oi1:OrderItem)
        WITH oi1.product_id AS prod , COUNT(*) AS freq
        ORDER BY freq DESC , prod LIMIT 1 // tiebreak = product_id
        WITH prod AS top_prod
        MATCH (u:User)-[:PLACED]->(:Order)-[:HAS_ITEM]->(:OrderItem {product_id: top_prod})
        WITH DISTINCT u , top_prod
        MATCH (u)-[:PLACED]->(:Order)-[:HAS_ITEM]->(oi2:OrderItem)
        WHERE oi2.product_id <> top_prod
        RETURN oi2.product_id AS rec_id ,
        COUNT(*) AS freq
        ORDER BY freq DESC , rec_id
        LIMIT 20;
        """,

        # View ∩ Purchase
        """
        MATCH (u:User)-[:VIEWED]->(:ProductView)-[:VIEWED_PRODUCT]->(p:Product)
        MATCH (u)-[:PLACED]->(:Order)-[:HAS_ITEM]->(:OrderItem {product_id: p.id})
        RETURN DISTINCT p.id   AS id,
                        p.name AS name
        ORDER BY id
        LIMIT 25;
        """,


        # Zwei-Hop-Netz um dasselbe Top-Produkt
        """
        MATCH (:Order)-[:HAS_ITEM]->(oi:OrderItem)
        WITH oi.product_id AS prod , COUNT(*) AS freq
        ORDER BY freq DESC , prod LIMIT 1
        WITH prod AS top_prod
        MATCH (u:User)-[:PLACED]->(:Order)-[:HAS_ITEM]->(:OrderItem {product_id: top_prod})
        WITH DISTINCT u , top_prod
        MATCH (u)-[:PLACED]->(:Order)-[:HAS_ITEM]->(oi2:OrderItem)
        WHERE oi2.product_id <> top_prod
        RETURN oi2.product_id AS product_id ,
        COUNT(*) AS freq
        ORDER BY freq DESC , product_id
        LIMIT 20;
        """,
    ],

    # ───────── CREATE ─────────
    Complexity.CREATE: [
        """
        OPTIONAL MATCH (a:Address)
        WITH coalesce(max(a.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        CREATE (u)-[:HAS_ADDRESS]->(a:Address {
        id: new_id ,
        street: 'Foo' ,
        city: 'Bar City' ,
        zip: '12345' ,
        country: 'DE' ,
        is_primary:false
        })
        RETURN a.id AS address_id;
        """,

        """
        OPTIONAL MATCH (o:Order)
        WITH coalesce(max(o.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        CREATE (u)-[:PLACED]->(o:Order {
        id: new_id ,
        status:'pending' ,
        total: 0.0 ,
        created_at: datetime()
        })
        RETURN o.id AS order_id;
        """,

        """
        OPTIONAL MATCH (ci:CartItem)
        WITH coalesce(max(ci.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        MATCH (p:Product) WITH u,p,new_id LIMIT 1
        CREATE (ci:CartItem {
        id: new_id ,
        user_id: u.id ,
        product_id: p.id ,
        quantity: 2 ,
        added_at: datetime()
        })
        CREATE (u)-[:HAS_IN_CART]->(ci)
        CREATE (ci)-[:CART_PRODUCT]->(p)
        RETURN ci.id AS cart_item_id;
        """,

        """
        OPTIONAL MATCH (pv:ProductView)
        WITH coalesce(max(pv.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        MATCH (p:Product) WITH u,p,new_id LIMIT 1
        CREATE (pv:ProductView {
        id: new_id ,
        user_id: u.id ,
        product_id: p.id ,
        viewed_at: datetime()
        })
        CREATE (u)-[:VIEWED]->(pv)
        CREATE (pv)-[:VIEWED_PRODUCT]->(p)
        RETURN pv.id AS product_view_id;
        """
    ],

    # ───────── UPDATE ─────────
    Complexity.UPDATE: [
        # 1) Lagerbestand +1  → node-id + neuer Stock
        """
        MATCH (p:Product)
        WITH p ORDER BY p.id LIMIT 1
        SET p.stock = coalesce(p.stock,0) + 1
        RETURN p.id AS product_id,
        p.stock AS new_stock;
        """,

        # 2) Rating −1 (min. 1)  → review_id + neues Rating
        """
        MATCH (r:Review) WITH r LIMIT 1
        SET   r.rating = CASE
                            WHEN toInteger(r.rating) > 1
                            THEN toInteger(r.rating) - 1
                            ELSE 1
                        END
        RETURN r.id     AS review_id,
            r.rating AS new_rating;
        """,

        # 3) Cart-Menge +3  → cartItem-id + neue Quantity
        """
        MATCH (ci:CartItem)
        WITH ci ORDER BY ci.id       /* deterministisch: kleinste id */
        LIMIT 1
        SET   ci.quantity = coalesce(toInteger(ci.quantity), 0) + 3
        RETURN ci.id       AS cart_item_id,
            ci.quantity AS new_quantity;
        """,

        # 4) E-Mail suffix  → user_id + neue Mail
        """
        MATCH (u:User) WITH u LIMIT 1
        SET   u.email = u.email + '.tmp'
        RETURN u.id   AS user_id,
            u.email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        # 1) Adresse löschen  → gelöschte id
        """
        MATCH (a:Address)
        WITH a ORDER BY a.id ASC
        LIMIT 1
        WITH a.id    AS deleted_address_id, a
        DETACH DELETE a
        RETURN deleted_address_id;
        """,

        # 2) Review-Knoten löschen
        """
        MATCH (r:Review)
        WITH r ORDER BY r.id ASC
        LIMIT 1
        WITH r.id AS deleted_review_id, r
        DETACH DELETE r
        RETURN deleted_review_id;
        """,

        # 3) Cart-Item-Knoten löschen
        """
        MATCH (ci:CartItem)
        WITH ci ORDER BY ci.id ASC
        LIMIT 1
        WITH ci.id AS deleted_cart_item_id, ci
        DETACH DELETE ci
        RETURN deleted_cart_item_id;
        """,

        # 4) Product-Purchase-Relationship löschen
        """
        MATCH (pp:ProductPurchase)
        WITH pp ORDER BY pp.id ASC
        LIMIT 1
        WITH pp.id AS deleted_purchase_id, pp
        DETACH DELETE pp
        RETURN deleted_purchase_id;
        """
    ],
}


# --- Neo4j optimiert --------------------------------------------------------
NEO_OPT_QUERIES = {

    # ───────── SIMPLE ─────────
    Complexity.SIMPLE: [
        """
        MATCH (p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 10000;
        """,

        # 2) Kategorien
        """
        MATCH (c:Category)
        RETURN c.id   AS id,
               c.name AS name
        ORDER BY id
        LIMIT 1000;
        """,

        # 3) Adressen
        """
        MATCH (a:Address)
        RETURN a.id         AS id,
               a.user_id    AS user_id,
               a.street     AS street,
               a.city       AS city,
               a.zip        AS zip,
               a.country    AS country,
               a.is_primary AS is_primary
        ORDER BY id
        LIMIT 25;
        """,
    ],

    # ───────── MEDIUM ─────────
    Complexity.MEDIUM: [
        # Produkte mit ≥ 1 Kategorie
        """
        MATCH (p:Product)-[:BELONGS_TO]->(:Category)
        WITH DISTINCT p
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 100;
        """,

        # 20 Positionen aus den letzten Bestellungen
        """
        MATCH (o:Order)
        WITH o ORDER BY o.created_at DESC, o.id DESC LIMIT 20
        MATCH (o)-[oi:CONTAINS]->(p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at,
               oi.quantity  AS quantity
        ORDER BY o.created_at DESC, o.id DESC, id
        LIMIT 20;
        """,

        # fünf neueste Reviews (Relationship-basiert)
        """
        MATCH (u:User)-[rev:REVIEWED]->(p:Product)
        RETURN rev.id        AS id,
               u.id          AS user_id,
               p.id          AS product_id,
               rev.rating    AS rating,
               rev.created_at AS created_at
        ORDER BY rev.created_at DESC, id DESC
        LIMIT 5;
        """,
    ],

    # ───────── COMPLEX ─────────
    Complexity.COMPLEX: [
        # Bestellsummen pro Bestellung
        """
        MATCH (o:Order)-[oi:CONTAINS]->(p:Product)
        WITH o, SUM(toInteger(oi.quantity) * toFloat(oi.price)) AS total
        RETURN o.id         AS id,
               o.created_at AS created_at,
               total        AS total
        ORDER BY id
        LIMIT 50;
        """,

        # Produkte mit Ø-Rating > 4
        """
        MATCH (p:Product)<-[rev:REVIEWED]-()
        WITH p, AVG(toFloat(rev.rating)) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id       AS id,
               p.name     AS name,
               avg_rating AS avg_rating
        ORDER BY avg_rating DESC, id
        LIMIT 200;
        """,

        # Bestellungen der letzten 30 Tage pro User
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE datetime(o.created_at) >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS orders_last_30d
        RETURN u.id            AS id,
               orders_last_30d AS orders_last_30d
        ORDER BY id
        LIMIT 50;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Sell (meistverkauftes Produkt → weitere Käufe)
        """
        MATCH (:Order)-[:CONTAINS]->(p1:Product)
        WITH p1, COUNT(*) AS freq
        ORDER BY freq DESC, p1.id
        LIMIT 1                               // top product

        MATCH (u:User)-[:PLACED]->(:Order)-[:CONTAINS]->(p1)
        WITH DISTINCT u, p1
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p2:Product)
        WHERE p2 <> p1
        RETURN p2.id  AS rec_id,
               COUNT(*) AS freq
        ORDER BY freq DESC, rec_id
        LIMIT 20;
        """,

        # View ∩ Purchase
        """
        MATCH (u:User)-[:VIEWED]->(p:Product)
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p)
        RETURN DISTINCT p.id   AS id,
                        p.name AS name
        ORDER BY id
        LIMIT 25;
        """,

        # Zwei-Hop um Top-Produkt
        """
        MATCH (:Order)-[:CONTAINS]->(tp:Product)
        WITH tp, COUNT(*) AS freq
        ORDER BY freq DESC, tp.id
        LIMIT 1

        MATCH (u:User)-[:PLACED]->(:Order)-[:CONTAINS]->(tp)
        WITH DISTINCT u, tp
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p2:Product)
        WHERE p2 <> tp
        RETURN p2.id  AS prod_id,
               COUNT(*) AS freq
        ORDER BY freq DESC, prod_id
        LIMIT 20;
        """,
    ],

    # ───────── CREATE ─────────
    Complexity.CREATE: [
        # 1) Adresse
        """
        OPTIONAL MATCH (a:Address)
        WITH coalesce(max(a.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        CREATE (u)-[:HAS_ADDRESS]->(a:Address {
            id:         new_id,
            street:     'Foo',
            city:       'Bar City',
            zip:        '12345',
            country:    'DE',
            is_primary: false
        })
        RETURN a.id AS address_id;
        """,

        # 2) Order
        """
        OPTIONAL MATCH (o:Order)
        WITH coalesce(max(o.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        CREATE (u)-[:PLACED]->(o:Order {
            id:         new_id,
            status:     'pending',
            total:      0.0,
            created_at: datetime()
        })
        RETURN o.id AS order_id;
        """,

        # 3) HAS_IN_CART-Relationship (mit eigener id)
        """
        OPTIONAL MATCH ()-[c:HAS_IN_CART]-()
        WITH coalesce(max(c.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        MATCH (p:Product) WITH u,p,new_id LIMIT 1
        CREATE (u)-[c:HAS_IN_CART {
            id:       new_id,
            quantity: 2,
            added_at: datetime()
        }]->(p)
        RETURN c.id AS cart_rel_id;
        """,

        # 4) VIEWED-Relationship (mit eigener id)
        """
        OPTIONAL MATCH ()-[v:VIEWED]-()
        WITH coalesce(max(v.id),0)+1 AS new_id
        MATCH (u:User) WITH u,new_id LIMIT 1
        MATCH (p:Product) WITH u,p,new_id LIMIT 1
        CREATE (u)-[v:VIEWED {
            id:        new_id,
            viewed_at: datetime()
        }]->(p)
        RETURN v.id AS view_rel_id;
        """
    ],

    # ───────── UPDATE ─────────
    Complexity.UPDATE: [
        # 1) Stock +1
        """
        MATCH (p:Product) WITH p ORDER BY p.id LIMIT 1
        SET   p.stock = coalesce(p.stock,0) + 1
        RETURN p.id   AS product_id,
               p.stock AS new_stock;
        """,

        # 2) Rating −1 (Relationship)
        """
        MATCH ()-[rev:REVIEWED]-()
        WITH  rev ORDER BY rev.id LIMIT 1
        SET   rev.rating = CASE
                             WHEN toInteger(rev.rating) > 1
                             THEN toInteger(rev.rating) - 1
                             ELSE 1
                           END
        RETURN rev.id     AS review_rel_id,
               rev.rating AS new_rating;
        """,

        # 3) Cart-Menge +3
        """
        MATCH ()-[c:HAS_IN_CART]-()
        WITH  c ORDER BY c.id LIMIT 1
        SET   c.quantity = coalesce(toInteger(c.quantity),0) + 3
        RETURN c.id       AS cart_rel_id,
               c.quantity AS new_quantity;
        """,

        # 4) E-Mail-Suffix
        """
        MATCH (u:User) WITH u LIMIT 1
        SET   u.email = u.email + '.tmp'
        RETURN u.id   AS user_id,
               u.email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        # 1) Adresse-Knoten
        """
        MATCH (a:Address)
        WITH  a ORDER BY a.id LIMIT 1
        WITH  a.id AS deleted_address_id, a
        DETACH DELETE a
        RETURN deleted_address_id;
        """,

        # 2) REVIEWED-Relationship
        """
        MATCH ()-[rev:REVIEWED]-()
        WITH  rev ORDER BY rev.id LIMIT 1
        WITH  rev.id AS deleted_review_rel_id, rev
        DELETE rev
        RETURN deleted_review_rel_id;
        """,

        # 3) HAS_IN_CART-Relationship
        """
        MATCH ()-[c:HAS_IN_CART]-()
        WHERE c.id IS NOT NULL
        WITH  c ORDER BY c.id LIMIT 1
        WITH  c.id AS deleted_cart_rel_id, c
        DELETE c
        RETURN deleted_cart_rel_id;
        """,

        # 4) PURCHASED-Relationship
        """
        MATCH ()-[pur:PURCHASED]-()
        WHERE pur.id IS NOT NULL
        WITH  pur ORDER BY pur.id LIMIT 1
        WITH  pur.id AS deleted_purchase_rel_id, pur
        DELETE pur
        RETURN deleted_purchase_rel_id;
        """
    ],
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