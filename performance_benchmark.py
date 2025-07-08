"""
performance_benchmark.py
============================================================
Dieses Skript führt systematische Benchmarks auf vier Datenbankvarianten durch (PostgreSQL/Neo4j jeweils normal & optimiert).
Dabei werden 24 Abfragen auf 4 parallelen Laststufen (1, 3, 5, 10 Threads) getestet.

Pro Benchmarklauf werden folgende Kennzahlen erfasst:
* Dauer in Millisekunden (Gesamtzeit aller parallelen Threads)
* CPU-Last des Docker-Containers in Prozent
* RAM-Verbrauch in Megabyte
* belegter Plattenspeicher in Megabyte (SizeRootFs)
* Komplexität der Query (einfache bis komplexe Abfragen)

Die Ergebnisse werden in eine CSV-Datei <variant>_results.csv geschrieben.
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
from functools import partial
from tqdm import tqdm
import math
import argparse
import json
import os
import logging
import multiprocessing, pathlib

# Pfad zur cgroup v2 (für Ressourcenmessung bei Docker-Containern)
CG_PATH = pathlib.Path("/sys/fs/cgroup")

# Anzahl der verfügbaren CPU-Kerne (für Normalisierung der CPU-Last)
CPU_CORES = multiprocessing.cpu_count()

# ───── Logging-Konfiguration ─────
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


def _read_cgroup_stats(cid: str) -> dict[str, int]:
    """
    Liefert kumulative CPU-, Block-I/O-, Netz-I/O- und Speicher-Statistiken
    eines Containers.  Fällt bei Docker-Desktop automatisch auf `docker exec`
    zurück.
    """
    cdir = CG_PATH / cid
    use_exec = not cdir.exists()  # Fallback bei fehlendem Zugriff auf cgroup v2

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

    # ---------- Memory ----------
    mem_now  = int(_read_file("memory.current").strip())

    return {
        "cpu_usec":  cpu_usec,
        "mem_now":   mem_now,
    }


def _delta(before: dict, after: dict) -> dict:
    # Berechnet Differenzen zwischen zwei Messpunkten
    return {k: after[k] - before[k] for k in before}


def _bytes_to_mb(b: int) -> float:
    return b / 1024**2


def _volume_usage(name: str) -> int:
    """
    Liefert belegte Bytes eines Docker-Volumes.
    Nutzt .UsageData.Size (Docker 20.10 +).  Fällt bei Fehler auf 0 B zurück.
    """
    try:
        out = subprocess.check_output(
            ["docker", "volume", "inspect",
             "--format", "{{.UsageData.Size}}", name],
            text=True, stderr=subprocess.DEVNULL
        ).strip() or "0"
        return int(out)
    except Exception:
        logger.debug(f"[DISK] Volumengröße für '{name}' nicht ermittelbar – 0 B angenommen.")
        return 0


def get_docker_disk_mb(container: str) -> float:
    """
    Gibt die **Gesamtgröße** (MB) zurück:

        SizeRootFs  +  Σ UsageData.Size aller Volume-Mounts
    """
    root_bytes = 0
    vol_bytes  = 0

    # 1️⃣  beschreibbarer Layer
    try:
        root_str = subprocess.check_output(
            ["docker", "container", "inspect", "--size",
             "--format", "{{.SizeRootFs}}", container],
            text=True).strip() or "0"
        root_bytes = int(root_str)
    except Exception as e:
        logger.warning(f"[DISK] Root-FS-Größe für '{container}' nicht ermittelbar: {e}")

    # 2️⃣  Volumes des Containers
    try:
        mounts_json = subprocess.check_output(
            ["docker", "container", "inspect",
             "--format", "{{json .Mounts}}", container],
            text=True).strip()
        mounts = json.loads(mounts_json)

        for m in mounts:
            if m.get("Type") == "volume":
                vol_bytes += _volume_usage(m["Name"])

    except Exception as e:
        logger.warning(f"[DISK] Volume-Infos für '{container}' fehlen: {e}")

    total_mb = _bytes_to_mb(root_bytes + vol_bytes)
    logger.debug(
        f"[DISK] {container}: root={_bytes_to_mb(root_bytes):.1f} MB, "
        f"volumes={_bytes_to_mb(vol_bytes):.1f} MB → total={total_mb:.1f} MB"
    )

    return total_mb if total_mb > 0 else math.nan


###############################################################################
# Query‑Definitionen ---------------------------------------------------------
###############################################################################

class Complexity(Enum):
    SIMPLE = "simple"               # Einfache Abfrage, geringe Last und geringe Ausführungskomplexität
    MEDIUM = "medium"               # Mittlere Abfrage mit leicht erhöhter Last oder Verknüpfung
    COMPLEX = "complex"             # Komplexere Abfrage mit mehreren Joins oder Bedingungen
    VERY_COMPLEX = "very_complex"   # Sehr aufwändige Abfrage, z. B. mit Aggregationen, Subqueries oder mehreren Joins
    CREATE = "create"               # Schreiboperation: Einfügen neuer Daten
    UPDATE = "update"               # Schreiboperation: Aktualisieren bestehender Daten
    DELETE = "delete"               # Schreiboperation: Löschen von Daten


# ==========================================================
# PostgreSQL-Benchmark-Queries (Normal & Optimised)
# ==========================================================
PG_NORMAL_QUERIES: Dict[Complexity, List[str]] = {

    # ───────── SIMPLE ─────────
    Complexity.SIMPLE: [
        # Gibt eine große Menge an Produktdaten zurück – Test für reine Leselast auf einer Einzel-Tabelle mit ORDER BY
        "SELECT id, name, price, stock, created_at, updated_at FROM products ORDER BY id LIMIT 50000;",

        # Abruf aller Kategorienamen – kleinerer Umfang, geringer Aufwand
        "SELECT id, name FROM categories ORDER BY id LIMIT 5000;",

        # Abfrage auf Adressen mit kleiner Ergebnisgröße – Basis-Leseoperation
        "SELECT * FROM addresses ORDER BY id LIMIT 1000;",
    ],

    # ───────── MEDIUM ─────────
    Complexity.MEDIUM: [
        # Liefert alle Produkte, die mindestens einer Kategorie zugeordnet sind (Existenzprüfung über Subquery)
        """
        SELECT p.id, p.name, p.price, p.stock, p.created_at, p.updated_at
          FROM products p
         WHERE EXISTS ( SELECT 1
                          FROM product_categories pc
                         WHERE pc.product_id = p.id )
         ORDER BY p.id
         LIMIT 1000;
        """,

        # Holt Artikel aus den zuletzt erstellten Bestellungen (JOINs über drei Tabellen, sortiert nach Zeit)
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
         LIMIT 500;
        """,

        # Gibt die fünf neuesten Bewertungen zurück – moderate Datenmenge mit ORDER BY auf Datum
        """
        SELECT id,
               user_id,
               product_id,
               rating,
               created_at
          FROM reviews
         ORDER BY created_at DESC, id DESC
         LIMIT 100;
        """,
    ],

    # ───────── COMPLEX ─────────
    Complexity.COMPLEX: [
        # Berechnet die Gesamtsumme jeder Bestellung über Menge × Preis – Gruppierung erforderlich
        """
        SELECT o.id,
               o.created_at,
               SUM(oi.quantity * oi.price) AS total
          FROM orders       o
          JOIN order_items  oi ON oi.order_id = o.id
         GROUP BY o.id, o.created_at
         ORDER BY o.id
         LIMIT 500;
        """,

        # Ermittelt Produkte mit einem durchschnittlichen Rating über 4 – Aggregation mit HAVING
        """
        SELECT p.id,
               p.name,
               AVG(r.rating) AS avg_rating
          FROM products p
          JOIN reviews  r ON r.product_id = p.id
         GROUP BY p.id, p.name
        HAVING AVG(r.rating) > 4
         ORDER BY avg_rating DESC, p.id LIMIT 1000;
        """,

        # Gibt Anzahl der Bestellungen in den letzten 30 Tagen pro User zurück, sofern mind. 1 Bestellung vorhanden ist
        """
        SELECT u.id,
               COUNT(*) AS orders_last_30d
          FROM users  u
          JOIN orders o ON o.user_id = u.id
         WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
         GROUP BY u.id
       HAVING COUNT(*) > 0
         ORDER BY u.id
         LIMIT 500;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Selling-Analyse: ermittelt Produkte, die von Käufern des Top-Sellers ebenfalls häufig gekauft wurden
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
         LIMIT 100;
        """,

        #Produkt-Co-Occurrence –  Top-25 Produktpaare, die gemeinsam
        #wenigstens einmal in derselben Bestellung auftauchten.
        #• Jede Bestellung zählt pro Paar nur einmal
        #• Reihenfolge der IDs wird festgelegt, damit (A,B) = (B,A)
        """
        WITH pairs AS (
            SELECT
                LEAST(oi1.product_id, oi2.product_id)      AS prodA,
                GREATEST(oi1.product_id, oi2.product_id)   AS prodB,
                oi1.order_id                               AS order_id
            FROM   order_items  oi1
            JOIN   order_items  oi2
                ON  oi2.order_id   = oi1.order_id
                AND oi2.product_id > oi1.product_id     -- Duplikate + Selbstpaare raus
        )
        SELECT  prodA,
                prodB,
                COUNT(DISTINCT order_id) AS co_orders      -- ⇦ jede Bestellung nur 1-mal
        FROM    pairs
        GROUP  BY prodA, prodB
        -- HAVING COUNT(DISTINCT order_id) >= 2          -- (falls Mindest-Support gewünscht)
        ORDER BY co_orders DESC, prodA, prodB
        LIMIT 100;
        """,

        # Zwei-Hop-Empfehlung: Welche Produkte kaufen Nutzer, die bereits den Top-Seller gekauft haben – zielt auf Relevanznetzwerk
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
         LIMIT 100;
        """,
    ],

        # ───────── CREATE ─────────
    Complexity.CREATE: [
        # Fügt eine neue Adresse ein – verwendet einen beliebigen User (LIMIT 1) und generiert zufällige Straße
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

        # Legt eine neue Bestellung mit Status 'pending' und Betrag 0.0 an – Zeitstempel ist aktuelle Uhrzeit
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

        # Fügt einen neuen Warenkorb-Eintrag für ein Produkt ein – testweise mit Menge 2
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

        # Erfasst eine Produktansicht mit aktuellem Zeitstempel – simuliert View-Event
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
        # Erhöht den Lagerbestand eines Produkts um 1 – simuliert z. B. Rücklieferung oder Korrektur
        """
        UPDATE products
        SET stock = stock + 1
        WHERE id = (SELECT id FROM products LIMIT 1)
        RETURNING id AS product_id, stock AS new_stock;
        """,

        # Verringert die Bewertung eines Reviews, aber nicht unter 1 – vermeidet ungültige Werte
        """
        UPDATE reviews
        SET rating = GREATEST(rating - 1, 1)
        WHERE id = (SELECT id FROM reviews LIMIT 1)
        RETURNING id AS review_id, rating AS new_rating;
        """,

        # Erhöht die Menge eines Warenkorbeintrags – simuliert erneutes Hinzufügen desselben Produkts
        """
        UPDATE cart_items
        SET quantity = quantity + 3
        WHERE id = (SELECT id FROM cart_items LIMIT 1)
        RETURNING id AS cart_item_id, quantity AS new_quantity;
        """,

        # Modifiziert die E-Mail eines Nutzers testweise – dient als Dummy-Update
        """
        UPDATE users
        SET email = email || '.tmp'
        WHERE id = (SELECT id FROM users LIMIT 1)
        RETURNING id AS user_id, email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        # Löscht eine Adresse – der zu löschende Eintrag wird zuvor per CTE ausgewählt
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

        # Löscht ein Review – ebenfalls über CTE ausgewählt, um das Ziel isoliert zu bestimmen
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

        # Entfernt einen Warenkorbeintrag – einfache CTE-Löschung mit Rückgabe
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

        # Löscht einen Produkteinkauf – Verwendung analog zu den vorherigen CTEs
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
    ]
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
        # Ruft bis zu 10.000 Produktknoten mit Basisattributen ab – sortiert nach ID
        """
        MATCH (p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 50000;
        """,

        # Liest maximal 1.000 Kategorien aus – Rückgabe von ID und Name
        """
        MATCH (c:Category)
        RETURN c.id   AS id,
               c.name AS name
        ORDER BY id
        LIMIT 5000;
        """,

        # Gibt 25 Adressen mit vollständigem Attributsatz zurück – sortiert nach ID
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
        LIMIT 1000;
        """,
    ],

    # ───────── MEDIUM ─────────
    Complexity.MEDIUM: [
        # Produkte, die mindestens einer Kategorie zugeordnet sind – Duplikate entfernt
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
        LIMIT 1000;
        """,

        # Ermittelt die letzten 20 Bestellungen und zugehörige Produkte – inkl. Menge
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
        LIMIT 500;
        """,

        # Liefert die 5 neuesten Reviews – nach Erstellungsdatum und ID sortiert
        """
        MATCH (r:Review)
        RETURN r.id         AS id,
               r.user_id    AS user_id,
               r.product_id AS product_id,
               r.rating     AS rating,
               r.created_at AS created_at
        ORDER BY created_at DESC, id DESC
        LIMIT 100;
        """,
    ],

        # ───────── COMPLEX ─────────
    Complexity.COMPLEX: [
        # Aggregiert Bestellsummen pro Order (Menge × Preis je Position)
        """
        MATCH (o:Order)-[:HAS_ITEM]->(oi:OrderItem)
        WITH o, SUM(oi.quantity * oi.price) AS total
        RETURN o.id         AS id,
               o.created_at AS created_at,
               total        AS total
        ORDER BY id
        LIMIT 500;
        """,

        # Produkte mit durchschnittlicher Bewertung > 4 (nach Bewertung absteigend)
        """
        MATCH (p:Product)<-[:REVIEWS]-(r:Review)
        WITH p, AVG(r.rating) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id        AS id,
               p.name      AS name,
               avg_rating  AS avg_rating
        ORDER BY avg_rating DESC, id
        LIMIT 1000;
        """,

        # Zählt Bestellungen der letzten 30 Tage pro Nutzer (nur Nutzer mit ≥1 Bestellung)
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE datetime(o.created_at) >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS orders_last_30d
        WHERE orders_last_30d > 0
        RETURN u.id            AS id,
               orders_last_30d AS orders_last_30d
        ORDER BY id
        LIMIT 500;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Selling: meistverkauftes Produkt → Empfehlungen basierend auf Käufen derselben Nutzer
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
        LIMIT 100;
        """,

        #Produkt-Co-Occurrence –  Top-25 Produktpaare, die gemeinsam
        #wenigstens einmal in derselben Bestellung auftauchten.
        #• Jede Bestellung zählt pro Paar nur einmal
        #• Reihenfolge der IDs wird festgelegt, damit (A,B) = (B,A)
        """
        MATCH (o:Order)-[:HAS_ITEM]->(:OrderItem)-[:REFERS_TO]->(p1:Product)
        MATCH (o)-[:HAS_ITEM]->(:OrderItem)-[:REFERS_TO]->(p2:Product)
        WHERE  p1.id < p2.id                          // Duplikate & Selbstpaare vermeiden

        WITH p1, p2, COUNT(DISTINCT o) AS co_orders   // ⇦ wie SQL: DISTINCT order_id
        RETURN p1.id  AS prodA,
            p2.id  AS prodB,
            co_orders
        ORDER BY co_orders DESC, prodA, prodB
        LIMIT 100;
        """,

        # Zwei-Hop-Produktempfehlung auf Basis von Top-Produkt: andere Käufe derselben Käuferschaft
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
        LIMIT 100;
        """,
    ],

        # ───────── CREATE ─────────
    Complexity.CREATE: [
        # Neue Adresse für zufälligen User – ID inkrementell, Relation HAS_ADDRESS
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

        # Neue Bestellung für User – mit ID und Zeitstempel
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

        # Neues CartItem für Produkt und Nutzer – mit Referenzrelationen
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

        # Neuer Produkt-View durch Nutzer – inkl. VIEWED- und VIEWED_PRODUCT-Relation
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
        # Lagerbestand erhöhen – erster Produkt-Knoten, neue Stock-Menge wird zurückgegeben
        """
        MATCH (p:Product)
        WITH p ORDER BY p.id LIMIT 1
        SET p.stock = coalesce(p.stock,0) + 1
        RETURN p.id AS product_id,
        p.stock AS new_stock;
        """,

        # Bewertung eines Reviews senken (min. 1) – neue Bewertung und ID zurückgeben
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

        # CartItem-Menge um +3 erhöhen – kleinstes CartItem wird verändert
        """
        MATCH (ci:CartItem)
        WITH ci ORDER BY ci.id       /* deterministisch: kleinste id */
        LIMIT 1
        SET   ci.quantity = coalesce(toInteger(ci.quantity), 0) + 3
        RETURN ci.id       AS cart_item_id,
            ci.quantity AS new_quantity;
        """,

        # Email eines Users modifizieren – durch Suffix .tmp
        """
        MATCH (u:User) WITH u LIMIT 1
        SET   u.email = u.email + '.tmp'
        RETURN u.id   AS user_id,
            u.email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        # Erstes Address-Objekt samt Relationen löschen (deterministisch)
        """
        MATCH (a:Address)
        WITH a ORDER BY a.id ASC
        LIMIT 1
        WITH a.id    AS deleted_address_id, a
        DETACH DELETE a
        RETURN deleted_address_id;
        """,

        # Erstes Review-Objekt samt Verknüpfungen löschen
        """
        MATCH (r:Review)
        WITH r ORDER BY r.id ASC
        LIMIT 1
        WITH r.id AS deleted_review_id, r
        DETACH DELETE r
        RETURN deleted_review_id;
        """,

        # Erstes CartItem löschen (inkl. Produktrelation)
        """
        MATCH (ci:CartItem)
        WITH ci ORDER BY ci.id ASC
        LIMIT 1
        WITH ci.id AS deleted_cart_item_id, ci
        DETACH DELETE ci
        RETURN deleted_cart_item_id;
        """,

        # Erstes ProductPurchase-Objekt löschen (inkl. evtl. zugehöriger Relationen)
        """
        MATCH (pp:ProductPurchase)
        WITH pp ORDER BY pp.id ASC
        LIMIT 1
        WITH pp.id AS deleted_purchase_id, pp
        DETACH DELETE pp
        RETURN deleted_purchase_id;
        """
    ]
}


# --- Neo4j optimiert --------------------------------------------------------
NEO_OPT_QUERIES = {

    # ───────── SIMPLE ─────────
    Complexity.SIMPLE: [
        # Alle Produkte mit Basisattributen – bis zu 10.000 Einträge
        """
        MATCH (p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 50000;
        """,

        # Alle Kategorien – alphabetisch sortiert nach ID
        """
        MATCH (c:Category)
        RETURN c.id   AS id,
               c.name AS name
        ORDER BY id
        LIMIT 5000;
        """,

        # Adressen mit allen gespeicherten Feldern – maximal 25
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
        LIMIT 1000;
        """,
    ],

    # ───────── MEDIUM ─────────
    Complexity.MEDIUM: [
        # Produkte, die mindestens einer Kategorie zugeordnet sind
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
        LIMIT 1000;
        """,

        # Produkte und Mengen aus den 20 neuesten Bestellungen – Relation CONTAINS
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
        LIMIT 500;
        """,

        # Neueste Reviews über REIEWED-Relationship – 5 Einträge mit Meta-Infos
        """
        MATCH (u:User)-[rev:REVIEWED]->(p:Product)
        RETURN rev.id        AS id,
               u.id          AS user_id,
               p.id          AS product_id,
               rev.rating    AS rating,
               rev.created_at AS created_at
        ORDER BY rev.created_at DESC, id DESC
        LIMIT 100;
        """,
    ],
    # ───────── COMPLEX ─────────
    Complexity.COMPLEX: [
        # Aggregierte Bestellsummen pro Bestellung (basierend auf Preis * Menge)
        """
        MATCH (o:Order)-[oi:CONTAINS]->(p:Product)
        WITH o, SUM(toInteger(oi.quantity) * toFloat(oi.price)) AS total
        RETURN o.id         AS id,
               o.created_at AS created_at,
               total        AS total
        ORDER BY id
        LIMIT 500;
        """,

        # Produkte mit durchschnittlicher Bewertung über 4 (basierend auf REVIEWED-Relation)
        """
        MATCH (p:Product)<-[rev:REVIEWED]-()
        WITH p, AVG(toFloat(rev.rating)) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id       AS id,
               p.name     AS name,
               avg_rating AS avg_rating
        ORDER BY avg_rating DESC, id
        LIMIT 1000;
        """,

        # Nutzer mit Bestellungen innerhalb der letzten 30 Tage (inkl. Zählung)
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE datetime(o.created_at) >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS orders_last_30d
        RETURN u.id            AS id,
               orders_last_30d AS orders_last_30d
        ORDER BY id
        LIMIT 500;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Selling: meistverkauftes Produkt → weitere Käufe durch gleiche Nutzer
        """
        // Schritt 1: best-seller bestimmen
        MATCH (:Order)-[:CONTAINS]->(top:Product)
        WITH top, count(*) AS freq ORDER BY freq DESC LIMIT 1

        // Schritt 2: alle weiteren Produkte derselben Käufer
        MATCH (top)<-[:CONTAINS]-(:Order)<-[:PLACED]-(u:User)
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
        WHERE p <> top
        WITH p, count(*) AS freq
        RETURN p.id AS rec_id, freq
        ORDER BY freq DESC, rec_id
        LIMIT 100;
        """,

        #Produkt-Co-Occurrence –  Top-25 Produktpaare, die gemeinsam
        #wenigstens einmal in derselben Bestellung auftauchten.
        #• Jede Bestellung zählt pro Paar nur einmal
        #• Reihenfolge der IDs wird festgelegt, damit (A,B) = (B,A)
        """
        MATCH (o:Order)-[:CONTAINS]->(p1:Product)
        MATCH (o)-[:CONTAINS]->(p2:Product)
        WHERE  p1.id < p2.id

        WITH p1, p2, COUNT(DISTINCT o) AS co_orders
        RETURN p1.id  AS prodA,
            p2.id  AS prodB,
            co_orders
        ORDER BY co_orders DESC, prodA, prodB
        LIMIT 100;
        """,

        # Zwei-Hop-Netz: Nutzer, die ein Top-Produkt gekauft haben + deren weitere Käufe
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
        LIMIT 100;
        """,
    ],

        # ───────── CREATE ─────────
    Complexity.CREATE: [
        # Erstellung einer neuen Adresse und Verknüpfung mit einem Nutzer (inkrementelle ID)
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

        # Erstellung einer neuen Bestellung mit Default-Werten und Nutzerverknüpfung
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

        # Erstellung einer neuen Warenkorb-Relation zwischen User und Produkt (inkl. Metadaten)
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

        # Erstellung einer neuen Produkt-View-Relation zwischen Nutzer und Produkt
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
        # Erhöhung des Lagerbestands um +1 bei erstem Produkt (deterministisch gewählt)
        """
        MATCH (p:Product) WITH p ORDER BY p.id LIMIT 1
        SET   p.stock = coalesce(p.stock,0) + 1
        RETURN p.id   AS product_id,
               p.stock AS new_stock;
        """,

        # Bewertung (REVIEWED-Relation) um −1 senken, Minimum: 1
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

        # Erhöhung der Menge im Warenkorb (HAS_IN_CART) um +3
        """
        MATCH ()-[c:HAS_IN_CART]-()
        WITH  c ORDER BY c.id LIMIT 1
        SET   c.quantity = coalesce(toInteger(c.quantity),0) + 3
        RETURN c.id       AS cart_rel_id,
               c.quantity AS new_quantity;
        """,

        # Anhängen eines Suffixes an die E-Mail-Adresse eines beliebigen Nutzers
        """
        MATCH (u:User) WITH u LIMIT 1
        SET   u.email = u.email + '.tmp'
        RETURN u.id   AS user_id,
               u.email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        # Löschen eines Address-Knotens (inkl. aller eingehenden/ausgehenden Kanten)
        """
        MATCH (a:Address)
        WITH  a ORDER BY a.id LIMIT 1
        WITH  a.id AS deleted_address_id, a
        DETACH DELETE a
        RETURN deleted_address_id;
        """,

        # Entfernen einer REVIEWED-Beziehung (inkl. ID-Rückgabe)
        """
        MATCH ()-[rev:REVIEWED]-()
        WITH  rev ORDER BY rev.id LIMIT 1
        WITH  rev.id AS deleted_review_rel_id, rev
        DELETE rev
        RETURN deleted_review_rel_id;
        """,

        # Entfernen einer HAS_IN_CART-Beziehung (Relation wird explizit mit ID referenziert)
        """
        MATCH ()-[c:HAS_IN_CART]-()
        WHERE c.id IS NOT NULL
        WITH  c ORDER BY c.id LIMIT 1
        WITH  c.id AS deleted_cart_rel_id, c
        DELETE c
        RETURN deleted_cart_rel_id;
        """,

        # Entfernen einer PURCHASED-Beziehung mit ID-Filterung
        """
        MATCH ()-[pur:PURCHASED]-()
        WHERE pur.id IS NOT NULL
        WITH  pur ORDER BY pur.id LIMIT 1
        WITH  pur.id AS deleted_purchase_rel_id, pur
        DELETE pur
        RETURN deleted_purchase_rel_id;
        """
    ]
}


###############################################################################
# Benchmark‑Runner -----------------------------------------------------------
###############################################################################

# Definierte parallele Auslastungsstufen (Anzahl gleichzeitiger Threads)
CONCURRENCY_LEVELS = [1, 3, 5, 10]

# Pause nach Warm-up-Durchlauf in Sekunden
WARMUP_SLEEP = 0.05 

# Spaltenüberschriften der CSV-Datei zur Ergebnisspeicherung
CSV_HEADER = [
    "db", "mode", "phase", "concurrency", "query_no", "repeat", "complexity",
    "duration_ms","qps", "avg_cpu", "avg_mem",
    "disk_mb", "statement", "result"
]


def _warmup_parallel(func, query: str, concurrency: int):
    """
    Führt eine definierte Anzahl von Warm-up-Durchläufen für eine Query parallel aus.

    Dies dient dem „Anwärmen“ der Datenbank und der JVM/Python VM,
    um Messverzerrungen durch Initialisierungsaufwand zu minimieren.
    Es findet keine Ergebnis- oder Zeitmessung statt.

    Parameter:
    - func: Funktion, die die Query ausführt (z. B. query_runner.run)
    - query: Die Cypher- oder SQL-Anweisung als String
    - concurrency: Anzahl der parallelen Threads für gleichzeitige Ausführung
    """
    if WARMUP_RUNS <= 0:
        logger.debug("Überspringe Warm-up, da WARMUP_RUNS <= 0.")
        return

    logger.debug(f"Starte Warm-up: {concurrency} Durchläufe mit concurrency={concurrency}")
    logger.debug(f"Warm-up Query: {query.replace(chr(10), ' ')}")

    # Ausführung mit ThreadPool für paralleles Warm-up
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(func, query) for _ in range(concurrency)]
        for ft in as_completed(futs):
            _ = ft.result()  # Fehler (z. B. Verbindungsprobleme) werden bewusst nicht unterdrückt

    logger.debug(f"Warm-up abgeschlossen. Warte {WARMUP_SLEEP} Sekunden...")
    time.sleep(WARMUP_SLEEP)


_CID_CACHE: dict[str, str] = {}

def _cid_of(name: str) -> str:
    """Liefert die vollständige Container-ID zu einem gegebenen Docker-Container-Namen.
    
    Die Ergebnisse werden in einem lokalen Cache (_CID_CACHE) gespeichert,
    um wiederholte `docker inspect`-Aufrufe zu vermeiden.
    """
    if name in _CID_CACHE:
        return _CID_CACHE[name]
    cid = subprocess.check_output(
        ["docker", "inspect", "--format", "{{.Id}}", name],
        text=True, stderr=subprocess.DEVNULL).strip()
    _CID_CACHE[name] = cid
    return cid

def _run_and_time(func, *a, **kw) -> float:
    """
    Führt die übergebene Funktion `func` mit den angegebenen Argumenten aus
    und misst die Laufzeit in Millisekunden.
    
    Die gemessene Dauer wird als Float-Wert zurückgegeben.
    """
    logger.debug(f"Starte Zeitmessung für Funktion {func.__name__} mit args={a}, kwargs={kw}")
    t0 = time.perf_counter_ns()
    func(*a, **kw)
    duration_ms = (time.perf_counter_ns() - t0) / 1_000_000
    logger.debug(f"Laufzeit für {func.__name__}: {duration_ms:.2f} ms")
    return duration_ms


def _log_csv(writer, *, phase, db, mode, conc, idx, repeat,
             comp, dur, qps, avg_cpu, avg_mem,
             disk_mb, stmt, res):
    """
    Schreibt eine vollständige Benchmark-Zeile in die CSV-Ausgabedatei und loggt
    gleichzeitig eine kompakte Zusammenfassung der Metriken.

    Parameter:
    - writer: CSV-Writer-Objekt
    - phase: Benchmark-Phase (z. B. warmup, measure)
    - db: Datenbank (postgresql oder neo4j)
    - mode: Ausführungsmodus (z. B. normal, optimized)
    - conc: Concurrency-Level (gleichzeitige Threads)
    - idx: Query-Nummer
    - repeat: Wiederholungsindex
    - comp: Komplexitätsstufe
    - dur: Gesamtdauer in Millisekunden
    - qps: Queries pro Sekunde
    - avg_cpu: durchschnittliche CPU-Auslastung in %
    - avg_mem: durchschnittlicher RAM-Verbrauch in MB
    - disk_mb: Festplattenverbrauch insgesamt in MB
    - stmt: ausgeführte Query
    - res: serialisiertes Query-Ergebnis
    """
    row = [
        db, mode, phase, conc, idx, repeat, comp.value,
        f"{dur:.2f}", f"{qps:.2f}", 
        f"{avg_cpu:.2f}", f"{avg_mem:.2f}",
        f"{disk_mb:.2f}",
        stmt.replace("\n", " "), json.dumps(res, ensure_ascii=False, default=str)
    ]
    writer.writerow(row)
    logger.info(
        f"[{db.upper()}] {phase} | Mode: {mode} | Query #{idx} | "
        f"Conc: {conc} | Time: {dur:.2f}ms | qps: {qps:.2f} | "
        f"AVG CPU: {avg_cpu:.2f}% |"
        f"AVG Mem: {avg_mem:.2f}MB |"
        f"Disk: {disk_mb:.2f}MB |"
    )


def _serialize_pg(cur, rows):
    """
    Serialisiert das Ergebnis eines PostgreSQL-Querys in ein standardisiertes Format.
    
    - Bei SELECT-Abfragen wird Anzahl und erste Zeile zurückgegeben.
    - Bei Datenänderungen wird nur die betroffene Zeilenanzahl (`rowcount`) erfasst.
    """
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
    """
    Serialisiert ein Neo4j-Ergebnisobjekt in ein standardisiertes Format.

    - Bei MATCH-Queries mit RETURN wird Anzahl und erster Datensatz ausgegeben.
    - Bei CREATE/SET/DELETE-Operationen werden Veränderungsmetriken übergeben.
    """
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

# Verbindungsparameter für den PostgreSQL-Zugriff (lokale Testumgebung)
PG_CONN_KWARGS = dict(host="localhost", port=5432,
                      user="postgres", password="pass", dbname="testdb")

# >>> globaler Connection-Pool; wird zentral in _pg_benchmark() erzeugt
PG_POOL: ThreadedConnectionPool | None = None


def _run_pg_query(query: str):
    """
    Führt exakt eine SQL-Query (SELECT, INSERT, UPDATE oder DELETE) gegen
    die PostgreSQL-Datenbank aus.

    Die Datenbankverbindung wird aus dem globalen ThreadPool entnommen und
    nach Ausführung wieder zurückgegeben.

    Ablauf:
    ➊ Verbindung aus dem Pool beziehen
    ➋ Autocommit aktivieren, um Transaktionen implizit zu committen
    ➌ Cursor öffnen, Query ausführen, Ergebnis ggf. abrufen
    ➍ Ergebnis serialisieren via _serialize_pg()
    ➎ Verbindung wieder dem Pool zurückgeben

    Fehler während der Ausführung werden geloggt und weitergereicht.
    """
    conn = PG_POOL.getconn()           # ➊ Connection leihen
    try:
        conn.autocommit = True         # ➋ explizites COMMIT entfällt
        with conn.cursor() as cur:
            logger.debug(f"[PG_QUERY] Start: {query.strip()}")
            cur.execute(query)         # ➌ Query ausführen
            rows = cur.fetchall() if cur.description else []  # Nur bei SELECT relevant
            result = _serialize_pg(cur, rows)  # ➍ Ergebnisstruktur vereinheitlichen
            logger.debug(f"[PG_QUERY] Ergebnis: {result}")
            return result
    except Exception as e:
        logger.error(f"[PG_QUERY] Fehler bei Query: {query.strip()} | Fehler: {e}")
        raise
    finally:
        PG_POOL.putconn(conn)          # ➎ Connection zurückgeben
        logger.debug("[PG_QUERY] Verbindung zurückgegeben an Pool")


def _pg_benchmark(queries: Dict[Complexity, List[str]],
                  container: str, mode: str, output: Path) -> None:
    """
    Führt systematische Performance-Benchmarks für PostgreSQL durch.

    Ablauf:
    - Initialisiert einmalig den Docker-Container und dessen CPU-Zähler (Taktfrequenz)
    - Erstellt einen ThreadedConnectionPool für parallele DB-Zugriffe
    - Iteriert über alle Queries (nach Komplexität) und Concurrency-Stufen
    - Führt jede Query im Warm-up (optional) und anschließend mehrfach im „steady“-Modus aus
    - Misst dabei Laufzeiten, Systemressourcen (CPU, RAM, IO) und schreibt CSV-Ausgabe

    Parameter:
    - queries: Dictionary aus Query-Komplexitätsstufe → Liste von SQL-Queries
    - container: Name des Docker-Containers, dessen Ressourcen gemessen werden
    - mode: z. B. "normal" oder "optimized" zur Unterscheidung verschiedener DB-Versionen
    - output: Pfad zur CSV-Zieldatei für Benchmark-Ergebnisse
    """
    logger.info("[PG_BENCHMARK] starte, container=%s", container)

    # ➊ Docker-ID und CPU-Taktfrequenz (für IO-Wait-Auswertung) holen
    cid = _cid_of(container)

    # Connection-Pool global initialisieren (für parallele Query-Ausführung)
    global PG_POOL
    PG_POOL = ThreadedConnectionPool(
        minconn=1,
        maxconn=max(CONCURRENCY_LEVELS),
        **PG_CONN_KWARGS
    )

    # Benchmark-Datei vorbereiten
    with open(output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(CSV_HEADER)  # Kopfzeile schreiben

        # Iterator über alle Kombinationen (Komplexität × Query)
        q_iter = [(c, q) for c, lst in queries.items() for q in lst]

        # Schleife über definierte Concurrency-Stufen (z. B. 1, 3, 5, 10 Threads)
        for conc in CONCURRENCY_LEVELS:
            pbar = tqdm(q_iter, desc=f"PostgreSQL {mode} x{conc}")
            for idx, (comp, query) in enumerate(pbar, 1):

                # ---------- WARM-UP ----------
                if WARMUP_RUNS > 0:
                    for wrep in range(1, WARMUP_RUNS + 1):
                        logger.debug(f"[PG_BENCHMARK] Warm-up {wrep}/{WARMUP_RUNS} | Query #{idx}")
                        warm_ms = _run_and_time(
                            _warmup_parallel,  # Führt Query mehrfach parallel aus
                            _run_pg_query,     # Funktionsreferenz: eine Query
                            query,             # SQL-Statement
                            conc               # Anzahl gleichzeitiger Threads
                        )
                        # Warm-up-Ergebnisse loggen (ohne detaillierte Systemdaten)
                        _log_csv(w, phase="warmup", db="postgres", mode=mode,
                                conc=conc, idx=idx, repeat=wrep, comp=comp,
                                dur=warm_ms, avg_cpu=math.nan,
                                avg_mem=math.nan, qps=math.nan,
                                disk_mb=get_docker_disk_mb(container),
                                stmt=query, res={"note": "warmup"})

                # ---------- STEADY-RUNS ----------
                for rep in range(1, REPETITIONS + 1):
                    start_stats = _read_cgroup_stats(cid)  # ➋

                    # Zeitmessung + parallele Ausführung der Query
                    t0 = time.perf_counter_ns()
                    with ThreadPoolExecutor(max_workers=conc) as ex:
                        futs = [ex.submit(_run_pg_query, query) for _ in range(conc)]
                        first_result = futs[0].result()  # erster Rückgabewert zur Logging-Ausgabe
                    duration_ms = (time.perf_counter_ns() - t0) / 1_000_000

                    end_stats = _read_cgroup_stats(cid)  # ➌
                    d = _delta(start_stats, end_stats)  # Delta-Werte berechnen

                    # Kennzahlen berechnen
                    qps = conc * 1000 / duration_ms
                    cpu_sec = d["cpu_usec"] / 1_000_000
                    avg_cpu = (cpu_sec / (duration_ms / 1000 * CPU_CORES)) * 100
                    avg_mem = (start_stats["mem_now"] + end_stats["mem_now"]) / 2 / 1024**2
                    disk_mb = get_docker_disk_mb(container)

                    # Ergebnisse in CSV schreiben und in Logdatei ausgeben
                    _log_csv(w, phase="steady", db="postgres", mode=mode,
                             conc=conc, idx=idx, repeat=rep, comp=comp,
                             dur=duration_ms, qps=qps,
                             avg_cpu=avg_cpu, avg_mem=avg_mem,
                             disk_mb=disk_mb,
                             stmt=query, res=first_result)

        logger.info(f"[PG_BENCHMARK] Benchmark abgeschlossen: {output.name}")

    # Pool schließen (Verbindungen zurückgeben und schließen)
    if PG_POOL:
        PG_POOL.closeall()


###############################################################################
# Neo4j helpers --------------------------------------------------------------
###############################################################################

NEO_BOLT_URI = "bolt://localhost:7687"
FETCH_SIZE = 100

def _open_session(drv):
    # fetch_size greift sofort, ohne extra Objekt
     return drv.session(fetch_size=FETCH_SIZE)

def _run_neo_query(query: str, driver) -> dict:
    """
    Führt eine Cypher-Query in Neo4j aus und serialisiert das Ergebnis.

    Ablauf:
    - Erstellt eine Session über den bereitgestellten Neo4j-Driver
    - Führt die übergebene Cypher-Anfrage aus
    - Wandelt das Ergebnis mithilfe von _serialize_neo() in ein standardisiertes Dictionary um
    - Gibt das serialisierte Ergebnis zurück

    Parameter:
    - query: Cypher-Abfrage (z. B. MATCH, CREATE etc.)
    - driver: Neo4j-Treiber-Instanz (bereitgestellt durch neo4j.GraphDatabase.driver)

    Rückgabe:
    - Dictionary mit Ergebnisstatistiken oder Datenvorschau, z. B.:
        - bei MATCH: Anzahl Zeilen, erste Zeile (`first`)
        - bei CREATE/DELETE: Zähler für betroffene Knoten/Beziehungen/Eigenschaften

    Fehlerbehandlung:
    - Alle Ausnahmen werden geloggt und erneut geworfen, damit sie im Benchmarking-Framework sichtbar bleiben
    """
    logger.debug("[NEO_QUERY] starte")
    try:
        rows, first = 0, None
        with _open_session(driver) as sess:          # einheitlicher Aufruf
            result = sess.run(query)
            for record in result:                    # streamend iterieren
                rows += 1
                if first is None:
                    first = record.data()
            summary = result.consume()

        return {
            "rows": rows,
            "first": first
        }

    except Exception:
        logger.exception("[NEO_QUERY] Fehler")
        raise


def _neo_benchmark(queries: Dict[Complexity, List[str]],
                   container: str, mode: str, output: Path) -> None:
    """
    Führt einen vollständigen Benchmark-Lauf für Neo4j aus.

    Vorgehen:
    - Erstellt einen Neo4j-Treiber auf Basis des BOLT-Protokolls
    - Iteriert über alle Queries und definierten Concurrency-Werte
    - Führt jede Query zunächst optional im "Warm-up" aus (mehrfach parallel, ohne Messwerte)
    - Führt danach mehrere Wiederholungen mit Messung durch (REPETITIONS)
    - Erfasst dabei die Metriken: Laufzeit, CPU, RAM, IO, Netz, Disk
    - Loggt jede Einzelmessung in eine CSV-Datei mit Standard-Header

    Parameter:
    - queries: Dictionary mit Komplexitätsstufen und zugehörigen Cypher-Queries
    - container: Name des Docker-Containers für den Benchmark
    - mode: Beschreibung des Modus (z. B. "normal", "optimized")
    - output: Pfad zur CSV-Zieldatei

    Besonderheiten:
    - Nutzt `_run_neo_query()` zur Ausführung einzelner Cypher-Befehle
    - Nutzt Container-Statistiken für RAM, CPU, Disk etc. per cgroup-Auslesung
    - Misst parallel mit ThreadPoolExecutor (abhängig von `CONCURRENCY_LEVELS`)
    - Gibt das erste Ergebnis zurück und speichert alle Daten zeilenweise in der CSV

    Die Funktion schließt Ressourcen (Treiber, Datei) sauber über `with`-Kontext.
    """
    logger.info("[NEO_BENCHMARK] starte, container=%s", container)
    cid = _cid_of(container)
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
                    for wrep in range(1, WARMUP_RUNS + 1):
                        logger.debug(f"[NEO_BENCHMARK] Warm-up {wrep}/{WARMUP_RUNS} | Query #{idx}")
                        warm_ms = _run_and_time(
                            _warmup_parallel,        # <- neue Signatur
                            partial(_run_neo_query, driver=driver),          # Query-Runner
                            query,                   # SQL-String
                            conc                     # concurrency
                        )
                        _log_csv(w, phase="warmup", db="neo4j", mode=mode,
                                conc=conc, idx=idx, repeat=wrep, comp=comp,
                                dur=warm_ms, avg_cpu=math.nan,
                                avg_mem=math.nan, qps=math.nan,
                                disk_mb=get_docker_disk_mb(container),
                                stmt=query, res={"note": "warmup"})

                # ---------- STEADY-RUNS ----------
                for rep in range(1, REPETITIONS+1):
                    s0 = _read_cgroup_stats(cid)
                    t0 = time.perf_counter_ns()
                    with ThreadPoolExecutor(max_workers=conc) as ex:
                        futs = [ex.submit(_run_neo_query, query, driver)
                                for _ in range(conc)]
                        first_result = futs[0].result()
                    duration_ms = (time.perf_counter_ns()-t0)/1_000_000
                    s1 = _read_cgroup_stats(cid)
                    d  = _delta(s0, s1)
                    qps          = conc * 1000 / duration_ms        # Concurrency / ms → / s
                    # ─── Kennzahlen berechnen ─────────────────────────────
                    cpu_sec     = d["cpu_usec"] / 1_000_000    # Δ CPU-Zeit
                    avg_cpu = (cpu_sec / (duration_ms / 1000 * CPU_CORES)) * 100
                    avg_mem  = (s0["mem_now"] + s1["mem_now"]) / 2 / 1024**2
                    disk_mb = get_docker_disk_mb(container)

                    _log_csv(w, phase="steady", db="neo4j", mode=mode,
                             conc=conc, idx=idx, repeat=rep, comp=comp,
                             dur=duration_ms, qps=qps,
                             avg_cpu=avg_cpu, avg_mem=avg_mem, disk_mb=disk_mb,
                             stmt=query, res=first_result)

    logger.info(f"[NEO_BENCHMARK] Benchmark abgeschlossen: {output.name}")

###############################################################################
# Öffentliche Funktionen -----------------------------------------------------
###############################################################################
"""
Diese Funktionen kapseln die jeweiligen Benchmark-Aufrufe für PostgreSQL und Neo4j
(in normaler und optimierter Variante). Sie dienen als Schnittstelle für das
CLI-Frontend und schreiben die Ergebnisse in die übergebene CSV-Datei
(innerhalb des "results/"-Verzeichnisses).

Parameter:
- output_csv: Dateiname für die CSV-Ergebnisdatei
"""

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
"""
Command-Line-Interface (CLI) zum Starten einzelner Benchmarks.

Parameter (via --args):
--variant      [str]   Pflichtfeld. Auswahl der Benchmark-Variante:
                       "pg_normal", "pg_opt", "neo_normal", "neo_opt"
--users        [int]   Pflichtfeld. Wird nur im CSV-Filenamen verwendet.
--round        [int]   Optionale Rundennummer für mehrfache Testsätze.
--repetitions  [int]   Anzahl der Wiederholungen pro Query (default: 3).
--warmups      [int]   Anzahl der Warm-up-Runden vor jeder Messung (default: 2).

Ablauf:
- Erzeugt das Zielverzeichnis "results/" falls nicht vorhanden
- Setzt globale Konstanten `WARMUP_RUNS` und `REPETITIONS`
- Generiert einen sprechenden CSV-Dateinamen mit Nutzeranzahl, Runde etc.
- Führt je nach ausgewählter Variante den zugehörigen Benchmark aus

Beispiel:
    python benchmark.py --variant pg_opt --users 10000 --round 3
"""

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