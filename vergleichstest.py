import subprocess, sys, time, logging
from pathlib import Path
from enum import Enum
from contextlib import contextmanager
from typing import Dict, List
import json
import psycopg2
from neo4j import GraphDatabase
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


@contextmanager
def timeit(msg: str):
    logging.info("⚙️  %s", msg)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        logging.info("✅ %s – %.1fs", msg, time.perf_counter() - t0)

class Complexity(Enum):
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"

PG_QUERIES: Dict[Complexity, List[str]] = {

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
############################################################
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


####################################################################
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


"""
Hilfsfunktionen zur Ausführung und Dokumentation von Benchmark-Queries:

1. flatten_queries(qdict)
   – Wandelt ein nach Komplexität gruppiertes Dictionary von SQL- oder Cypher-Queries in eine flache, sortierte Liste um.
   – Die feste Reihenfolge (SIMPLE → … → DELETE) ermöglicht vergleichbare Abläufe in Benchmarks.
   – Nicht vorhandene Komplexitätsstufen werden dabei übersprungen.

2. exec_pg_queries(conn, queries)
   – Führt eine Liste von SQL-Statements sequenziell auf einer bestehenden PostgreSQL-Verbindung aus.
   – Ergebnisse werden gesammelt und – falls vorhanden – spaltenweise in Dictionaries überführt, um JSON-kompatibel zu sein.
   – Rückgabewert ist eine Liste von Ergebnislisten (je Query eine).

3. exec_neo_queries(driver, queries)
   – Führt eine Liste von Cypher-Statements mit einem übergebenen Neo4j-Treiber aus.
   – Verwendet `sess.run(...).data()` zur direkten Umwandlung in Listen von Dictionaries.
   – Rückgabewert ist analog zu `exec_pg_queries`.

4. dump_results(variant, rows_per_query, out_dir)
   – Schreibt die Ergebnisse aller ausgeführten Queries zeilenweise in eine `.txt`-Datei im UTF-8-Format.
   – Format: Variantentitel als Überschrift, dann je Query-Index die zugehörige Ergebnisliste als JSON-Block.
   – Dient der optionalen Nachvollziehbarkeit und dem Export von Query-Resultaten für Debugging oder Analysezwecke.
"""

def flatten_queries(qdict: Dict[Complexity, List[str]]) -> List[str]:
    """Erhält die natürliche Reihenfolge SIMPLE→…→DELETE."""
    order = [Complexity.SIMPLE, Complexity.MEDIUM, Complexity.COMPLEX,
             Complexity.VERY_COMPLEX, Complexity.CREATE,
             Complexity.UPDATE, Complexity.DELETE]
    flat = []
    for comp in order:
        flat.extend(qdict.get(comp, []))
    return flat


def exec_pg_queries(conn, queries: List[str]) -> List[list]:
    """Führt alle SQL-Statements aus und liefert eine Liste mit Result-Rows."""
    results = []
    with conn, conn.cursor() as cur:
        for q in queries:
            cur.execute(q)
            rows = cur.fetchall() if cur.description else []
            # Spalten in Dicts verwandeln, damit JSON-serialisierbar
            if cur.description:
                cols = [c[0] for c in cur.description]
                rows = [dict(zip(cols, r)) for r in rows]
            results.append(rows)
    return results


def exec_neo_queries(driver, queries: List[str]) -> List[list]:
    """Dasselbe für Neo4j-Cypher."""
    results = []
    with driver.session() as sess:
        for q in queries:
            rows = sess.run(q).data()
            results.append(rows)
    return results


def dump_results(variant: str, rows_per_query: List[list], out_dir: Path):
    out_dir.mkdir(exist_ok=True)
    outfile = out_dir / f"{variant}.txt"
    with outfile.open("w", encoding="utf-8") as f:
        f.write(f"{variant}:\n")
        for idx, rows in enumerate(rows_per_query, 1):
            f.write(f"{idx}:\n")
            json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
            f.write("\n\n")
    logging.info("📄 Ergebnisse für %s → %s", variant, outfile)


def run_once(n_users: int) -> None:
    """
    Funktion: run_once(n_users)

    Diese Funktion führt einen vollständigen End-to-End-Testlauf für eine bestimmte Anzahl an Nutzern (`n_users`) aus.
    Ziel ist es, strukturierte Vergleichsdaten für verschiedene Datenbankvarianten zu erzeugen und abzuspeichern.

    Ablauf:

    1. Datengenerierung:
    – Startet das Python-Skript `generate_data.py`, um eine synthetische JSON-Datenbasis zu erzeugen.
    – Führt anschließend `export_sql_cypher.py` aus, um die Daten in SQL- und Cypher-kompatible Formate zu exportieren.

    2. PostgreSQL (normal):
    – Erzeugt Image und Container mit Basisstruktur.
    – Führt strukturierte Inserts mit `insert_normal_postgresql_data.py` durch.
    – Verbindet sich zur PostgreSQL-Instanz, führt alle definierten Queries aus (`PG_QUERIES`),
        und speichert die Ergebnisse strukturiert im Verzeichnis `cmp_results`.
    – Container und Image werden nach Abschluss gelöscht.

    3. Neo4j (normal):
    – Analog zu PostgreSQL: Aufbau des Containers, Laden der Struktur, Einfügen der Daten.
    – Cypher-Queries (`NEO_NORMAL_QUERIES`) werden mit einem Bolt-Treiber ausgeführt.
    – Die Ergebnisse werden ebenfalls in `cmp_results` abgelegt.

    4. Neo4j (optimiert):
    – Getrenntes Setup mit optimierter Modellierung.
    – Führt eine zweite Query-Menge (`NEO_OPT_QUERIES`) aus und speichert Ergebnisse.

    5. Clean-up:
    – Unabhängig vom Ausgang des Skripts sorgt der `finally`-Block für ein sauberes Entfernen aller Docker-Komponenten,
        um Konflikte in Folgeläufen zu vermeiden.

    Hinweis: Alle Teilbereiche werden per Zeitmessung (`timeit`) geloggt, um Performance-Metriken bei Bedarf analysieren zu können.
    """
    try:
        # ───────────────────────────────────────────────────────── Datengenerierung
        print(f"\n=== Starte Daten-Generation für {n_users} User ===")
        with timeit(f"generate_data.py ({n_users})"):
           subprocess.run(
               [sys.executable, "-u", str(GEN), "--users", str(n_users)],
               check=True
           )

        print("✔️  JSON-Export …")
        with timeit("export_sql_cypher.py"):
           subprocess.run([sys.executable, "-u", str(EXPORT)], check=True)

        # ───────────────────────────────────────────────────── PostgreSQL (normal)
        print("\n=== PostgreSQL-normal: Container, Struktur & Inserts ===")
        build_normal_postgres_image("./postgresql_normal")
        start_normal_postgres_container()
        apply_normal_sql_structure("./postgresql_normal/setup_postgres_normal.sql")

        with timeit("insert_normal_postgresql_data.py"):
            subprocess.run(
                [sys.executable, "-u", str(INSERT_POSTGRESQL_NORMAL),
                 "--file-id", str(n_users), "--json-dir", "./output"],
                check=True
            )

        # ── Queries ausführen
        print("→ Führe SQL-Queries aus …")
        pg_conn = psycopg2.connect(
           host="localhost",
            port=5432,
            user="postgres",
            password="pass",
            dbname="testdb"
        )

        pg_queries_flat = flatten_queries(PG_QUERIES)
        logging.debug("PostgreSQL-Query-Liste:\n%s",
                      "\n".join(f"{i+1:02d} {q.splitlines()[0][:60]}…"  # erste Zeile
                                for i, q in enumerate(pg_queries_flat)))

        pg_results = exec_pg_queries(pg_conn, pg_queries_flat)
        dump_results("pg_normal", pg_results, BASE_DIR / "cmp_results")
        pg_conn.close()

        stop_normal_postgres_container()
        delete_normal_postgres_image()
        print("✔️  PostgreSQL-normal abgeschlossen.")

        # ──────────────────────────────────────────────────────── PostgreSQL (optimiert)
        print("\n=== PostgreSQL-optimiert: Container, Struktur & Inserts ===")
        build_optimized_postgres_image("./postgresql_optimized")
        start_optimized_postgres_container()
        apply_optimized_sql_structure("./postgresql_optimized/setup_postgres_optimized.sql")
        with timeit("insert_optimized_postgresql_data.py"):
            subprocess.run(
                [sys.executable, "-u", str(INSERT_POSTGRESQL_OPTIMIZED),
                 "--file-id", str(n_users), "--json-dir", "./output"],
                check=True
            )
        print("→ Führe SQL-Queries (pg_opt) aus …")
        pg_opt_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="pass",
            dbname="testdb"
        )
        pg_opt_queries_flat = flatten_queries(PG_QUERIES)
        logging.debug("PostgreSQL-optimiert-Query-Liste:\n%s",
                      "\n".join(f"{i+1:02d} {q.splitlines()[0][:60]}…"
                                for i, q in enumerate(pg_opt_queries_flat)))
        pg_opt_results = exec_pg_queries(pg_opt_conn, pg_opt_queries_flat)
        dump_results("pg_opt", pg_opt_results, BASE_DIR / "cmp_results")
        pg_opt_conn.close()
        stop_optimized_postgres_container()
        delete_optimized_postgres_image()
        print("✔️  PostgreSQL-optimiert abgeschlossen.")

        # ─────────────────────────────────────────────────────── Neo4j (normal)
        print("\n=== Neo4j-normal: Container, Struktur & Inserts ===")
        build_normal_neo4j_image("./neo4j_normal")
        start_normal_neo4j_container()
        apply_normal_cypher_structure("./neo4j_normal/setup_neo4j_normal.cypher")

        with timeit("insert_normal_neo4j_data.py"):
           subprocess.run(
               [sys.executable, "-u", str(INSERT_NEO4J_NORMAL),
                "--file-id", str(n_users), "--json-dir", "./output"],
               check=True
           )

        print("→ Führe Cypher-Queries (neo_normal) aus …")
        neo_driver = GraphDatabase.driver(
           "bolt://localhost:7687", auth=("neo4j","superpassword55")
        )
        neo_queries_flat = flatten_queries(NEO_NORMAL_QUERIES)
        logging.debug("Neo_normal-Query-Liste:\n%s",
                     "\n".join(f"{i+1:02d} {q.splitlines()[0][:60]}…"
                               for i, q in enumerate(neo_queries_flat)))

        neo_results = exec_neo_queries(neo_driver, neo_queries_flat)
        dump_results("neo_normal", neo_results, BASE_DIR / "cmp_results")
        neo_driver.close()

        stop_normal_neo4j_container()
        delete_normal_neo4j_image()
        print("✔️  Neo4j-normal abgeschlossen.")

        # ───────────────────────────────────────────────────── Neo4j (optimiert)
        print("\n=== Neo4j-optimiert: Container, Struktur & Inserts ===")
        build_optimized_neo4j_image("./neo4j_optimized")
        start_optimized_neo4j_container()
        apply_optimized_cypher_structure("./neo4j_optimized/setup_neo4j_optimized.cypher")

        with timeit("insert_optimized_neo4j_data.py"):
           subprocess.run(
               [sys.executable, "-u", str(INSERT_NEO4J_OPTIMIZED),
                "--file-id", str(n_users), "--json-dir", "./output"],
               check=True
           )

        print("→ Führe Cypher-Queries (neo_opt) aus …")
        neo_opt_driver = GraphDatabase.driver(
           "bolt://localhost:7687", auth=("neo4j","superpassword55")
        )
        neo_opt_queries_flat = flatten_queries(NEO_OPT_QUERIES)
        logging.debug("Neo_opt-Query-Liste:\n%s",
                     "\n".join(f"{i+1:02d} {q.splitlines()[0][:60]}…"
                               for i, q in enumerate(neo_opt_queries_flat)))

        neo_opt_results = exec_neo_queries(neo_opt_driver, neo_opt_queries_flat)
        dump_results("neo_opt", neo_opt_results, BASE_DIR / "cmp_results")
        neo_opt_driver.close()

        stop_optimized_neo4j_container()
        delete_optimized_neo4j_image()
        print("✔️  Neo4j-optimiert abgeschlossen.")

        print("\n✅ Durchlauf für", n_users, "User komplett.\n")

    finally:
        # Fallback-Clean-up (falls irgendwo vorher Exception)
        stop_normal_postgres_container()
        delete_normal_postgres_image()
        stop_optimized_postgres_container()
        delete_optimized_postgres_image()
        stop_normal_neo4j_container()
        delete_normal_neo4j_image()
        stop_optimized_neo4j_container()
        delete_optimized_neo4j_image()
        print("🧹 Säubere Docker-Ressourcen …")


def main():
    run_once(n_users=10)

if __name__ == "__main__":
    main()
