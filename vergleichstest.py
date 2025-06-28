# main.py
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
INSERT_NEO4J_NORMAL  = BASE_DIR / "neo4j_normal" / "insert_normal_neo4j_data.py"
INSERT_NEO4J_OPTIMIZED  = BASE_DIR / "neo4j_optimized" / "insert_optimized_neo4j_data.py"
BENCH   = BASE_DIR / "performance_benchmark.py"


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
        """
        SELECT id, name, price, stock, created_at, updated_at
          FROM products
         ORDER BY id
         LIMIT 10;
        """,
        "SELECT id, name FROM categories ORDER BY id LIMIT 10;",
        "SELECT * FROM addresses ORDER BY id LIMIT 10;",
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
         LIMIT 10;
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
         LIMIT 10;
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
         LIMIT 10;
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
         ORDER BY avg_rating DESC, p.id LIMIT 10;
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
         LIMIT 10;
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
         LIMIT 10;
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
         LIMIT 10;
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
         LIMIT 10;
        """,
    ],

    # ───────── CREATE ─────────
    Complexity.CREATE: [
        # 1) neue Adresse  → liefert address_id
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

        # 2) neue Bestellung  → order_id
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

        # 3) Cart-Item  → cart_item_id
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

        # 4) Produkt-View  → product_view_id
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
        # 1) Lagerbestand erhöhen  → liefert id + neuer Stock-Wert
        """
        UPDATE products
        SET stock = stock + 1
        WHERE id = (SELECT id FROM products LIMIT 1)
        RETURNING id AS product_id, stock AS new_stock;
        """,

        # 2) Rating um 1 senken (min. 1)  → id + neues Rating
        """
        UPDATE reviews
        SET rating = GREATEST(rating - 1, 1)
        WHERE id = (SELECT id FROM reviews LIMIT 1)
        RETURNING id AS review_id, rating AS new_rating;
        """,

        # 3) Cart-Menge +3  → id + neue Quantity
        """
        UPDATE cart_items
        SET quantity = quantity + 3
        WHERE id = (SELECT id FROM cart_items LIMIT 1)
        RETURNING id AS cart_item_id, quantity AS new_quantity;
        """,

        # 4) E-Mail anpassen  → id + neue Mail
        """
        UPDATE users
        SET email = email || '.tmp'
        WHERE id = (SELECT id FROM users LIMIT 1)
        RETURNING id AS user_id, email AS new_email;
        """
    ],

    # ───────── DELETE ─────────
    Complexity.DELETE: [
        # 1) Adresse löschen  → gibt gelöschte id zurück
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
    ]

}
############################################################
NEO_NORMAL_QUERIES = {

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
        LIMIT 10;
        """,

        """
        MATCH (c:Category)
        RETURN c.id   AS id,
               c.name AS name
        ORDER BY id
        LIMIT 10;
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
        LIMIT 10;
        """,
    ],

    Complexity.MEDIUM: [
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
        LIMIT 10;
        """,

        """
        MATCH (o:Order)
        WITH o ORDER BY o.created_at DESC, o.id DESC LIMIT 10
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
        LIMIT 10;
        """,

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

    Complexity.COMPLEX: [
        """
        MATCH (o:Order)-[:HAS_ITEM]->(oi:OrderItem)
        WITH o, SUM(oi.quantity * oi.price) AS total
        RETURN o.id         AS id,
               o.created_at AS created_at,
               total        AS total
        ORDER BY id
        LIMIT 10;
        """,

        """
        MATCH (p:Product)<-[:REVIEWS]-(r:Review)
        WITH p, AVG(r.rating) AS avg_rating
        WHERE avg_rating > 4
        RETURN p.id        AS id,
               p.name      AS name,
               avg_rating  AS avg_rating
        ORDER BY avg_rating DESC, id
        LIMIT 10;
        """,

        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE datetime(o.created_at) >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS orders_last_30d
        WHERE orders_last_30d > 0
        RETURN u.id            AS id,
               orders_last_30d AS orders_last_30d
        ORDER BY id
        LIMIT 10;
        """,
    ],

    Complexity.VERY_COMPLEX: [
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
        LIMIT 10;
        """,

        """
        MATCH (u:User)-[:VIEWED]->(:ProductView)-[:VIEWED_PRODUCT]->(p:Product)
        MATCH (u)-[:PLACED]->(:Order)-[:HAS_ITEM]->(:OrderItem {product_id: p.id})
        RETURN DISTINCT p.id   AS id,
                        p.name AS name
        ORDER BY id
        LIMIT 10;
        """,

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
        LIMIT 10;
        """,
    ],

    # ───────── CREATE ─────────
    Complexity.CREATE: [
        # 1) neue Adresse  → liefert address_id
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

        # 2) neue Bestellung  → order_id
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

        # 3) Cart-Item (Relationship)  → product_id + neue Menge
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

        # 4) Produkt-View  → product_view_id
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


####################################################################
NEO_OPT_QUERIES = {

    # ───────── SIMPLE ─────────
    Complexity.SIMPLE: [
        # 1) Produkte
        """
        MATCH (p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at
        ORDER BY id
        LIMIT 10;
        """,

        # 2) Kategorien
        """
        MATCH (c:Category)
        RETURN c.id   AS id,
               c.name AS name
        ORDER BY id
        LIMIT 10;
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
        LIMIT 10;
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
        LIMIT 10;
        """,

        # 20 Positionen aus den letzten Bestellungen
        """
        MATCH (o:Order)
        WITH o ORDER BY o.created_at DESC, o.id DESC LIMIT 10
        MATCH (o)-[oi:CONTAINS]->(p:Product)
        RETURN p.id         AS id,
               p.name       AS name,
               p.price      AS price,
               p.stock      AS stock,
               p.created_at AS created_at,
               p.updated_at AS updated_at,
               oi.quantity  AS quantity
        ORDER BY o.created_at DESC, o.id DESC, id
        LIMIT 10;
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
        LIMIT 10;
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
        LIMIT 10;
        """,

        # Bestellungen der letzten 30 Tage pro User
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        WHERE datetime(o.created_at) >= datetime() - duration({days:30})
        WITH u, COUNT(o) AS orders_last_30d
        RETURN u.id            AS id,
               orders_last_30d AS orders_last_30d
        ORDER BY id
        LIMIT 10;
        """,
    ],

    # ───────── VERY COMPLEX ─────────
    Complexity.VERY_COMPLEX: [
        # Cross-Sell (Top-Produkt → weitere Käufe)
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
        LIMIT 10;
        """,

        # View ∩ Purchase
        """
        MATCH (u:User)-[:VIEWED]->(p:Product)
        MATCH (u)-[:PLACED]->(:Order)-[:CONTAINS]->(p)
        RETURN DISTINCT p.id   AS id,
                        p.name AS name
        ORDER BY id
        LIMIT 10;
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
        LIMIT 10;
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
    try:
        # ───────────────────────────────────────────────────────── Datengenerierung
        #print(f"\n=== Starte Daten-Generation für {n_users} User ===")
        #with timeit(f"generate_data.py ({n_users})"):
        #    subprocess.run(
        #        [sys.executable, "-u", str(GEN), "--users", str(n_users)],
        #        check=True
        #    )

        # print("✔️  JSON-Export …")
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

        # ─────────────────────────────────────────────────────── Neo4j (normal)
        # print("\n=== Neo4j-normal: Container, Struktur & Inserts ===")
        # build_normal_neo4j_image("./neo4j_normal")
        # start_normal_neo4j_container()
        # apply_normal_cypher_structure("./neo4j_normal/setup_neo4j_normal.cypher")

        # with timeit("insert_normal_neo4j_data.py"):
        #    subprocess.run(
        #        [sys.executable, "-u", str(INSERT_NEO4J_NORMAL),
        #         "--file-id", str(n_users), "--json-dir", "./output"],
        #        check=True
        #    )

        # print("→ Führe Cypher-Queries (neo_normal) aus …")
        # neo_driver = GraphDatabase.driver(
        #    "bolt://localhost:7687", auth=("neo4j","superpassword55")
        # )
        # neo_queries_flat = flatten_queries(NEO_NORMAL_QUERIES)
        # logging.debug("Neo_normal-Query-Liste:\n%s",
        #              "\n".join(f"{i+1:02d} {q.splitlines()[0][:60]}…"
        #                        for i, q in enumerate(neo_queries_flat)))

        # neo_results = exec_neo_queries(neo_driver, neo_queries_flat)
        # dump_results("neo_normal", neo_results, BASE_DIR / "cmp_results")
        # neo_driver.close()

        # stop_normal_neo4j_container()
        # delete_normal_neo4j_image()
        # print("✔️  Neo4j-normal abgeschlossen.")

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
        stop_normal_neo4j_container()
        delete_normal_neo4j_image()
        stop_optimized_neo4j_container()
        delete_optimized_neo4j_image()


def main():
    run_once(n_users=1000)

if __name__ == "__main__":
    main()
