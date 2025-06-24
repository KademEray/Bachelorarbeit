// ============================================================================
// Neo4j 5.x – Optimierter Graph  (Relationship-Properties, saubere Indexe)
// ============================================================================

/* ───────────────────────── 1. CONSTRAINTS ───────────────────────── */
CREATE CONSTRAINT IF NOT EXISTS FOR (u:User)     REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product)  REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order)    REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address)  REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:Shipment) REQUIRE s.id IS UNIQUE;

/* ───────────────────────── 2. PRODUCT ↔ CATEGORY ───────────────────────── */
/*  Join-Knoten zuerst holen, dann auflösen → kein Kreuzprodukt mehr          */
MATCH (pc:ProductCategory)
MATCH (p:Product  {id: pc.product_id})
MATCH (c:Category {id: pc.category_id})
MERGE (p)-[:BELONGS_TO]->(c);

/* ───────────────────────── 3. ORDERS → CONTAINS ─────────────────────────── */
MATCH (oi:OrderItem)
MATCH (o:Order   {id: oi.order_id})
MATCH (p:Product {id: oi.product_id})
MERGE (o)-[cont:CONTAINS]->(p)
  SET cont.quantity = toInteger(oi.quantity),
      cont.price    = toFloat(oi.price);

/* ───────────────────────── 4. REVIEWS ───────────────────────────────────── */
MATCH (r:Review)
MATCH (u:User    {id: r.user_id})
MATCH (p:Product {id: r.product_id})
MERGE (u)-[rev:REVIEWED]->(p)
  SET rev.rating     = toInteger(r.rating),
      rev.comment    = r.comment,
      rev.created_at = datetime(r.created_at);

/* ───────────────────────── 5. CART ITEMS ───────────────────────────────── */
MATCH (ci:CartItem)
MATCH (u:User    {id: ci.user_id})
MATCH (p:Product {id: ci.product_id})
MERGE (u)-[c:HAS_IN_CART]->(p)
  SET c.quantity = toInteger(ci.quantity),
      c.added_at = datetime(ci.added_at);

/* ───────────────────────── 6. PRODUCT VIEWS ────────────────────────────── */
MATCH (pv:ProductView)
MATCH (u:User    {id: pv.user_id})
MATCH (p:Product {id: pv.product_id})
MERGE (u)-[v:VIEWED]->(p)
  SET v.viewed_at = datetime(pv.viewed_at);

/* ───────────────────────── 7. PURCHASES ────────────────────────────────── */
MATCH (pp:ProductPurchase)
MATCH (u:User    {id: pp.user_id})
MATCH (p:Product {id: pp.product_id})
MERGE (u)-[pur:PURCHASED]->(p)
  SET pur.purchased_at = datetime(pp.purchased_at);

/* ───────────────────────── 8. WISHLIST ─────────────────────────────────── */
MATCH (wsrc:Wishlist)
MATCH (u:User    {id: wsrc.user_id})
MATCH (p:Product {id: wsrc.product_id})
MERGE (u)-[w:WISHLISTED]->(p)
  ON CREATE SET w.created_at = coalesce(wsrc.created_at, datetime());

/* ───────────────────────── 9. PAYMENT RELATIONSHIP ─────────────────────── */
/* Dummy-Links entfernen                                                     */
MATCH (o:Order)-[r:PAID_WITH]->(dummy:User {id:-1})
DELETE r;
/* Korrekte Beziehung anlegen                                                */
MATCH (pay:Payment)
MATCH (o:Order {id: pay.order_id})
MATCH (u:User  {id: o.user_id})
MERGE (o)-[pw:PAID_WITH]->(u)
  SET pw.method  = pay.payment_method,
      pw.status  = pay.payment_status,
      pw.paid_at = CASE WHEN pay.paid_at IS NOT NULL
                        THEN datetime(pay.paid_at) END;

/* ───────────────────────── 10. SHIPMENTS ───────────────────────────────── */
MATCH (s:Shipment)
MATCH (o:Order {id: s.order_id})
MERGE (o)-[ship:SHIPPED]->(s)
  SET ship.tracking_number = s.tracking_number,
      ship.shipped_at     = datetime(s.shipped_at),
      ship.delivered_at   = CASE WHEN s.delivered_at IS NOT NULL
                                 THEN datetime(s.delivered_at) END,
      ship.carrier        = s.carrier;

/* ───────────────────────── 11. CLEANUP ─────────────────────────────────── */
MATCH (n:ProductCategory) DETACH DELETE n;
MATCH (n:OrderItem)       DETACH DELETE n;
MATCH (n:CartItem)        DETACH DELETE n;
MATCH (n:Review)          DETACH DELETE n;
MATCH (n:ProductView)     DETACH DELETE n;
MATCH (n:ProductPurchase) DETACH DELETE n;

/* ───────────────────────── 12. INDEXE ──────────────────────────────────── */
/* Relationship-Eigenschaften */
CREATE INDEX IF NOT EXISTS FOR ()-[r:REVIEWED]-()   ON (r.rating);
CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_IN_CART]-()ON (r.added_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:CONTAINS]-()   ON (r.quantity);
CREATE INDEX IF NOT EXISTS FOR ()-[r:VIEWED]-()     ON (r.viewed_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:PURCHASED]-()  ON (r.purchased_at);

/* Knoten-Eigenschaften */
CREATE INDEX IF NOT EXISTS FOR (u:User)     ON (u.email);
CREATE INDEX IF NOT EXISTS FOR (p:Product)  ON (p.name);
CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name);
CREATE INDEX IF NOT EXISTS FOR (o:Order)    ON (o.created_at);

/* Einfache (separate) Mehrspalten-Indexe */
CREATE INDEX IF NOT EXISTS FOR (u:User)   ON (u.id);
CREATE INDEX IF NOT EXISTS FOR (o:Order)  ON (o.user_id, o.created_at);
