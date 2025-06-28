// ============================================================================
// Neo4j 5.x – Optimierter Graph  (Relationship-Properties, saubere Indexe)
// ============================================================================

/* ───────────────────────── 1. CONSTRAINTS ───────────────────────── */
CREATE CONSTRAINT IF NOT EXISTS FOR (u:User)      REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product)   REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category)  REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order)     REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address)   REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:Shipment)  REQUIRE s.id IS UNIQUE;

/* ───────────────────────── 2. INDEXE ──────────────────────────────────── */
/* Relationship-Eigenschaften */
CREATE INDEX IF NOT EXISTS FOR ()-[r:REVIEWED]-()      ON (r.rating);
CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_IN_CART]-()   ON (r.added_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:CONTAINS]-()      ON (r.quantity, r.price);
CREATE INDEX IF NOT EXISTS FOR ()-[r:VIEWED]-()        ON (r.viewed_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:PURCHASED]-()     ON (r.purchased_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:PAID_WITH]-()     ON (r.paid_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:SHIPPED]-()       ON (r.shipped_at, r.delivered_at);

/* Knoten-Eigenschaften */
CREATE INDEX IF NOT EXISTS FOR (u:User)     ON (u.email);
CREATE INDEX IF NOT EXISTS FOR (p:Product)  ON (p.name);
CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name);
CREATE INDEX IF NOT EXISTS FOR (o:Order)    ON (o.created_at);
CREATE INDEX IF NOT EXISTS FOR (o:Order)    ON (o.user_id, o.created_at);