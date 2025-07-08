// ============================================================================
// Neo4j 5.x – Optimierter Graph  (Constraints, Indexe, Shortcut-Rels)
// ============================================================================

// ───────────────────────── 1. CONSTRAINTS ─────────────────────────
// Eindeutige IDs für alle wichtigen Knoten
CREATE CONSTRAINT IF NOT EXISTS FOR (u:User)      REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product)   REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category)  REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order)     REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address)   REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:Shipment)  REQUIRE s.id IS UNIQUE;

// ─────────────────────────── 2. INDEXE (KNOTEN) ──────────────────────────
// Schnelle Suche nach E-Mail
CREATE INDEX IF NOT EXISTS FOR (u:User)    ON (u.email);
// Suche/Filter nach Produktnamen
CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.name);
// Kategorie-Bezeichnungen
CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name);
// Zeitbasierte Queries auf Bestellungen
CREATE INDEX IF NOT EXISTS FOR (o:Order)   ON (o.created_at);
CREATE INDEX IF NOT EXISTS FOR (o:Order)   ON (o.user_id, o.created_at);
CREATE INDEX IF NOT EXISTS FOR (pp:ProductPurchase) ON (pp.purchased_at);
CREATE INDEX IF NOT EXISTS FOR (pv:ProductView)     ON (pv.viewed_at);

// ─────────────────────── 3. INDEXE (BEZIEHUNGEN) ─────────────────────────
// Review-Rating
CREATE INDEX IF NOT EXISTS FOR ()-[r:REVIEWED]-()    ON (r.rating);
// Warenkorb-Zeitpunkt
CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_IN_CART]-() ON (r.added_at);
// OrderItem (Menge & Preis)
CREATE INDEX IF NOT EXISTS FOR ()-[r:CONTAINS]-()    ON (r.quantity, r.price);
// Produkt-Views
CREATE INDEX IF NOT EXISTS FOR ()-[r:VIEWED]-()      ON (r.viewed_at);
// Käufe
CREATE INDEX IF NOT EXISTS FOR ()-[r:PURCHASED]-()   ON (r.purchased_at);
// Zahlungen
CREATE INDEX IF NOT EXISTS FOR ()-[r:PAID_WITH]-()   ON (r.paid_at);
// Shipments
CREATE INDEX IF NOT EXISTS FOR ()-[r:SHIPPED]-()     ON (r.shipped_at, r.delivered_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:BOUGHT]-() ON (r.purchased_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:SEEN]-()   ON (r.viewed_at);
CREATE INDEX IF NOT EXISTS FOR ()-[r:ORDERED]-() ON (r.quantity, r.price);
CREATE INDEX IF NOT EXISTS FOR ()-[r:ALSO_BOUGHT]-() ON (r.freq);

// ──────────────────── 4. SHORTCUT-BEZIEHUNGEN ───────────────────────────
// 4.1 Direkter Link User→Product bei Kauf (statt User→Order→OrderItem→Product)
MATCH (u:User)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
WITH u, p
MERGE (u)-[:BOUGHT]->(p);  // Optional ohne Timestamp, wenn bereits als REL optimiert

// 4.2 Direkter Link User→Product bei View
MATCH (u:User)-[:VIEWED]->(p:Product)
MERGE (u)-[:SEEN]->(p);

// 4.3 Direkter Link Order→Product (statt OrderItem)
MATCH (o:Order)-[:CONTAINS]->(p:Product)
MERGE (o)-[:ORDERED]->(p);

// ──────────────────── 5. MATERIALISIERTE AGGREGATBEZIEHUNGEN ─────────────────────────
// 5.1 Produkt-Popularität vorberechnen (z. B. für Top-N Queries)
MATCH (:Order)-[:CONTAINS]->(p:Product)
WITH p, COUNT(*) AS freq
MERGE (p)<-[r:POPULAR]-(:Meta)
  ON CREATE SET r.freq = freq;

// 5.2 Beliebteste Ko-Käufe materialisieren (vereinfacht später JOINs)
MATCH (u:User)-[:BOUGHT]->(p1:Product)
MATCH (u)-[:BOUGHT]->(p2:Product)
WHERE p1 <> p2
WITH p1, p2, COUNT(*) AS c
WHERE c > 10
MERGE (p1)-[r:ALSO_BOUGHT]->(p2)
  ON CREATE SET r.freq = c;