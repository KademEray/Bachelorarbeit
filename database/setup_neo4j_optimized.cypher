
// === Neo4j Optimized v2 ===
// - Join‑Tabellen entfernt (Nodes gelöscht)
// - Wishlist-Datum als Property
// - Payments richtig mit User verbunden
// - Indexe auf häufige Relationship-Properties

/* ---------- CONSTRAINTS (wie gehabt) ---------- */
CREATE CONSTRAINT user_id IF NOT EXISTS     FOR (u:User)     REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS  FOR (p:Product)  REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS    FOR (o:Order)    REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT address_id IF NOT EXISTS  FOR (a:Address)  REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT shipment_id IF NOT EXISTS FOR (s:Shipment) REQUIRE s.id IS UNIQUE;

/* ---------- RELATIONSHIPS MIT PROPERTIES ---------- */

// Produkt‑Kategorien
MATCH (p:Product), (c:Category)
WHERE EXISTS { MATCH (:ProductCategory {product_id:p.id, category_id:c.id}) }
MERGE (p)-[:IN_CATEGORY]->(c);

// Bestellung enthält Produkt
MATCH (o:Order)<-[:HAS_ITEM]-(oi:OrderItem)-[:REFERS_TO]->(p:Product)
MERGE (o)-[:CONTAINS {quantity:oi.quantity, price:oi.price}]->(p);

// Reviews
MATCH (u:User)<-[:WROTE]-(r:Review)-[:REVIEWS]->(p:Product)
MERGE (u)-[:REVIEWED {rating:r.rating, comment:r.comment, created_at:r.created_at}]->(p);

// Warenkorb
MATCH (u:User)<-[:HAS_IN_CART]-(ci:CartItem)-[:CART_PRODUCT]->(p:Product)
MERGE (u)-[:HAS_IN_CART {quantity:ci.quantity, added_at:ci.added_at}]->(p);

// Views
MATCH (u:User)<-[:VIEWED]-(pv:ProductView)-[:VIEWED_PRODUCT]->(p:Product)
MERGE (u)-[:VIEWED {viewed_at:pv.viewed_at}]->(p);

// Purchases
MATCH (u:User)<-[:PURCHASED]-(pp:ProductPurchase)-[:PURCHASED_PRODUCT]->(p:Product)
MERGE (u)-[:PURCHASED {purchased_at:pp.purchased_at}]->(p);

// Wishlist (Datum)
MATCH (u:User)-[:WISHLISTED]->(p:Product)
SET  (u)-[:WISHLISTED {created_at: datetime()}]->(p);  // assumes wishlists table has created_at; else replace

/* ---------- Correct Payment relationship ---------- */
// Entferne Dummy‑User‑Links
MATCH (o:Order)-[r:PAID_WITH]->(dummy:User {id:-1})
DELETE r;

// Erzeuge echte Beziehung (Order –> User)
MATCH (o:Order)<-[:PLACED]-(u:User)
MATCH (pay:Payment {order_id:o.id})
MERGE (o)-[:PAID_WITH {
  method: pay.payment_method,
  status: pay.payment_status,
  paid_at: pay.paid_at
}]->(u);

/* ---------- Versandinfos ---------- */
MATCH (o:Order)<-[:HAS_SHIPMENT]-(s:Shipment)
MERGE (o)-[:HAS_SHIPMENT {
  tracking_number:s.tracking_number,
  shipped_at:s.shipped_at,
  delivered_at:s.delivered_at,
  carrier:s.carrier
}]->(s);

/* ---------- CLEANUP: Lösche überflüssige Join‑Nodes ---------- */
MATCH (n:ProductCategory|OrderItem|CartItem|Review|ProductView|ProductPurchase)
DETACH DELETE n;

/* ---------- INDEXE FÜR REL‑PROPS ---------- */
CREATE INDEX rev_rating IF NOT EXISTS FOR ()-[r:REVIEWED]-()      ON (r.rating);
CREATE INDEX cart_added IF NOT EXISTS FOR ()-[r:HAS_IN_CART]-()   ON (r.added_at);
CREATE INDEX contains_qty IF NOT EXISTS FOR ()-[r:CONTAINS]-()    ON (r.quantity);
