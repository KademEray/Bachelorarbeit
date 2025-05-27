
// === Knoten-Erstellung ===
CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT address_id IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT orderitem_id IF NOT EXISTS FOR (oi:OrderItem) REQUIRE oi.id IS UNIQUE;
CREATE CONSTRAINT payment_id IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE;
CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE;
CREATE CONSTRAINT cartitem_id IF NOT EXISTS FOR (ci:CartItem) REQUIRE ci.id IS UNIQUE;
CREATE CONSTRAINT shipment_id IF NOT EXISTS FOR (s:Shipment) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT productview_id IF NOT EXISTS FOR (pv:ProductView) REQUIRE pv.id IS UNIQUE;
CREATE CONSTRAINT productpurchase_id IF NOT EXISTS FOR (pp:ProductPurchase) REQUIRE pp.id IS UNIQUE;

// === Beziehungen ===
// Adressen
MATCH (u:User), (a:Address) WHERE a.user_id = u.id CREATE (u)-[:HAS_ADDRESS]->(a);

// Bestellungen
MATCH (u:User), (o:Order) WHERE o.user_id = u.id CREATE (u)-[:PLACED]->(o);

// Order Items
MATCH (o:Order), (oi:OrderItem) WHERE oi.order_id = o.id CREATE (o)-[:HAS_ITEM]->(oi);
MATCH (p:Product), (oi:OrderItem) WHERE oi.product_id = p.id CREATE (oi)-[:REFERS_TO]->(p);

// Zahlungen
MATCH (o:Order), (pay:Payment) WHERE pay.order_id = o.id CREATE (o)-[:PAID_WITH]->(pay);

// Kategorien
MATCH (p:Product), (c:Category), (pc:ProductCategory)
WHERE pc.product_id = p.id AND pc.category_id = c.id
CREATE (p)-[:IN_CATEGORY]->(c);

// Bewertungen
MATCH (u:User), (p:Product), (r:Review)
WHERE r.user_id = u.id AND r.product_id = p.id
CREATE (u)-[:WROTE]->(r)-[:REVIEWS]->(p);

// Warenkorb
MATCH (u:User), (p:Product), (ci:CartItem)
WHERE ci.user_id = u.id AND ci.product_id = p.id
CREATE (u)-[:HAS_IN_CART]->(ci)-[:CART_PRODUCT]->(p);

// Versand
MATCH (o:Order), (s:Shipment) WHERE s.order_id = o.id CREATE (o)-[:HAS_SHIPMENT]->(s);

// Wunschliste
MATCH (u:User), (p:Product)
WHERE EXISTS {
    MATCH (:Wishlist {user_id: u.id, product_id: p.id})
}
CREATE (u)-[:WISHLISTED]->(p);

// Produktansichten
MATCH (u:User), (p:Product), (pv:ProductView)
WHERE pv.user_id = u.id AND pv.product_id = p.id
CREATE (u)-[:VIEWED]->(pv)-[:VIEWED_PRODUCT]->(p);

// Produktkäufe
MATCH (u:User), (p:Product), (pp:ProductPurchase)
WHERE pp.user_id = u.id AND pp.product_id = p.id
CREATE (u)-[:PURCHASED]->(pp)-[:PURCHASED_PRODUCT]->(p);
