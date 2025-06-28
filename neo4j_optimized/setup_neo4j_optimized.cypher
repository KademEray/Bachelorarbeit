// ============================================================================
// Neo4j 5.x – Optimierter Graph  (Relationship-Properties, saubere Indexe)
// ============================================================================

/* ───────────────────────── 1. CONSTRAINTS ───────────────────────── */
// Eindeutige Constraints für alle Knoten-IDs
// Diese stellen sicher, dass jede Entität genau einmal vorkommt
// und vermeiden doppelte Daten beim Import
CREATE CONSTRAINT IF NOT EXISTS FOR (u:User)      REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product)   REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category)  REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order)     REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address)   REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:Shipment)  REQUIRE s.id IS UNIQUE;

/* ───────────────────────── 2. INDEXE ──────────────────────────────────── */
/* Relationship-Eigenschaften */
// Index für Bewertungen nach Sternebewertung – verbessert die Filterung nach Bewertungshöhe
CREATE INDEX IF NOT EXISTS FOR ()-[r:REVIEWED]-()      ON (r.rating);

// Ermöglicht effiziente Sortierung und Filterung nach Hinzufügedatum im Warenkorb
CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_IN_CART]-()   ON (r.added_at);

// Kombinierter Index für Menge und Preis eines Bestellitems – unterstützt Preisberechnungen und Analysen
CREATE INDEX IF NOT EXISTS FOR ()-[r:CONTAINS]-()      ON (r.quantity, r.price);

// Optimiert Zugriffe auf Produktansichten nach Zeitpunkten – z. B. für Zeitreihenanalysen
CREATE INDEX IF NOT EXISTS FOR ()-[r:VIEWED]-()        ON (r.viewed_at);

// Beschleunigt Abfragen zu Käufen nach Zeitstempeln (z. B. Umsatzanalysen)
CREATE INDEX IF NOT EXISTS FOR ()-[r:PURCHASED]-()     ON (r.purchased_at);

// Verbessert Zugriff auf Zahlungen nach Zahlungsdatum (z. B. für Monatsberichte)
CREATE INDEX IF NOT EXISTS FOR ()-[r:PAID_WITH]-()     ON (r.paid_at);

// Ermöglicht effiziente Filterung nach Versand- und Lieferdatum – z. B. für Logistikstatistiken
CREATE INDEX IF NOT EXISTS FOR ()-[r:SHIPPED]-()       ON (r.shipped_at, r.delivered_at);

/* Knoten-Eigenschaften */
// Eindeutiger E-Mail-Index für Benutzer – sinnvoll für Authentifizierung und Abfragen
CREATE INDEX IF NOT EXISTS FOR (u:User)     ON (u.email);

// Index auf Produktnamen – unterstützt Suchen und Filter im Katalog
CREATE INDEX IF NOT EXISTS FOR (p:Product)  ON (p.name);

// Kategorisierung erleichtert durch Index auf Kategoriebezeichnungen
CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name);

// Erlaubt zeitbasierte Sortierung und Filterung von Bestellungen
CREATE INDEX IF NOT EXISTS FOR (o:Order)    ON (o.created_at);

// Komposit-Index für Benutzer- und Zeitbasierte Order-Analysen (z. B. Nutzerverhalten)
CREATE INDEX IF NOT EXISTS FOR (o:Order)    ON (o.user_id, o.created_at);
