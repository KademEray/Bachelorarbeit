# file: export_neo_schema.py
# Dieses Skript verbindet sich mit einer laufenden Neo4j-Instanz und exportiert
# das aktuelle Schema der Datenbank (Labels, Beziehungstypen, Constraints etc.)
# als JSON-Datei.

import json, pathlib, collections
from neo4j import GraphDatabase

URI  = "bolt://localhost:7687"               # Adresse der Neo4j-Datenbank
AUTH = ("neo4j", "superpassword55")          # Zugangsdaten (anpassen falls nötig)
OUT  = pathlib.Path("neo4j_schema_dump.json") # Zieldatei für den Schema-Export

def run_single(tx, cypher):
    """
    Führt eine einzelne Cypher-Abfrage aus und gibt alle Ergebnisse
    als Liste von Dictionaries zurück.
    """
    return [dict(r) for r in tx.run(cypher)]


def main():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        # Labels + Counts --------------------------------------------------
        # Ermittelt alle Node-Labels und wie viele Knoten es je Label gibt.
        node_counts = session.read_transaction(
            run_single,
            """MATCH (n) UNWIND labels(n) AS l
               RETURN l AS label, count(*) AS count
               ORDER BY label"""
        )

        # Relationship-Typen + Counts -------------------------------------
        # Listet alle Beziehungstypen und wie häufig sie vorkommen.
        rel_counts = session.read_transaction(
            run_single,
            """MATCH ()-[r]->() RETURN type(r) AS type,
                        count(*) AS count
               ORDER BY type"""
        )

        # Eigenschaften pro Node-Label ------------------------------------
        # Liefert für jedes Node-Label die Eigenschaften + deren Typen.
        node_props = session.read_transaction(
            run_single,
            """CALL db.schema.nodeTypeProperties()
               YIELD nodeLabels, propertyName, propertyTypes
               RETURN nodeLabels[0] AS label,
                      propertyName    AS prop,
                      propertyTypes   AS types
               ORDER BY label, prop"""
        )

        # Eigenschaften pro Relationship-Type -----------------------------
        # Liefert für jeden Beziehungstyp die Eigenschaften + deren Typen.
        rel_props = session.read_transaction(
            run_single,
            """CALL db.schema.relTypeProperties()
               YIELD relType, propertyName, propertyTypes
               RETURN relType        AS type,
                      propertyName   AS prop,
                      propertyTypes  AS types
               ORDER BY type, prop"""
        )

    # Ergebnisdaten in ein Dictionary verpacken und als JSON-Datei schreiben
    agg = {
        "node_labels"        : node_counts,
        "relationship_types" : rel_counts,
        "node_properties"    : node_props,
        "rel_properties"     : rel_props,
    }
    OUT.write_text(json.dumps(agg, indent=2, ensure_ascii=False))
    print(f"✅  Schema-Dump geschrieben nach:  {OUT.resolve()}")

if __name__ == "__main__":
    main()