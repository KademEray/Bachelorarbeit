# file: export_neo_schema.py
import json, pathlib, collections
from neo4j import GraphDatabase

URI  = "bolt://localhost:7687"
AUTH = ("neo4j", "superpassword55")          # ggf. anpassen
OUT  = pathlib.Path("neo4j_schema_dump.json")

def run_single(tx, cypher):
    return [dict(r) for r in tx.run(cypher)]

def main():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        # Labels + Counts --------------------------------------------------
        node_counts = session.read_transaction(
            run_single,
            """MATCH (n) UNWIND labels(n) AS l
               RETURN l AS label, count(*) AS count
               ORDER BY label"""
        )

        # Relationship-Typen + Counts -------------------------------------
        rel_counts = session.read_transaction(
            run_single,
            """MATCH ()-[r]->() RETURN type(r) AS type,
                        count(*) AS count
               ORDER BY type"""
        )

        # Eigenschaften pro Node-Label ------------------------------------
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
        rel_props = session.read_transaction(
            run_single,
            """CALL db.schema.relTypeProperties()
               YIELD relType, propertyName, propertyTypes
               RETURN relType        AS type,
                      propertyName   AS prop,
                      propertyTypes  AS types
               ORDER BY type, prop"""
        )

    # handlich in ein Dictionary packen
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
