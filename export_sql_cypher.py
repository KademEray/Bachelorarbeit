# Skript: export_sql_cypher.py
# Zweck:
# Dieses Skript konvertiert statische Daten aus einer generierten JSON-Datei in SQL- und Cypher-Skripte.
# Es werden vier separate Ausgabedateien erstellt:
# - static_products_data.sql (für PostgreSQL normal + optimized)
# - static_products_data_normal.cypher (für Neo4j normal)
# - static_products_data_optimized.cypher (für Neo4j optimized)
# Die Pfade können über CLI angepasst werden.

import argparse
import json
from pathlib import Path
from tqdm import tqdm

def escape_sql_value(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

def escape_cypher_string(value):
    return str(value).replace("'", "\\'")

def export_static_tables_to_sql_and_cypher(json_path: Path,
                                           sql_normal_path: Path,
                                           sql_optimized_path: Path,
                                           cypher_normal_path: Path,
                                           cypher_optimized_path: Path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    static_tables = ["categories", "products", "product_categories"]

    # Verzeichnisse anlegen
    for path in [sql_normal_path, sql_optimized_path, cypher_normal_path, cypher_optimized_path]:
        path.parent.mkdir(parents=True, exist_ok=True)

    # SQL normal & optimized (identisch)
    for path in [sql_normal_path, sql_optimized_path]:
        with open(path, "w", encoding="utf-8") as sql_file:
            for table in tqdm(static_tables, desc=f"SQL Export to {path.name}", ncols=80):
                rows = data.get(table, [])
                if not rows:
                    continue
                for row in rows:
                    columns = ", ".join(row.keys())
                    values = ", ".join(escape_sql_value(v) for v in row.values())
                    sql_file.write(f"INSERT INTO {table} ({columns}) VALUES ({values});\n")

    # Cypher normal & optimized
    for variant, cypher_path in [("normal", cypher_normal_path), ("optimized", cypher_optimized_path)]:
        with open(cypher_path, "w", encoding="utf-8") as cypher_file:
            for table in tqdm(static_tables, desc=f"Cypher Export [{variant}]", ncols=80):
                rows = data.get(table, [])
                if not rows:
                    continue
                for row in rows:
                    if table == "categories":
                        name = escape_cypher_string(row['name'])
                        if variant == "normal":
                            cypher_file.write(
                                f"MERGE (c:Category {{id: {row['id']}}}) SET c.name = '{name}';\n"
                            )
                        else:
                            cypher_file.write(
                                f"MERGE (c:Category {{id: {row['id']}, name: '{name}'}});\n"
                            )
                    elif table == "products":
                        name = escape_cypher_string(row['name'])
                        cypher_file.write(
                            f"MERGE (p:Product {{id: {row['id']}}}) SET "
                            f"p.name = '{name}', p.price = {row['price']}, p.stock = {row['stock']}, "
                            f"p.created_at = datetime('{row['created_at']}'), "
                            f"p.updated_at = datetime('{row['updated_at']}');\n"
                        )
                    elif table == "product_categories":
                        if variant == "normal":
                            cypher_file.write(
                                f"MATCH (p:Product {{id: {row['product_id']}}}), "
                                f"(c:Category {{id: {row['category_id']}}}) "
                                f"MERGE (p)-[:BELONGS_TO]->(c);\n"
                            )
                        else:
                            cypher_file.write(
                                f"MATCH (p:Product {{id: {row['product_id']}}}), "
                                f"(c:Category {{id: {row['category_id']}}}) "
                                f"MERGE (c)-[:CONTAINS]->(p);\n"
                            )

    print("\n✅ Export abgeschlossen.")

# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exportiert statische Produktdaten in SQL- und Cypher-Dateien für normale und optimierte Datenbankstrukturen.")
    parser.add_argument("--input", required=True, type=str, help="Pfad zur JSON-Datei (z. B. users_0.json)")

    parser.add_argument("--sql-normal", type=str, default="postgresql_normal/static_products_data.sql",
                        help="Pfad zur SQL-Datei für PostgreSQL normal")
    parser.add_argument("--sql-optimized", type=str, default="postgresql_optimized/static_products_data.sql",
                        help="Pfad zur SQL-Datei für PostgreSQL optimized")
    parser.add_argument("--cypher-normal", type=str, default="neo4j_normal/static_products_data_normal.cypher",
                        help="Pfad zur Cypher-Datei für Neo4j normal")
    parser.add_argument("--cypher-optimized", type=str, default="neo4j_optimized/static_products_data_optimized.cypher",
                        help="Pfad zur Cypher-Datei für Neo4j optimized")

    args = parser.parse_args()

    export_static_tables_to_sql_and_cypher(
        Path(args.input),
        Path(args.sql_normal),
        Path(args.sql_optimized),
        Path(args.cypher_normal),
        Path(args.cypher_optimized)
    )
