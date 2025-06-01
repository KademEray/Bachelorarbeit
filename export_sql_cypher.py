# Skript: export_sql_cypher.py
# Zweck:
# Dieses Skript konvertiert statische Daten aus einer generierten JSON-Datei in SQL- und Cypher-Skripte.
# Es werden vier separate Ausgabedateien erstellt:
# - static_products_data.sql (für PostgreSQL normal + optimized)
# - static_products_data_normal.cypher (für Neo4j normal)
# - static_products_data_optimized.cypher (für Neo4j optimized)

import argparse, json, random, shutil
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

# === Helferfunktionen für JSON-Export ===
def stream_write(file, obj):
    file.write(json.dumps(obj, ensure_ascii=False) + "\n")

def open_stream_files(base_dir: Path, tables: list[str]):
    files = {}
    for name in tables:
        path = base_dir / f"{name}.jsonl"
        files[name] = open(path, "w", encoding="utf-8")
    return files

def close_stream_files(files: dict):
    for f in files.values():
        f.close()

def beautify_json_file(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def merge_jsonl_to_single_file(stream_dir: Path, final_path: Path):
    tables = ["categories", "products", "product_categories"]
    final_path.parent.mkdir(parents=True, exist_ok=True)

    with open(final_path, "w", encoding="utf-8") as out:
        out.write("{\n")
        for i, table in enumerate(tables):
            out.write(f'"{table}": [\n')
            first = True
            with open(stream_dir / f"{table}.jsonl", "r", encoding="utf-8") as f:
                for line in f:
                    if not first:
                        out.write(",\n")
                    out.write(line.strip())
                    first = False
            out.write("\n]")
            if i < len(tables) - 1:
                out.write(",\n")
        out.write("\n}\n")

    shutil.rmtree(stream_dir)
    beautify_json_file(final_path)
    print(f"✓ Datei erstellt unter: {final_path.resolve()}")

def generate_static_json(product_csv: Path, tmp_dir: Path, output_file: Path):
    tmp_dir.mkdir(exist_ok=True)
    stream_files = open_stream_files(tmp_dir, ["categories", "products", "product_categories"])

    df = pd.read_csv(
        product_csv,
        usecols=["title", "price", "categoryName", "reviews"],
        encoding="utf-8",
        converters={"reviews": lambda x: int(str(x).replace(",", "").strip()) if x else 0}
    ).dropna()

    products_raw = df.reset_index(drop=True)
    cat_name_to_id, categories = {}, []

    def get_cat_id(name: str) -> int:
        name = name.strip()
        if name not in cat_name_to_id:
            cid = len(categories) + 1
            cat_name_to_id[name] = cid
            categories.append({"id": cid, "name": name})
        return cat_name_to_id[name]

    products, product_categories = [], []
    for idx, row in products_raw.iterrows():
        pid = idx + 1
        now = datetime.now()
        ten_years_ago = now - timedelta(days=365 * 10)
        created_dt = ten_years_ago + timedelta(seconds=random.randint(0, int((now - ten_years_ago).total_seconds())))
        created_at = created_dt.isoformat(timespec="seconds")
        updated_dt = created_dt + timedelta(seconds=random.randint(0, int((now - created_dt).total_seconds())))
        updated_at = updated_dt.isoformat(timespec="seconds")

        products.append({
            "id": pid,
            "name": str(row.title)[:255],
            "description": None,
            "price": float(row.price),
            "stock": random.randint(1, 100),
            "created_at": created_at,
            "updated_at": updated_at
        })

        for cat in {c.strip() for c in row.categoryName.split(",") if c.strip()}:
            cid = get_cat_id(cat)
            product_categories.append({"product_id": pid, "category_id": cid})

    for c in categories:
        stream_write(stream_files["categories"], c)
    for p in products:
        stream_write(stream_files["products"], p)
    for pc in product_categories:
        stream_write(stream_files["product_categories"], pc)

    close_stream_files(stream_files)
    merge_jsonl_to_single_file(tmp_dir, output_file)

# === Exporter ===
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

    for path in [sql_normal_path, sql_optimized_path, cypher_normal_path, cypher_optimized_path]:
        path.parent.mkdir(parents=True, exist_ok=True)

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
                            cypher_file.write(f"MERGE (c:Category {{id: {row['id']}}}) SET c.name = '{name}';\n")
                        else:
                            cypher_file.write(f"MERGE (c:Category {{id: {row['id']}, name: '{name}'}});\n")
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

# === CLI-Wrapper ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generiert und exportiert statische Produktdaten als SQL- und Cypher-Dateien.")
    parser.add_argument("--product-csv", type=str, default="product_data/product_dataset.csv",
                        help="Pfad zur CSV-Datei mit Produktdaten")
    parser.add_argument("--tmp", type=str, default="tmp_static_gen",
                        help="Temporärer Ordner für Stream-Dateien")
    parser.add_argument("--static-json", type=str, default="static.json",
                        help="Zielpfad für temporäre JSON-Datei")

    parser.add_argument("--sql-normal", type=str, default="postgresql_normal/static_products_data.sql")
    parser.add_argument("--sql-optimized", type=str, default="postgresql_optimized/static_products_data.sql")
    parser.add_argument("--cypher-normal", type=str, default="neo4j_normal/static_products_data_normal.cypher")
    parser.add_argument("--cypher-optimized", type=str, default="neo4j_optimized/static_products_data_optimized.cypher")
    args = parser.parse_args()

    # 1. Generiere static.json
    generate_static_json(Path(args.product_csv), Path(args.tmp), Path(args.static_json))

    # 2. Exportiere nach SQL & Cypher
    export_static_tables_to_sql_and_cypher(
        Path(args.static_json),
        Path(args.sql_normal),
        Path(args.sql_optimized),
        Path(args.cypher_normal),
        Path(args.cypher_optimized)
    )

    # 3. Lösche static.json
    try:
        Path(args.static_json).unlink()
        print(f"🧹 static.json gelöscht.")
    except Exception as e:
        print(f"⚠ Fehler beim Löschen von static.json: {e}")
