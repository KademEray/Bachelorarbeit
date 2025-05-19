"""
generate_from_product_dataset.py  – v3
--------------------------------------
• Liest product_dataset.csv (ISO-8859-1)
• Zufällige Produktauswahl (nicht die ersten Zeilen)
• categoryName kann mehrere, komma­getrennte Kategorien enthalten
• description = "<Titel>. <Faker-Satz>"
"""

import json, argparse, random
from pathlib import Path
from datetime import datetime
import pandas as pd
from faker import Faker

# ------------------ Hauptfunktion ------------------
def build_dataset(num_users: int, data_dir: Path, out_dir: Path):
    faker = Faker("de_DE")
    out_dir.mkdir(exist_ok=True)
    fname = out_dir / f"ecommerce_{num_users}.json"

    # 1) CSV einlesen  (Latin-1, damit Umlaute korrekt)
    df = pd.read_csv(
        data_dir / "product_dataset.csv",
        usecols=["title", "price", "categoryName"],
        encoding="ISO-8859-1"
    ).dropna(subset=["title", "price", "categoryName"])

    # Zufällige Produktauswahl
    products_raw = df.sample(n=min(num_users * 2, len(df)))

    # 2) Kategorien sammeln
    cat_name_to_id, categories = {}, []

    def get_cat_id(raw_cat: str) -> int:
        cat = raw_cat.strip()
        if not cat:
            return None
        if cat not in cat_name_to_id:
            cid = len(categories) + 1
            cat_name_to_id[cat] = cid
            categories.append({"id": cid, "name": cat})
        return cat_name_to_id[cat]

    # 3) Produkte & product_categories
    products, product_categories = [], []
    for idx, row in products_raw.reset_index(drop=True).iterrows():
        pid = idx + 1
        title = str(row.title)[:255]
        products.append({
            "id": pid,
            "name": title,
            "description": None,                 
            "price": float(row.price),
            "stock": random.randint(1, 100),
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds")
        })


        # Mehrere Kategorien (getrimmt + dedupliziert)
        cats = {c.strip() for c in str(row.categoryName).split(",") if c.strip()}
        for cat in cats:
            cid = get_cat_id(cat)
            product_categories.append({"product_id": pid, "category_id": cid})

    # 4) Users & Adressen
    users, addresses = [], []
    for uid in range(1, num_users + 1):
        users.append({
            "id": uid,
            "name": faker.name(),
            "email": faker.email(),
            "created_at": faker.date_time_this_year().isoformat()
        })
        addresses.append({
            "id": uid,
            "user_id": uid,
            "street": faker.street_address(),
            "city": faker.city(),
            "zip": faker.postcode(),
            "country": "Deutschland",
            "is_primary": random.random() < 0.4
        })

    # 5) JSON speichern  (UTF-8, Umlaute OK)
    dataset = {
        "users": users,
        "addresses": addresses,
        "categories": categories,
        "products": products,
        "product_categories": product_categories
    }
    fname.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ Datensatz geschrieben: {fname.resolve()}")

# ------------------ CLI ------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--users", type=int, default=10,
                   help="Anzahl Nutzer (Default 10)")
    p.add_argument("--data", default="data",
                   help="Ordner mit product_dataset.csv")
    p.add_argument("--out", default="output",
                   help="Ausgabeordner")
    args = p.parse_args()
    build_dataset(args.users, Path(args.data), Path(args.out))
