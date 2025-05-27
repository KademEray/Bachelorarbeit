import json, argparse, random
from pathlib import Path
from datetime import datetime, timedelta
from random import randint
import pandas as pd
from faker import Faker
from tqdm import tqdm

ORDER_STATI = ["NEW", "PAID", "SHIPPED", "COMPLETED", "CANCELLED"]
PAY_METHODS = ["card", "paypal", "invoice"]
CARRIERS = ["DHL", "Hermes", "DPD", "GLS"]

faker = Faker("de_DE")

def random_date_this_year():
    now = datetime.now()
    start_of_year = datetime(now.year, 1, 1)
    max_seconds = int((now - start_of_year).total_seconds())
    offset = randint(0, max_seconds)
    return (start_of_year + timedelta(seconds=offset)).isoformat(timespec="seconds")

def random_date_between(start: str, end: str) -> str:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.now() if isinstance(end, str) and end == "now" else datetime.fromisoformat(end)
    delta = end_dt - start_dt
    offset = timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    return (start_dt + offset).isoformat(timespec="seconds")

def open_stream_files(base_dir: Path, tables: list[str]):
    files = {}
    for name in tables:
        path = base_dir / f"{name}.jsonl"
        files[name] = open(path, "w", encoding="utf-8")
    return files

def close_stream_files(files: dict):
    for f in files.values():
        f.close()

def stream_write(file, obj):
    file.write(json.dumps(obj, ensure_ascii=False) + "\n")

def build_dataset(num_users: int, data_dir: Path, out_dir: Path):
    out_dir.mkdir(exist_ok=True)
    stream_files = open_stream_files(out_dir, [
        "users", "addresses", "categories", "products", "product_categories",
        "orders", "order_items", "payments", "shipments",
        "reviews", "cart_items", "wishlists",
        "product_views", "product_purchases"
    ])

    df = pd.read_csv(
        data_dir / "product_dataset.csv",
        usecols=["title", "price", "categoryName", "reviews"],
        encoding="ISO-8859-1",
        converters={"reviews": lambda x: int(str(x).replace(",", "").strip()) if x else 0}
    ).dropna()

    max_reviews = df["reviews"].max()
    df["review_weight"] = df["reviews"].apply(lambda r: 0.05 + 0.85 * (r / max_reviews if max_reviews else 0))
    sample_size = min(num_users * 2, len(df))
    products_raw = df.sample(n=sample_size).reset_index(drop=True)

    cat_name_to_id, categories = {}, []
    def get_cat_id(name: str) -> int:
        name = name.strip()
        if name not in cat_name_to_id:
            cid = len(categories) + 1
            cat_name_to_id[name] = cid
            categories.append({"id": cid, "name": name})
        return cat_name_to_id[name]

    products, product_categories, prod_weights = [], [], {}
    for idx, row in products_raw.iterrows():
        pid = idx + 1
        title = str(row.title)[:255]
        products.append({
            "id": pid, "name": title, "description": None,
            "price": float(row.price), "stock": random.randint(1, 100),
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds")
        })
        prod_weights[pid] = float(products_raw.loc[idx, "review_weight"])
        for cat in {c.strip() for c in row.categoryName.split(",") if c.strip()}:
            cid = get_cat_id(cat)
            product_categories.append({"product_id": pid, "category_id": cid})

    for c in categories:
        stream_write(stream_files["categories"], c)
    for p in products:
        stream_write(stream_files["products"], p)
    for pc in product_categories:
        stream_write(stream_files["product_categories"], pc)

    for uid in tqdm(range(1, num_users + 1), desc="Generiere Nutzer"):
        stream_write(stream_files["users"], {
            "id": uid, "name": faker.name(), "email": faker.email(),
            "created_at": random_date_this_year()
        })

        num_addr = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        primary_set = False
        for i in range(num_addr):
            stream_write(stream_files["addresses"], {
                "id": uid * 10 + i, "user_id": uid,
                "street": faker.street_address(), "city": faker.city(),
                "zip": faker.postcode(), "country": "Deutschland",
                "is_primary": not primary_set or (i == num_addr - 1 and not primary_set)
            })
            primary_set = primary_set or True

        for _ in range(random.randint(1, 3)):
            oid = uid * 10 + _
            o_ts = random_date_this_year()
            status = random.choice(ORDER_STATI)
            items = random.sample(products, k=random.randint(1, 4))
            total = 0.0
            for prod in items:
                qty = random.randint(1, 3)
                stream_write(stream_files["order_items"], {
                    "id": oid * 10 + prod["id"],
                    "order_id": oid, "product_id": prod["id"],
                    "quantity": qty, "price": prod["price"]
                })
                stream_write(stream_files["product_purchases"], {
                    "id": oid * 10 + prod["id"],
                    "user_id": uid, "product_id": prod["id"], "purchased_at": o_ts
                })
                total += qty * prod["price"]
                if random.random() < prod_weights[prod["id"]]:
                    stream_write(stream_files["reviews"], {
                        "id": oid * 10 + prod["id"],
                        "user_id": uid, "product_id": prod["id"],
                        "rating": random.randint(1, 5), "comment": None,
                        "created_at": o_ts
                    })

            stream_write(stream_files["orders"], {
                "id": oid, "user_id": uid, "status": status,
                "total": round(total, 2), "created_at": o_ts, "updated_at": o_ts
            })

            stream_write(stream_files["payments"], {
                "id": oid, "order_id": oid,
                "payment_method": random.choice(PAY_METHODS),
                "payment_status": "paid" if status != "CANCELLED" else "failed",
                "paid_at": o_ts
            })

            if status in {"SHIPPED", "COMPLETED"}:
                ship_ts = random_date_between(o_ts, "now")
                stream_write(stream_files["shipments"], {
                    "id": oid,
                    "order_id": oid,
                    "tracking_number": faker.bothify("??########"),
                    "shipped_at": ship_ts,
                    "delivered_at": random_date_between(ship_ts, "now"),
                    "carrier": random.choice(CARRIERS)
                })

        for prod in random.sample(products, k=random.randint(0, 3)):
            stream_write(stream_files["cart_items"], {
                "id": uid * 10 + prod["id"],
                "user_id": uid, "product_id": prod["id"],
                "quantity": random.randint(1, 2),
                "added_at": random_date_this_year()
            })

        for prod in random.sample(products, k=random.randint(1, 5)):
            stream_write(stream_files["wishlists"], {
                "user_id": uid, "product_id": prod["id"],
                "created_at": random_date_this_year()
            })

        for _ in range(random.randint(1, 10)):
            prod = random.choice(products)
            stream_write(stream_files["product_views"], {
                "id": uid * 100 + prod["id"],
                "user_id": uid, "product_id": prod["id"],
                "viewed_at": random_date_this_year()
            })

    close_stream_files(stream_files)
    print(f"\n✓ Streaming-Datensatz gespeichert unter: {out_dir.resolve()}")

# CLI
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, required=True, help="Anzahl User (z. B. 1000)")
    ap.add_argument("--data", default="product_data", help="Ordner mit product_dataset.csv")
    ap.add_argument("--out", default="output_streamed", help="Ausgabe-Ordner")
    args = ap.parse_args()
    build_dataset(args.users, Path(args.data), Path(args.out))
