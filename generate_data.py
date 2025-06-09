import json, argparse, random
from pathlib import Path
from datetime import datetime, timedelta
from random import randint
import pandas as pd
from faker import Faker
from tqdm import tqdm
import shutil


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


def build_dataset(num_users: int, data_dir: Path, out_dir: Path, final_dir: Path):
    out_dir.mkdir(exist_ok=True)
    stream_files = open_stream_files(out_dir, [
        "users", "addresses",
        "orders", "order_items", "payments", "shipments",
        "reviews", "cart_items", "wishlists",
        "product_views", "product_purchases"
    ])


    df = pd.read_csv(
        data_dir / "product_dataset.csv",
        usecols=["title", "price", "categoryName", "reviews"],
        encoding="utf-8",
        converters={"reviews": lambda x: int(str(x).replace(",", "").strip()) if x else 0}
    ).dropna()

    max_reviews = df["reviews"].max()
    df["review_weight"] = df["reviews"].apply(lambda r: 0.05 + 0.85 * (r / max_reviews if max_reviews else 0))
    
    products_raw = df.reset_index(drop=True)

    cat_name_to_id, categories = {}, []
    def get_cat_id(name: str) -> int:
        name = name.strip()
        if name not in cat_name_to_id:
            cid = len(categories) + 1
            cat_name_to_id[name] = cid
            categories.append({"id": cid, "name": name})
        return cat_name_to_id[name]

    products, prod_weights = [], {}
    pid = 1
    for idx, row in products_raw.iterrows():
        title = str(row.title)[:255]
        price = float(row.price)
        products.append({
            "id": pid,
            "name": title,
            "price": price
        })
        prod_weights[pid] = float(products_raw.loc[idx, "review_weight"])
        pid += 1


    # ID-Counter initialisieren
    addr_id = order_id = item_id = pay_id = ship_id = rev_id = cart_id = view_id = pur_id = 1

    for uid in tqdm(range(1, num_users + 1), desc="Generiere Nutzer"):
        stream_write(stream_files["users"], {
            "id": uid, "name": faker.name(), "email": faker.email(),
            "created_at": random_date_this_year()
        })

        num_addr = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        primary_set = False
        for i in range(num_addr):
            stream_write(stream_files["addresses"], {
                "id": addr_id, "user_id": uid,
                "street": faker.street_address(), "city": faker.city(),
                "zip": faker.postcode(), "country": "Deutschland",
                "is_primary": not primary_set or (i == num_addr - 1 and not primary_set)
            })
            addr_id += 1
            primary_set = True

        for _ in range(random.randint(1, 3)):
            created_ts = random_date_this_year()
            delta_weeks = random.randint(1, 3)
            updated_dt = datetime.fromisoformat(created_ts) + timedelta(weeks=delta_weeks)
            updated_ts = updated_dt.isoformat(timespec="seconds")
            status = random.choice(ORDER_STATI)
            items = random.sample(products, k=random.randint(1, 4))
            total = 0.0
            for prod in items:
                qty = random.randint(1, 3)
                stream_write(stream_files["order_items"], {
                    "id": item_id,
                    "order_id": order_id, "product_id": prod["id"],
                    "quantity": qty, "price": prod["price"]
                })
                item_id += 1
                stream_write(stream_files["product_purchases"], {
                    "id": pur_id,
                    "user_id": uid, "product_id": prod["id"], "purchased_at": created_ts
                })
                stream_write(stream_files["product_views"], {
                    "id": view_id,
                    "user_id": uid, "product_id": prod["id"],
                    "viewed_at": (datetime.fromisoformat(created_ts) - timedelta(minutes=random.randint(1, 10))).isoformat(timespec="seconds")
                })
                view_id += 1
                pur_id += 1
                total += qty * prod["price"]
                if random.random() < prod_weights[prod["id"]]:
                    stream_write(stream_files["reviews"], {
                        "id": rev_id,
                        "user_id": uid, "product_id": prod["id"],
                        "rating": random.randint(1, 5), "comment": None,
                        "created_at": created_ts
                    })
                    rev_id += 1

            stream_write(stream_files["orders"], {
                "id": order_id, "user_id": uid, "status": status,
                "total": round(total, 2), "created_at": created_ts, "updated_at": updated_ts
            })
            
            payment_offset = timedelta(hours=random.randint(1, 48))
            paid_at_dt = min(datetime.fromisoformat(created_ts) + payment_offset, datetime.now())
            paid_at = paid_at_dt.isoformat(timespec="seconds")

            stream_write(stream_files["payments"], {
                "id": pay_id, "order_id": order_id,
                "payment_method": random.choice(PAY_METHODS),
                "payment_status": "paid" if status != "CANCELLED" else "failed",
                "paid_at": None if status == "CANCELLED" else paid_at
            })
            pay_id += 1

            if status in {"SHIPPED", "COMPLETED"}:
                ship_ts = random_date_between(created_ts, "now")
                stream_write(stream_files["shipments"], {
                    "id": ship_id,
                    "order_id": order_id,
                    "tracking_number": faker.bothify("??########"),
                    "shipped_at": ship_ts,
                    "delivered_at": random_date_between(ship_ts, "now"),
                    "carrier": random.choice(CARRIERS)
                })
                ship_id += 1

            order_id += 1

        for prod in random.sample(products, k=random.randint(0, 3)):
            stream_write(stream_files["cart_items"], {
                "id": cart_id,
                "user_id": uid, "product_id": prod["id"],
                "quantity": random.randint(1, 2),
                "added_at": random_date_this_year()
            })
            cart_id += 1

        for prod in random.sample(products, k=random.randint(1, 5)):
            stream_write(stream_files["wishlists"], {
                "user_id": uid, "product_id": prod["id"],
                "created_at": random_date_this_year()
            })

        for _ in range(random.randint(1, 10)):
            prod = random.choice(products)
            stream_write(stream_files["product_views"], {
                "id": view_id,
                "user_id": uid, "product_id": prod["id"],
                "viewed_at": random_date_this_year()
            })
            view_id += 1

    # 📌 Sicherstellen, dass mindestens 1 Review existiert
    reviews_path = out_dir / "reviews.jsonl"
    if reviews_path.stat().st_size == 0:
        print("⚠️ Keine Reviews generiert – erzeuge 1 Dummy-Review")
        random_uid = random.randint(1, num_users)
        random_prod = random.choice(products)
        stream_write(stream_files["reviews"], {
            "id": rev_id,
            "user_id": random_uid,
            "product_id": random_prod["id"],
            "rating": random.randint(1, 5),
            "comment": None,
            "created_at": random_date_this_year()
        })

    close_stream_files(stream_files)
    print(f"\n✓ Streaming-Datensatz gespeichert unter: {out_dir.resolve()}")
    print(f"Dateien Zusammensetzen...")
    merge_jsonl_to_single_file(out_dir, final_dir, num_users)


def beautify_json_file(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def merge_jsonl_to_single_file(stream_dir: Path, final_dir: Path, num_users: int):
    tables = [
        "users", "addresses",
        "orders", "order_items", "payments", "shipments",
        "reviews", "cart_items", "wishlists",
        "product_views", "product_purchases"
    ]


    final_dir.mkdir(parents=True, exist_ok=True)
    final_file = final_dir / f"users_{num_users}.json"

    with open(final_file, "w", encoding="utf-8") as out:
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

    print(f"\n✓ Zusammengeführte Datei gespeichert unter: {final_file.resolve()}")

    # Stream-Verzeichnis löschen
    try:
        shutil.rmtree(stream_dir)
        print(f"✓ Stream-Ordner gelöscht: {stream_dir}")
    except Exception as e:
        print(f"⚠ Fehler beim Löschen von {stream_dir}: {e}")
    
    beautify_json_file(final_file)


# CLI
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, required=True, help="Anzahl User (z. B. 1000)")
    ap.add_argument("--data", default="product_data", help="Ordner mit product_dataset.csv")
    ap.add_argument("--out", default="output_streamed", help="Ordner für Stream-Dateien")
    ap.add_argument("--final", default="output", help="Zielordner für die finale JSON-Datei")
    args = ap.parse_args()

    build_dataset(args.users, Path(args.data), Path(args.out), Path(args.final))
