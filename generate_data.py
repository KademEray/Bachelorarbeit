import json, argparse, random
from pathlib import Path
from datetime import datetime
import pandas as pd
from faker import Faker
from tqdm import tqdm  # Fortschrittsbalken

# -------------------------------------------------- Konstanten
ORDER_STATI  = ["NEW", "PAID", "SHIPPED", "COMPLETED", "CANCELLED"]
PAY_METHODS  = ["card", "paypal", "invoice"]
CARRIERS     = ["DHL", "Hermes", "DPD", "GLS"]
faker = Faker("de_DE")

# -------------------------------------------------- Hauptgenerator
def build_dataset(num_users: int, data_dir: Path, out_dir: Path):
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"ecommerce_{num_users}.json"

    df = pd.read_csv(
        data_dir / "product_dataset.csv",
        usecols=["title", "price", "categoryName", "reviews"],
        encoding="ISO-8859-1",
        converters={"reviews": lambda x: int(str(x).replace(",", "").strip()) if x else 0}
    ).dropna()

    max_reviews = df["reviews"].max()
    df["review_weight"] = df["reviews"].apply(lambda r: 0.05 + 0.85 * (r / max_reviews if max_reviews else 0))

    sample_size   = min(num_users * 2, len(df))
    products_raw  = df.sample(n=sample_size).reset_index(drop=True)

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
        products.append({
            "id": pid,
            "name": str(row.title)[:255],
            "description": None,
            "price": float(row.price),
            "stock": random.randint(1, 100),
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds")
        })
        prod_weights[pid] = float(products_raw.loc[idx, "review_weight"])
        for cat in {c.strip() for c in row.categoryName.split(",") if c.strip()}:
            cid = get_cat_id(cat)
            product_categories.append({"product_id": pid, "category_id": cid})

    users, addresses, orders, order_items = [], [], [], []
    payments, shipments, reviews, cart_items = [], [], [], []
    wishlists, views, purchases = [], [], []

    for uid in tqdm(range(1, num_users + 1), desc="Generiere Nutzerdaten"):
        users.append({
            "id": uid, "name": faker.name(), "email": faker.email(),
            "created_at": faker.date_time_this_year().isoformat()
        })

        num_addr = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        primary_set = False
        for i in range(num_addr):
            addresses.append({
                "id": len(addresses) + 1,
                "user_id": uid,
                "street": faker.street_address(),
                "city": faker.city(),
                "zip": faker.postcode(),
                "country": "Deutschland",
                "is_primary": not primary_set or (i == num_addr - 1 and not primary_set)
            })
            primary_set = primary_set or addresses[-1]["is_primary"]

        for _ in range(random.randint(1, 3)):
            oid  = len(orders) + 1
            o_ts = faker.date_time_this_year()
            status = random.choice(ORDER_STATI)
            items = random.sample(products, k=random.randint(1, 4))
            total = 0.0
            for prod in items:
                qty = random.randint(1, 3)
                order_items.append({
                    "id": len(order_items)+1, "order_id": oid,
                    "product_id": prod["id"], "quantity": qty, "price": prod["price"]
                })
                purchases.append({
                    "id": len(purchases)+1, "user_id": uid,
                    "product_id": prod["id"], "purchased_at": o_ts.isoformat()
                })
                total += qty * prod["price"]
                if random.random() < prod_weights[prod["id"]]:
                    reviews.append({
                        "id": len(reviews)+1, "user_id": uid, "product_id": prod["id"],
                        "rating": random.randint(1, 5),
                        "comment": None,
                        "created_at": o_ts.isoformat()
                    })
            orders.append({
                "id": oid, "user_id": uid, "status": status,
                "total": round(total, 2),
                "created_at": o_ts.isoformat(), "updated_at": o_ts.isoformat()
            })
            payments.append({
                "id": len(payments)+1, "order_id": oid,
                "payment_method": random.choice(PAY_METHODS),
                "payment_status": "paid" if status != "CANCELLED" else "failed",
                "paid_at": o_ts.isoformat()
            })
            if status in {"SHIPPED", "COMPLETED"}:
                ship_ts = faker.date_time_between(o_ts, "now")
                shipments.append({
                    "id": len(shipments)+1, "order_id": oid,
                    "tracking_number": faker.bothify("??########"),
                    "shipped_at": ship_ts.isoformat(),
                    "delivered_at": faker.date_time_between(ship_ts, "now").isoformat(),
                    "carrier": random.choice(CARRIERS)
                })

        bought_ids = {pi["product_id"] for pi in purchases if pi["user_id"] == uid}
        not_bought = [p for p in products if p["id"] not in bought_ids]
        for prod in random.sample(not_bought, k=random.randint(0, 3)):
            cart_items.append({
                "id": len(cart_items)+1, "user_id": uid,
                "product_id": prod["id"],
                "quantity": random.randint(1, 2),
                "added_at": faker.date_time_this_year().isoformat()
            })
        for prod in random.sample(products, k=random.randint(1, 5)):
            wishlists.append({
                "user_id": uid, "product_id": prod["id"],
                "created_at": faker.date_time_this_year().isoformat()
            })
        for _ in range(random.randint(1, 10)):
            prod = random.choice(products)
            views.append({
                "id": len(views)+1, "user_id": uid,
                "product_id": prod["id"],
                "viewed_at": faker.date_time_this_year().isoformat()
            })

    dataset = {
        "users": users, "addresses": addresses,
        "categories": categories, "products": products,
        "product_categories": product_categories,
        "orders": orders, "order_items": order_items,
        "payments": payments, "shipments": shipments,
        "reviews": reviews, "cart_items": cart_items,
        "wishlists": wishlists, "product_views": views,
        "product_purchases": purchases
    }
    out_file.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✓ Datensatz geschrieben: {out_file.resolve()}")

# -------------------------------------------------- CLI
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, required=True, help="Anzahl User (z. B. 1000)")
    ap.add_argument("--data",  default="product_data", help="Ordner mit product_dataset.csv")
    ap.add_argument("--out",   default="output", help="Ausgabe-Ordner")
    args = ap.parse_args()
    build_dataset(args.users, Path(args.data), Path(args.out))
