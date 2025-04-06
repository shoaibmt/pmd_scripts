import requests
import pandas as pd

url = "https://getpetermd.com/wp-json/wc/v3/products"
consumer_key = "ck_5840ed9a9e7d0c211f8a994899e671fe18e637c5"
consumer_secret = "cs_6ebe988c97af09f589d562d14af4851e84c3ead7"

params = {
    'consumer_key': consumer_key,
    'consumer_secret': consumer_secret
}

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, params=params, headers=headers)

if response.status_code == 200:
    data = response.json()

    products = []
    for item in data:
        product = {
            "name": item.get("name"),
            "categories": ", ".join([cat["name"] for cat in item.get("categories", [])]),
            "price": item.get("price"),
            "regular_price": item.get("regular_price"),
            "sale_price": item.get("sale_price"),
        }
        products.append(product)

    df = pd.DataFrame(products)
    print(df)
    # df.to_csv("woocommerce_products.csv", index=False)
else:
    print("Failed to fetch data. Response:")
    print(response.text)