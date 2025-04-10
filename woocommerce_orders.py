from dotenv import load_dotenv
import os
import requests
import pandas as pd
import time
from google_sheets_helper import google_sheet_connection

load_dotenv()

# CONFIG
MAX_ORDERS = 100  # Set to None to fetch all orders, or specify a number like 1000
ORDERS_PER_PAGE = 100  # WooCommerce allows up to 100 per request

# WooCommerce API credentials
order_url = "https://getpetermd.com/wp-json/wc/v3/orders"
product_url = "https://getpetermd.com/wp-json/wc/v3/products"
consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")

headers = {"User-Agent": "Mozilla/5.0"}
params = {
    'consumer_key': consumer_key,
    'consumer_secret': consumer_secret,
    'per_page': ORDERS_PER_PAGE,
    'orderby': 'date',
    'order': 'desc'
}

# Fetch product categories
product_categories = {}
response = requests.get(product_url, params={'consumer_key': consumer_key, 'consumer_secret': consumer_secret})
if response.status_code == 200:
    products = response.json()
    for i, product in enumerate(products):
        product_id = product.get('id')
        categories = product.get('categories', [])
        product_categories[product_id] = ", ".join([cat.get('name', '') for cat in categories])
        print(f"Product ID: {product_id}, Categories: {product_categories[product_id]}")  # Debug output
        if (i + 1) % 100 == 0:
            print("Cooling down to avoid category API rate limit...")
            time.sleep(10)

start_time = time.time()
print("Fetching orders...")

all_orders = []
page = 1
while True:
    paged = params.copy()
    paged['page'] = page
    retries = 0
    success = False

    while not success and retries < 5:
        try:
            response = requests.get(order_url, params=paged, headers=headers)
            if response.status_code != 200:
                print(f"Page {page} failed with status {response.status_code}. Retrying in 5s...")
                retries += 1
                time.sleep(5 * retries)
                continue

            data = response.json()
            if not data:
                print(f"Page {page} returned empty data.")
                success = True
                break

            all_orders.extend(data)
            print(f"Fetched page {page} with {len(data)} orders")

            if len(data) < ORDERS_PER_PAGE:
                success = True
                break

            success = True
        except Exception as e:
            print(f"Exception while fetching page {page}: {e}. Retrying in 5s...")
            retries += 1
            time.sleep(5 * retries)

    if not success:
        print(f"Giving up on page {page} after {retries} retries.")
        break

    if MAX_ORDERS and len(all_orders) >= MAX_ORDERS:
        all_orders = all_orders[:MAX_ORDERS]
        print(f"Reached limit of {MAX_ORDERS} orders.")
        break

    if len(data) < ORDERS_PER_PAGE:
        break

    page += 1

    if page % 20 == 0:
        print("Cooling down to avoid rate limit...")
        time.sleep(30)

print(f"\nTotal orders fetched: {len(all_orders)}")

# Expand products into individual rows
expanded_orders = []
for order in all_orders:
    for item in order.get('line_items', []):
        flat_order = {
            'Order ID': order.get('id'),
            'Date Created': order.get('date_created'),
            'Date Paid': order.get('date_paid'),
            'Status': order.get('status'),
            'Customer ID': order.get('customer_id'),
            'Name': f"{order.get('billing', {}).get('first_name', '')} {order.get('billing', {}).get('last_name', '')}".strip(),
            'Email': order.get('billing', {}).get('email'),
            'Product ID': item.get('product_id'),
            'Product Name': item.get('name'),
            'Category': product_categories.get(item.get('product_id'), ""),
            'Total Amount': order.get('total'),
            'Total Discount': order.get('discount_total'),
            'Payment Method': order.get('payment_method'),
            'Payment Method Title': order.get('payment_method_title'),
        }
        expanded_orders.append(flat_order)

orders_df = pd.DataFrame(expanded_orders)
orders_df['Date Created'] = pd.to_datetime(orders_df['Date Created'], errors='coerce')
orders_df = orders_df.sort_values(by='Date Created', ascending=True)

print(orders_df)

orders_df.to_csv("orders_export.csv", index=False)

# Upload to Google Sheets
sheet_service = google_sheet_connection()
sheet_id = "1AhF-dqocc3s362-7pARTWWDyw_SYTWrFD2gVW4eaJZw"
sheet_name = "Orders"

# Clear existing data below headers
sheet_service.spreadsheets().values().clear(
    spreadsheetId=sheet_id,
    range=f"{sheet_name}!A2:Z"
).execute()

values = orders_df.fillna("").astype(str).values.tolist()
range_all = f"{sheet_name}!A2"
sheet_service.spreadsheets().values().update(
    spreadsheetId=sheet_id,
    range=range_all,
    valueInputOption="USER_ENTERED",
    body={"values": values}
).execute()
print(f"Uploaded {len(values)} records to Google Sheets.")

print(f"\nDone. Total orders uploaded: {len(orders_df)} in {time.time() - start_time:.2f} seconds.")