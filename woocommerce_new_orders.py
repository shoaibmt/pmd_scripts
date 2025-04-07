from dotenv import load_dotenv
import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
from google_sheets_helper import google_sheet_connection

load_dotenv()
ORDERS_PER_PAGE = int(os.getenv("ORDERS_PER_PAGE", 100))
MAX_ORDERS = int(os.getenv("MAX_ORDERS", 10000))
sheet_id = os.getenv("SHEET_ID") 
store_timezone = os.getenv("STORE_TIMEZONE", "UTC")
store_tz = pytz.timezone(store_timezone)

# Setup
order_url = "https://getpetermd.com/wp-json/wc/v3/orders"
product_url = "https://getpetermd.com/wp-json/wc/v3/products"
consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
headers = {"User-Agent": "Mozilla/5.0"}
sheet_service = google_sheet_connection()
log_sheet_name = "logs"

# Load last logged order datetime
log_data = sheet_service.spreadsheets().values().get(
    spreadsheetId=sheet_id,
    range=f"{log_sheet_name}!A2:H"
).execute().get("values", [])

if not log_data:
    print("No logs found. Skipping fetch. Please run an initial sync manually.")
    exit()

last_log_row = log_data[-1]
last_logged_created = pd.to_datetime(last_log_row[1], errors='coerce')
if pd.isna(last_logged_created):
    print("Last logged 'Date Created' is missing or invalid. Skipping fetch.")
    exit()

# Ensure timezone-aware datetime in America/Tijuana
if last_logged_created.tzinfo is None:
    last_logged_created = store_tz.localize(last_logged_created)
else:
    last_logged_created = last_logged_created.astimezone(store_tz)

# Add 1 second and convert to ISO 8601
after_dt = last_logged_created + timedelta(seconds=1)
after = after_dt.isoformat()
after_dt_obj = pd.to_datetime(after).tz_localize(None)
print(f"Fetching orders after {after} (Tijuana time)")

# Fetch orders
params = {
    'consumer_key': consumer_key,
    'consumer_secret': consumer_secret,
    'per_page': ORDERS_PER_PAGE,
    'orderby': 'date',
    'order': 'asc',
    'after': after
}

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
            print(f"Exception on page {page}: {e}")
            retries += 1
            time.sleep(5 * retries)

    if not success or len(data) < ORDERS_PER_PAGE or len(all_orders) >= MAX_ORDERS:
        break
    page += 1
    if page % 20 == 0:
        print("Cooling down to avoid rate limit...")
        time.sleep(30)

if not all_orders:
    print("No new orders to process.")
    exit()

# Manually filter orders by 'date_created' to avoid WC inconsistencies
filtered_orders = []
for order in all_orders:
    created = pd.to_datetime(order.get("date_created"), errors='coerce').tz_localize(None)
    if created and created > after_dt_obj and order.get("status") not in ["failed", "draft"]:
        filtered_orders.append(order)

all_orders = filtered_orders
print(f"Total orders after filtering by date: {len(all_orders)}")

# Fetch categories
product_ids = list(set(item['product_id'] for order in all_orders for item in order.get('line_items', [])))
product_categories = {}
for pid in product_ids:
    try:
        prod_resp = requests.get(f"{product_url}/{pid}", auth=(consumer_key, consumer_secret), headers=headers)
        if prod_resp.status_code == 200:
            prod_data = prod_resp.json()
            product_categories[pid] = ", ".join([cat["name"] for cat in prod_data.get("categories", [])])
        else:
            product_categories[pid] = ""
    except Exception as e:
        product_categories[pid] = ""

# Flatten orders
expanded_orders = []
for order in all_orders:
    for item in order.get('line_items', []):
        expanded_orders.append({
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
        })

# Save to sheet
orders_df = pd.DataFrame(expanded_orders)
orders_df['Date Created'] = pd.to_datetime(orders_df['Date Created'], errors='coerce')
orders_df = orders_df.sort_values(by='Date Created', ascending=True)
values = orders_df.fillna("").astype(str).values.tolist()

if values:
    chunk_size = 100
    max_upload_retries = 3
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i+chunk_size]
        upload_success = False
        attempt = 0

        while not upload_success and attempt < max_upload_retries:
            try:
                sheet_service.spreadsheets().values().append(
                    spreadsheetId=sheet_id,
                    range="Orders!A2",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": chunk}
                ).execute()
                print(f"Appended {len(chunk)} records to Google Sheets (rows {i+1} to {i+len(chunk)}).")
                upload_success = True
                time.sleep(1)
            except Exception as e:
                attempt += 1
                print(f"Failed to upload rows {i+1}-{i+len(chunk)} (attempt {attempt}): {e}")
                time.sleep(5 * attempt)

# Log last order
if not orders_df.empty:
    last_row = orders_df.iloc[-1]
    log_row = [
        str(last_row["Order ID"]),
        str(last_row["Date Created"]),
        str(last_row["Date Paid"]),
        str(last_row["Customer ID"]),
        str(last_row["Name"]),
        str(last_row["Email"]),
        store_timezone
    ]
    sheet_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{log_sheet_name}!A2:G2",
        valueInputOption="USER_ENTERED",
        body={"values": [log_row]}
    ).execute()
    print("Updated last log entry in logs sheet.")

print(f"\nDone. Total orders uploaded: {len(orders_df)} in {time.time() - start_time:.2f} seconds.")