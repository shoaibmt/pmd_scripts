import pandas as pd
import time
from google_sheets_helper import google_sheet_connection

# Google Sheets settings
sheet_id = "1AhF-dqocc3s362-7pARTWWDyw_SYTWrFD2gVW4eaJZw"
orders_sheet = "Orders"
customers_sheet = "Customers"

# Connect to Google Sheets
sheet_service = google_sheet_connection()

# Fetch all order data
print("Reading data from Google Sheets...")
start_time = time.time()
response = sheet_service.spreadsheets().values().get(
    spreadsheetId=sheet_id,
    range=orders_sheet
).execute()

data = response.get("values", [])
headers = data[0]
rows = data[1:]

if not rows:
    print("No order data found.")
    exit()

orders_df = pd.DataFrame(rows, columns=headers)

# Ensure types
orders_df['Total Amount'] = pd.to_numeric(orders_df['Total Amount'], errors='coerce').fillna(0)
orders_df['Customer ID'] = orders_df['Customer ID'].fillna("0")
orders_df['Customer ID'] = orders_df['Customer ID'].astype(str)
orders_df['Email'] = orders_df['Email'].str.strip().str.lower()
orders_df['Date Created'] = pd.to_datetime(orders_df['Date Created'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
orders_df['Discount Total'] = pd.to_numeric(orders_df['Total Discount'], errors='coerce').fillna(0)

# Registered customers
registered_df = orders_df[orders_df['Customer ID'] != "0"]
reg_summary = registered_df.groupby('Customer ID').agg({
    'Name': 'first',
    'Email': 'first',
    'Order ID': 'count',
    'Total Amount': 'sum',
    'Discount Total': 'sum',
    'Date Created': ['min', 'max']
}).reset_index()
reg_summary.columns = ['ID', 'Name', 'Email', 'Total Orders', 'Amount Spent', 'Total Discount', 'First Order Date', 'Last Order Date']
reg_summary = reg_summary[['Email', 'ID', 'First Order Date', 'Last Order Date', 'Name', 'Total Orders', 'Amount Spent', 'Total Discount']]

# Guest customers
guest_df = orders_df[orders_df['Customer ID'] == "0"]
guest_summary = guest_df.groupby('Email').agg({
    'Name': 'first',
    'Order ID': 'count',
    'Total Amount': 'sum',
    'Discount Total': 'sum',
    'Date Created': ['min', 'max']
}).reset_index()
guest_summary.columns = ['Email', 'Name', 'Total Orders', 'Amount Spent', 'Total Discount', 'First Order Date', 'Last Order Date']
guest_summary['ID'] = "0"
guest_summary = guest_summary[['Email', 'ID', 'First Order Date', 'Last Order Date', 'Name', 'Total Orders', 'Amount Spent', 'Total Discount']]

# Combine both
final_df = pd.concat([reg_summary, guest_summary], ignore_index=True)
final_df = final_df.fillna("")
final_df = final_df[['Email', 'ID', 'First Order Date', 'Last Order Date', 'Name', 'Total Orders', 'Amount Spent', 'Total Discount']]

# Upload to Google Sheets
print("\nUploading customer summary to Google Sheets...")

# Clear old customer data except headers
sheet_service.spreadsheets().values().clear(
    spreadsheetId=sheet_id,
    range=f"{customers_sheet}!A2:Z"
).execute()

# Upload in chunks
def split_dataframe(df, chunk_size):
    for i in range(0, df.shape[0], chunk_size):
        yield df.iloc[i:i + chunk_size]

for i, chunk in enumerate(split_dataframe(final_df, 1000)):
    chunk['First Order Date'] = pd.to_datetime(chunk['First Order Date'], errors='coerce')
    chunk['Last Order Date'] = pd.to_datetime(chunk['Last Order Date'], errors='coerce')
    chunk['First Order Date'] = chunk['First Order Date'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
    chunk['Last Order Date'] = chunk['Last Order Date'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("")
    values = chunk.astype(str).values.tolist()

    range_start = f"{customers_sheet}!A2"
    sheet_service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=range_start,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()

    print(f"Uploaded batch {i + 1} ({len(chunk)} records)")

print(f"\nDone. Total unique customers uploaded: {len(final_df)} in {time.time() - start_time:.2f} seconds.")