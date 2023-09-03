import pandas as pd
import sqlite3
import csv

# Step 1: Create or connect to the SQLite database
conn = sqlite3.connect("shipping_data.db")
cursor = conn.cursor()

# Step 2: Create tables if they don't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        quantity INTEGER,
        shipping_identifier TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipments (
        shipping_identifier TEXT PRIMARY KEY,
        origin TEXT,
        destination TEXT
    )
''')

# Step 3: Read and insert data from Spreadsheet 0 (CSV) using Pandas
df0 = pd.read_csv('./data/shipping_data_0.csv')
df0.rename(columns={
    'product': 'product_name',
    'product_quantity': 'quantity',
    'driver_identifier': 'shipping_identifier'
}, inplace=True)
df0.to_sql('products', conn, if_exists='replace', index=False)

# Step 4: Read and insert data from Spreadsheet 2 (CSV) using Pandas
df2 = pd.read_csv('./data/shipping_data_2.csv')
df2.rename(columns={
    'shipment_identifier': 'shipping_identifier',
    'origin_warehouse': 'origin',
    'destination_store': 'destination',
    'driver_identifier': 'driver_identifier'
}, inplace=True)
df2.to_sql('shipments', conn, if_exists='replace', index=False)

# Step 5: Read and insert data from Spreadsheet 1 (CSV) using Pandas
df1 = pd.read_csv('./data/shipping_data_1.csv', usecols=['shipment_identifier', 'product', 'on_time'])
df1.rename(columns={
    'shipment_identifier': 'shipping_identifier',
    'on_time': 'is_on_time'
}, inplace=True)

# Group by shipping_identifier and product_name to calculate the total quantity for each product in each shipment
grouped = df1.groupby(['shipping_identifier', 'product'])['is_on_time'].count().reset_index()

# Iterate through grouped data and insert each product with its quantity
for _, row in grouped.iterrows():
    product_name = row['product']
    quantity = row['is_on_time']
    shipping_identifier = row['shipping_identifier']

    cursor.execute('''
        INSERT INTO products (product_name, quantity, shipping_identifier)
        VALUES (?, ?, ?)
    ''', (product_name, quantity, shipping_identifier))

conn.commit()
conn.close()