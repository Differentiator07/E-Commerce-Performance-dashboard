import pandas as pd

#load data
df = pd.read_csv ("ecommerce_dataset.csv")
print (df.head())

#Understand data[]
print (df.info())
print (df.describe())

#check missing values
print(df.isnull().sum())

#Remove duplicates
df = df.drop_duplicates()

#Validate Revenue
df['calculated_revenue'] = df['quantity'] * df['price']

#convert date
df['order_date'] = pd.to_datetime(df['order_date'])

#final output
print(df.head())

import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# Insert data row by row
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO ecommerce_data (
            order_id, customer_id, product_id, category,
            quantity, price, order_date, payment_method,
            city, revenue, calculated_revenue
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['order_id'],
        row['customer_id'],
        row['product_id'],
        row['category'],
        row['quantity'],
        row['price'],
        row['order_date'],
        row['payment_method'],
        row['city'],
        row['revenue'],
        row['calculated_revenue']
    ))

conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully!")

import psycopg2
print("psycopg2 working")