import csv
import psycopg2


conn = psycopg2.connect(
    dbname="north",
    user="postgres",
    host="localhost"
)
with open('north_data/employees_data.csv', 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    with conn.cursor() as cur:
        for row in reader:
            cur.execute(
                "INSERT INTO employees (first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s)",
                (row['first_name'], row['last_name'], row['title'], row['birth_date'], row['notes'])
            )
            conn.commit()

with open('north_data/customers_data.csv', 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    with conn.cursor() as cur:
        for row in reader:
            cur.execute(
                "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                (row['customer_id'], row['company_name'], row['contact_name'])
            )
            conn.commit()

with open('north_data/orders_data.csv', 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    with conn.cursor() as cur:
        for row in reader:
            cur.execute(
                "INSERT INTO orders (customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s)",
                (row['customer_id'], int(row['employee_id']), row['order_date'], row['ship_city'])
            )
            conn.commit()
