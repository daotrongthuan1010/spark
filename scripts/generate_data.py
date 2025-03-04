import psycopg2
import random
from datetime import datetime, timedelta

# Kết nối tới PostgreSQL trong Docker
conn = psycopg2.connect(
    host="postgres",
    database="spark_demo_db",
    user="admin",
    password="password",
    port=5432
)
cursor = conn.cursor()

# Tạo bảng nếu chưa tồn tại
cursor.execute("""
    DROP TABLE IF EXISTS sales_data;
    CREATE TABLE sales_data (
        id SERIAL PRIMARY KEY,
        product VARCHAR(50),
        quantity INTEGER,
        price DECIMAL(10,2),
        timestamp TIMESTAMP
    );
""")
conn.commit()

# Danh sách sản phẩm mẫu
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headset']
prices = {'Laptop': 999.99, 'Mouse': 29.99, 'Keyboard': 59.99, 'Monitor': 199.99, 'Headset': 79.99}

# Tạo 10 triệu bản ghi
start_date = datetime(2025, 1, 1)
batch_size = 10000  # Kích thước batch để tránh lỗi bộ nhớ
total_records = 10000000

print("Bắt đầu tạo dữ liệu...")

for i in range(0, total_records, batch_size):
    values = []
    for _ in range(batch_size if i + batch_size <= total_records else total_records - i):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        price = prices[product]
        timestamp = start_date + timedelta(days=random.randint(0, 365))
        values.append((product, quantity, price, timestamp))

    # Chèn dữ liệu theo batch
    insert_query = """
        INSERT INTO sales_data (product, quantity, price, timestamp)
        VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(insert_query, values)
    conn.commit()
    print(f"Đã tạo {i + len(values)} bản ghi...")

print("Hoàn thành tạo dữ liệu!")

# Đóng kết nối
cursor.close()
conn.close()