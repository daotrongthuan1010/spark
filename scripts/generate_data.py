import psycopg2
import random
from datetime import datetime, timedelta
from decimal import Decimal

# Kết nối
conn = psycopg2.connect(
    host="postgres",
    database="spark_demo_db",
    user="admin",
    password="password",
    port=5432
)
cursor = conn.cursor()

# Tạo bảng
cursor.execute("DROP TABLE IF EXISTS sales_data")
cursor.execute("""
    CREATE TABLE sales_data (
        id SERIAL PRIMARY KEY,
        product VARCHAR(50),
        quantity INTEGER,
        price DECIMAL(10,2),
        timestamp TIMESTAMP
    )
""")
conn.commit()

products = [
    "Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Quạt", "Tivi", "Điện thoại", "Tai nghe", "Máy giặt",
    "Quạt điều hòa", "Tay cầm", "Camera", "Loa bluetooth", "Bếp từ", "Tủ lạnh", "Máy lạnh", "Máy hút bụi",
    "Router Wifi", "Máy in", "Máy chiếu", "Đèn bàn", "Pin sạc", "Sạc dự phòng", "Ổ cứng", "SSD", "RAM",
    "Card màn hình", "CPU", "Mainboard", "Nguồn máy tính", "Case máy tính", "MacBook", "iPad", "AirPods",
    "Apple Watch", "Smartwatch", "Đồng hồ thông minh", "Bàn phím cơ", "Chuột gaming", "Ghế gaming",
    "Bàn gaming", "Microphone", "Webcam", "Máy pha cà phê", "Nồi cơm điện", "Nồi chiên không dầu", "Máy lọc nước",
    "Máy xay sinh tố", "Máy ép chậm", "Máy sấy tóc", "Máy uốn tóc", "Máy cạo râu", "Bàn ủi hơi nước",
    "Máy đo huyết áp", "Máy massage", "Robot hút bụi", "Đèn LED", "Ổ cắm thông minh", "Máy đọc sách",
    "Tay cầm chơi game", "Bàn di chuột", "Balo laptop", "Bao da điện thoại", "Kính thực tế ảo", "Thiết bị trình chiếu",
    "Bảng vẽ điện tử", "USB", "Thẻ nhớ", "Dock sạc", "Máy chơi game", "PlayStation", "Xbox", "Nintendo Switch",
    "Tay cầm PS5", "Kính VR", "Điều hòa mini", "Tủ đông", "Tủ mát", "Máy làm mát", "Máy tạo độ ẩm", "Loa kéo",
    "Máy rửa chén", "TV Box", "Set-top box", "Thiết bị định vị", "Thiết bị chống trộm", "Camera hành trình",
    "Màn hình cong", "Màn hình 4K", "Máy tính bảng", "Điện thoại gập", "Điện thoại gaming", "Laptop gaming",
    "Máy tính mini", "PC để bàn", "Máy ảnh kỹ thuật số", "Ống kính", "Đèn flash máy ảnh", "Tripod", "Gimbal"
]

# Tạo giá
prices = {}
for product in products:
    if any(keyword in product.lower() for keyword in ['laptop', 'macbook', 'pc', 'máy tính']):
        price = random.uniform(500000, 50000000)
    elif any(keyword in product.lower() for keyword in ['điện thoại', 'iphone', 'ipad', 'tablet']):
        price = random.uniform(200000, 30000000)
    elif any(keyword in product.lower() for keyword in ['tai nghe', 'headset', 'airpods']):
        price = random.uniform(50000, 7000000)
    elif any(keyword in product.lower() for keyword in ['nồi', 'quạt', 'máy']):
        price = random.uniform(100000, 10000000)
    else:
        price = random.uniform(50000, 10000000)
    prices[product] = round(price, 2)

# Tạo dữ liệu
start_date = datetime(2024, 1, 1)
batch_size = 1000
total_records = 10000000

print("Bắt đầu tạo dữ liệu...")

for i in range(0, total_records, batch_size):
    values = []
    for _ in range(min(batch_size, total_records - i)):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        price = Decimal(prices[product])
        timestamp = start_date + timedelta(days=random.randint(0, 365))
        values.append((product, quantity, price, timestamp))

    try:
        cursor.executemany("""
            INSERT INTO sales_data (product, quantity, price, timestamp)
            VALUES (%s, %s, %s, %s)
        """, values)
        conn.commit()
        print(f"Đã tạo {i + len(values)} bản ghi...")
    except Exception as e:
        print("❌ Lỗi khi chèn dữ liệu:", e)
        conn.rollback()
        break

print("✅ Hoàn thành tạo dữ liệu!")
cursor.close()
conn.close()
