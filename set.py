import math
import sqlite3
import random
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import ec
from ecdsa import SECP256k1


def is_square(n, p):
    """ Check if n is a quadratic residue modulo p """
    return pow(n, (p - 1) // 2, p) == 1


# 创建数据库并连接
def create_database(db_name):
    conn = sqlite3.connect(db_name)
    return conn


# 创建表
def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS h_value (
            id INTEGER PRIMARY KEY,
            x TEXT NOT NULL,
            y TEXT NOT NULL
        )
    ''')
    conn.commit()


# 插入随机数据到第一个数据库
def insert_random_data(conn, num_entries):
    cursor = conn.cursor()
    for _ in range(num_entries):
        value = random.randint(0, 1)  # 生成0或1
        cursor.execute('INSERT INTO data (value) VALUES (?)', (value,))
    conn.commit()


# 生成椭圆曲线P-256上的随机点并插入到h_value表
def insert_h_value(conn, num_entries):
    curve = SECP256k1.curve
    p = curve.p()
    a = curve.a()
    b = curve.b()

    cursor = conn.cursor()
    for i in range(num_entries):
        while True:
            x = random.randrange(1, p)
            y_squared = (x ** 3 + a * x + b) % p
            if is_square(y_squared, p):
                y = pow(y_squared, (p + 1) // 4, p)
                cursor.execute('INSERT INTO h_value (id, x, y) VALUES (?, ?, ?)', (i + 1, str(x), str(y)))
                break  # 找到有效的点后退出循环
    conn.commit()


# 查询数据
def fetch_data(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM data')
    data_rows = cursor.fetchall()

    cursor.execute('SELECT * FROM h_value')
    h_value_rows = cursor.fetchall()

    return data_rows, h_value_rows


def add_points(h1, h2, a, p):
    """ Add two points on the elliptic curve """
    x1, y1 = h1
    x2, y2 = h2

    if h1 == h2:  # Doubling case
        m = (3 * x1 ** 2 + a) * mod_inverse(2 * y1, p) % p
    else:  # Addition case
        m = (y2 - y1) * mod_inverse(x2 - x1, p) % p

    x3 = (m ** 2 - x1 - x2) % p
    y3 = (m * (x1 - x3) - y1) % p

    return (x3, y3)


def mod_inverse(k, p):
    """ Calculate the modular inverse of k under modulo p """
    return pow(k, p - 2, p)


def calculate_data_table_size(conn):
    """ 计算 data 表的存储大小 """
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(LENGTH(value)) FROM data;")
    total_size = cursor.fetchone()[0]
    return total_size if total_size is not None else 0  # 如果没有数据，则返回 0


# 主程序
def main():
    db_name = 'data.db'

    # 检查并删除现有的数据库文件
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"Deleted existing database: {db_name}")

    # 创建数据库
    try:
        conn = create_database(db_name)
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return

    # 创建表
    create_tables(conn)

    # 插入数据  1kib = 1024 1mib = 1024*1024 1gib = 1024*1024*1024
    # size = 1024*8
    # bytes = 8
    # num_entries = math.ceil(size // bytes)
    num_entries = 49
    insert_random_data(conn, num_entries)
    insert_h_value(conn, num_entries)  # 将 h 值插入到 h_value 表中

    # 查询并打印数据
    data_rows, h_value_rows = fetch_data(conn)
    print("Data from first database:", data_rows)
    print("Data from h_value table (points):", h_value_rows)

    # 计算并打印 data 表的大小
    data_table_size = calculate_data_table_size(conn)
    print(f"Size of data table in bytes: {data_table_size}")

    # 关闭连接
    conn.close()


if __name__ == '__main__':
    main()