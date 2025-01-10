import sqlite3
import hashlib
import os
import random
import secrets
from ecdsa import SECP256k1
import time
from concurrent.futures import ThreadPoolExecutor

def create_new_database(db_name):
    """ 创建一个新的数据库文件 """
    conn = sqlite3.connect(db_name)
    return conn

def fetch_data_from_existing_db(existing_db_name):
    """ 从已有的数据库中获取 data 表的值 """
    conn = sqlite3.connect(existing_db_name)
    cursor = conn.cursor()
    cursor.execute('SELECT id, value FROM data')  # 查询 id 和 value 列
    rows = cursor.fetchall()  # 获取所有行
    conn.close()  # 关闭连接
    return rows

def fetch_h_values(conn):
    """ 获取 h_value 表中的数据 """
    cursor = conn.cursor()
    cursor.execute('SELECT x, y FROM h_value')  # 查询 x 和 y 列
    rows = cursor.fetchall()  # 获取所有行
    return [(int(x), int(y)) for x, y in rows]  # 转换为整数元组

def fetch_lookup_table_values(conn):
    """ 获取 look_table 中每个桶的 value """
    cursor = conn.cursor()
    cursor.execute('SELECT b, value FROM look_table')  # 查询 b 和 value 列
    rows = cursor.fetchall()  # 获取所有行
    buckets = {}

    for b, value in rows:
        if b not in buckets:
            buckets[b] = []
        buckets[b].append(value)

    return buckets  # 返回每个桶的值字典

def is_square(n, p):
    """ Check if n is a quadratic residue modulo p """
    return pow(n, (p - 1) // 2, p) == 1

def sha1_hash(entry):
    """ Generate SHA-1 hash of the entry index, return as an integer """
    hash_object = hashlib.sha1(str(entry['index']).encode())
    hash_hex = hash_object.hexdigest()  # 获取十六进制表示
    return int(hash_hex, 16)  # 将十六进制转换为整数

def distribute_entries(entries, num_buckets):
    """ Distribute entries into buckets using linear probing for better uniformity """
    buckets = {i: [] for i in range(num_buckets)}  # 初始化桶
    bucket_counts = [0] * num_buckets  # 初始化桶的计数器

    for entry in entries:
        hash_value = sha1_hash(entry)
        bucket_index = hash_value % num_buckets  # 计算初始桶索引

        while len(buckets[bucket_index]) >= (len(entries) // num_buckets) + 1:
            bucket_index = (bucket_index + 1) % num_buckets  # 循环到下一个桶

        buckets[bucket_index].append(entry)
        bucket_counts[bucket_index] += 1  # 更新当前桶的计数

    max_count = max(bucket_counts)
    return buckets, max_count

def fill_virtual_values(buckets, max_count):
    """ 对每个桶不足最大元素的桶添加虚拟值 (id=0, value=0) """
    for bucket in buckets.values():
        while len(bucket) < max_count:
            bucket.append({'index': 0, 'value': 0})  # 添加虚拟值

def insert_h_values(conn, num_entries):
    """ 在椭圆曲线 P-256 上生成 h 值并插入数据库 """
    curve = SECP256k1.curve
    p = curve.p()
    a = curve.a()
    b = curve.b()

    cursor = conn.cursor()
    h_values = []
    for i in range(num_entries):
        while True:
            x = random.randrange(1, p)
            y_squared = (x ** 3 + a * x + b) % p
            if is_square(y_squared, p):
                y = pow(y_squared, (p + 1) // 4, p)
                h_values.append((i + 1, str(x), str(y)))
                break

    cursor.executemany('INSERT INTO h_value (id, x, y) VALUES (?, ?, ?)', h_values)
    conn.commit()

def generate_random_value(probability=0):
    """ 生成有限域 P-256 中的随机值 """
    curve = SECP256k1.curve
    p = curve.p()  # 获取有限域的素数 p

    if random.random() < probability:
        smaller_length = (len(str(p)) * 2) // 3
        return secrets.randbelow(10 ** smaller_length)
    else:
        return secrets.randbelow(p)

def add_points(h1, h2, a, p):
    """ Add two points on the elliptic curve """
    if h1 is None or h2 is None:
        return None

    x1, y1 = h1
    x2, y2 = h2

    # 计算斜率 m
    if h1 == h2:
        m = (3 * x1 ** 2 + a) * mod_inverse(2 * y1, p) % p
    else:
        m = (y2 - y1) * mod_inverse(x2 - x1, p) % p

    # 计算新点的坐标
    x3 = (m ** 2 - x1 - x2) % p
    y3 = (m * (x1 - x3) - y1) % p

    return (x3, y3)

def mod_inverse(k, p):
    """ Calculate the modular inverse of k under modulo p """
    return pow(k, p - 2, p)

def scalar_multiply(k, h, a, p):
    """ Perform scalar multiplication on an elliptic curve point """
    if k == 0:
        return None
    if k == 1:
        return h

    R = None
    H = h

    # 使用倍加法
    while k > 0:
        if k % 2 == 1:
            R = add_points(R, H, a, p) if R else H
        H = add_points(H, H, a, p)
        k //= 2

    return R

def create_tables(conn):
    """ 创建 h_value 和 look_table 数据库表 """
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS h_value (
            id INTEGER PRIMARY KEY,
            x TEXT,
            y TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS look_table (
            b INTEGER,
            b_index INTEGER,
            id INTEGER,
            value INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS buck_digest (
            b INTEGER,
            x TEXT,
            y TEXT
        )
    ''')

def create_lookup_table(conn, buckets):
    """ 创建 look_table 数据库并插入分桶数据 """
    cursor = conn.cursor()

    for bucket_index, bucket in buckets.items():
        for b_index, entry in enumerate(bucket):
            cursor.execute('''
                INSERT INTO look_table (b, b_index, id, value)
                VALUES (?, ?, ?, ?)
            ''', (bucket_index, b_index + 1, entry['index'], entry['value']))

    conn.commit()

def calculate_look_table_b_size(conn):
    """计算 look_table 表中 b 和 b_index 的存储大小"""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(b), COUNT(b_index) FROM look_table;")
    b_count, b_index_count = cursor.fetchone()
    return b_count, b_index_count

def process_scalar_multiplication(h_values, r, a, p):
    """ 处理标量乘法的辅助函数 """
    results = []
    for h in h_values:
        results.append(scalar_multiply(r, h, a, p))
    return results

def process_bucket_scalar_results(bucket_index, values, results, a, p):
    """ 处理每个桶的标量乘法计算 """
    scalar_result_list = []
    for i, value in enumerate(values):
        result_h = results[i % len(results)]
        scalar_result = scalar_multiply(value, result_h, a, p)
        if scalar_result is not None:
            scalar_result_list.append(scalar_result)
    return bucket_index, scalar_result_list
def distribute_entries(entries, num_buckets):
    """ Distribute entries into buckets using linear probing for better uniformity """
    buckets = {i: [] for i in range(num_buckets)}  # 初始化桶
    bucket_counts = [0] * num_buckets  # 初始化桶的计数器

    for entry in entries:
        hash_value = sha1_hash(entry)
        bucket_index = hash_value % num_buckets  # 计算初始桶索引

        while len(buckets[bucket_index]) >= (len(entries) // num_buckets) + 1:
            bucket_index = (bucket_index + 1) % num_buckets  # 循环到下一个桶

        buckets[bucket_index].append(entry)
        bucket_counts[bucket_index] += 1  # 更新当前桶的计数

    max_count = max(bucket_counts)  # 计算最大桶容量
    return buckets, max_count

def main():
    existing_db_name = 'data.db'  # 假设已有数据库名为 data.db
    look_table_db_name = 'look_table.db'  # 创建的数据库名

    # 检查并删除现有的数据库文件
    if os.path.exists(look_table_db_name):
        os.remove(look_table_db_name)  # 删除已有的数据库文件
        print(f"Deleted existing database: {look_table_db_name}")

    num_buckets = 13  # 设置桶的个数

    # 从已有数据库中获取数据
    data_rows = fetch_data_from_existing_db(existing_db_name)

    # 将获取到的数据转化为适合分桶处理的格式
    entries = [{'index': row[0], 'value': row[1]} for row in data_rows]

    # 分桶处理并获取最大桶数量
    buckets, max_bucket_count = distribute_entries(entries, num_buckets)

    # 创建数据库并插入 h 值
    conn = create_new_database(look_table_db_name)
    create_tables(conn)  # 创建表
    insert_h_values(conn, max_bucket_count)  # 生成 h 值并插入数据库

    start = time.time()

    # 填充虚拟值
    fill_virtual_values(buckets, max_bucket_count)

    # 打印分桶情况
    for bucket_index, bucket in buckets.items():
        print(f"Bucket {bucket_index}: {[(entry['index'], entry['value']) for entry in bucket]}")

    # 创建 look_table 数据库和表，并插入数据
    create_lookup_table(conn, buckets)

    # 获取 h_value 表中的数据
    h_values = fetch_h_values(conn)

    # 生成随机值 r
    r = generate_random_value()

    # 计算每个 h 值与 r 的标量乘法，使用多线程
    curve = SECP256k1.curve
    a = curve.a()
    p = curve.p()

    with ThreadPoolExecutor() as executor:
        results = list(executor.submit(process_scalar_multiplication, h_values, r, a, p).result())

    # 获取 look_table 中每个桶的 value
    lookup_table_values = fetch_lookup_table_values(conn)

    # 对每个桶中的 value 和生成的结果进行标量乘法，使用多线程
    scalar_results = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_bucket_scalar_results, bucket_index, values, results, a, p): bucket_index
                   for bucket_index, values in lookup_table_values.items()}

        for future in futures.keys():
            bucket_index, scalar_result_list = future.result()
            scalar_results[bucket_index] = scalar_result_list

    # 对每个桶的所有 scalar_result 做椭圆曲线加法
    for bucket_index, scalar_result_list in scalar_results.items():
        if scalar_result_list:
            final_result = scalar_result_list[0]  # 初始化为第一个结果
            for scalar_result in scalar_result_list[1:]:
                final_result = add_points(final_result, scalar_result, a, p)
            print(f" bucket {bucket_index} digest: {final_result}")
        else:
            final_result = None

        # 将 final_result 的 x 和 y 存储到 buck_digest 表中
        cursor = conn.cursor()
        if final_result is None:
            cursor.execute('INSERT INTO buck_digest (b, x, y) VALUES (?, ?, ?)', (bucket_index, None, None))
        else:
            x_final, y_final = final_result
            cursor.execute('INSERT INTO buck_digest (b, x, y) VALUES (?, ?, ?)',
                           (bucket_index, str(x_final), str(y_final)))

    # 计算耗时
    elapsed_time = (time.time() - start) * 1000
    print(f"Time taken for computation (excluding file I/O): {elapsed_time:.4f} milliseconds")
    print(f"Generated random value r: {r}")

    # 计算并打印 look_table 表中 b 和 b_index 的大小
    b_count, b_index_count = calculate_look_table_b_size(conn)
    total_count = num_buckets + b_index_count
    print(f"Number of entries in 'b': {b_count}, Number of entries in 'b_index': {b_index_count}")
    print(f"The total count: {total_count}")
    # 提交事务
    conn.commit()

    print(f"Lookup table created in database '{look_table_db_name}' with bucket data.")
    # 在这里打印桶的最大容量
    print(f"Maximum bucket capacity: {max_bucket_count}")
    conn.close()

if __name__ == '__main__':
    main()