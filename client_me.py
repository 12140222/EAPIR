import random
import sqlite3
import secrets
import os  # 导入 os 模块以处理文件操作
from ecdsa import SECP256k1
from cryptography.fernet import Fernet
import base64  # 导入 base64 模块以进行编码
import time

def fetch_h_values_from_db(db_name):
    """ 从指定的数据库中获取 h_value 表的所有值 """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # 查询 h_value 表中的所有值
        cursor.execute('SELECT x, y FROM h_value')
        h_values = cursor.fetchall()  # 获取所有行

        return h_values
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        conn.close()  # 确保连接关闭

def generate_random_value(probability=1):

    curve = SECP256k1.curve
    p = curve.p()  # 获取有限域的素数 p
    p_length = len(str(p))  # p 的位数

    if random.random() < probability:
        smaller_length = (p_length * 4) // 5

        max_small_value = 10 ** smaller_length  # 计算最大值
        small_random_value = secrets.randbelow(max_small_value)  # 生成小随机值
        return small_random_value
    else:
        # 生成 [0, p) 范围内的随机数
        return secrets.randbelow(p)

def add_points(h1, h2, a, p):
    """ Add two points on the elliptic curve """
    if h1 is None or h2 is None:
        return None  # 如果其中一个点是无效的，返回 None

    x1, y1 = h1
    x2, y2 = h2

    if h1 == h2:  # Doubling case
        m = (3 * x1**2 + a) * mod_inverse(2 * y1, p) % p
    else:  # Addition case
        m = (y2 - y1) * mod_inverse(x2 - x1, p) % p

    x3 = (m**2 - x1 - x2) % p
    y3 = (m * (x1 - x3) - y1) % p

    return (x3, y3)

def mod_inverse(k, p):
    """ Calculate the modular inverse of k under modulo p """
    return pow(k, p - 2, p)  # Fermat's little theorem

def scalar_multiply(k, h, a, p):
    """ Perform scalar multiplication on an elliptic curve point """
    if k == 0:
        return None  # Point at infinity (无穷远点)
    if k == 1:
        return h  # Return the point itself

    R = None  # Initialize result as the point at infinity
    H = h  # Current point

    while k > 0:
        if k % 2 == 1:  # If the current bit is 1
            if R is None:
                R = H  # First addition
            else:
                R = add_points(R, H, a, p)
        H = add_points(H, H, a, p)  # Double the point
        k //= 2  # Shift right

    return R

def create_h_m_table(conn):
    """ 创建 h_m 数据库表 """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS h_m (
            id INTEGER PRIMARY KEY,
            x_result TEXT,
            y_result TEXT
        )
    ''')
    conn.commit()
    cursor.close()  # 关闭游标

def create_data_pk_table(conn):
    """ 创建 data_pk 数据库表 """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_pk (
            id INTEGER PRIMARY KEY,
            encrypted_data TEXT
        )
    ''')
    conn.commit()
    cursor.close()  # 关闭游标

def remove_existing_db(db_name):
    """ 检查并删除现有的数据库文件 """
    if os.path.exists(db_name):
        os.remove(db_name)

def main():
    db_name = 'look_table.db'  # 要连接的数据库名
    h_values = fetch_h_values_from_db(db_name)

    # 打印获取到的 h 值
    print("Fetched h_values from the database:")
    for x, y in h_values:
        print(f"x: {x}, y: {y}")


    # 生成椭圆曲线 P-256 上的随机值 m
    m = generate_random_value()
    # m = 23


    # 获取椭圆曲线的参数
    curve = SECP256k1.curve
    a = curve.a()
    p = curve.p()
    # 加载密钥
    with open("key", "rb") as key_file:
        key = key_file.read()
    cipher_suite = Fernet(key)

    # 加密数据
    num = 1  # 获取用户输入的整数
    data = str(num).encode()
    encrypted_data = cipher_suite.encrypt(data)

    # 将加密数据编码为 Base64 字符串
    encrypted_data_str = base64.urlsafe_b64encode(encrypted_data).decode('utf-8')


    # 检查并删除现有的 h_m.db 数据库
    h_m_db_name = 'h_m.db'
    remove_existing_db(h_m_db_name)



    # 创建连接，并创建 h_m 表
    conn = sqlite3.connect(h_m_db_name)
    create_h_m_table(conn)
    create_data_pk_table(conn)  # 创建 data_pk 表

    start = time.time()
    # 对每个 h 值与 m 进行标量乘法，并存储结果
    for index, (x, y) in enumerate(h_values):
        h_point = (int(x), int(y))  # 将字符串转为整数元组
        result = scalar_multiply(m, h_point, a, p)
        if result is None:
            print(f"Scalar multiplication of h ({x}, {y}) with m results in: None (Point at infinity)")
            x_result, y_result = None, None  # 处理无效结果
        else:
            x_result, y_result = result
            print(f" in: ({x_result}, {y_result})")

        # 将结果插入 h_m 表中，使用文本格式存储大整数

        cursor = conn.cursor()
        cursor.execute('INSERT INTO h_m (id, x_result, y_result) VALUES (?, ?, ?)', (index + 1, str(x_result), str(y_result)))  # index 从 1 开始
        cursor.close()  # 显式关闭游标

        # 计算耗时
    elapsed_time = (time.time() - start)*1000
    print(f"Time taken for computation (excluding file I/O): {elapsed_time:.4f} seconds")
    print(f" m : {m}")

    # 将加密数据插入 data_pk 表中
    cursor = conn.cursor()
    cursor.execute('INSERT INTO data_pk (id, encrypted_data) VALUES (?, ?)', (1, encrypted_data_str))  # 插入一条记录，id 从 1 开始
    cursor.close()  # 显式关闭游标

    # 提交事务并关闭连接
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()