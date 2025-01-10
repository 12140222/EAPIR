import sqlite3
import base64
import secrets
from ecdsa import SECP256k1
from cryptography.fernet import Fernet
import os
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor


def check_and_remove_db(db_name):
    """ 检查并删除现有的数据库文件 """
    if os.path.exists(db_name):
        os.remove(db_name)


def fetch_data_pk(conn):
    """ 从 data_pk 表中获取加密数据值 """
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM data_pk')
    return cursor.fetchall()


def fetch_h_m_values(conn):
    """ 从 h_m 表中获取所有值 """
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM h_m')
    return cursor.fetchall()


def fetch_look_table_entry(conn, num):
    """ 根据 num 从 look_table 中查找对应的记录 """
    cursor = conn.cursor()
    cursor.execute('SELECT b, b_index FROM look_table WHERE id = ?', (num,))
    return cursor.fetchone()


def generate_random_value(probability=0):
    curve = SECP256k1.curve
    p = curve.p()  # 获取有限域的素数 p
    p_length = len(str(p))  # p 的位数

    # 根据概率决定是否生成一个较小的随机值
    if random.random() < probability:

        smaller_length = p_length // 2

        max_small_value = 10 ** smaller_length  # 计算最大值
        small_random_value = secrets.randbelow(max_small_value)  # 生成小随机值
        return small_random_value
    else:
        # 生成 [0, p) 范围内的随机数
        return secrets.randbelow(p)


def mod_inverse(k, p):
    """ 计算 k 在模 p 下的逆 """
    return pow(k, p - 2, p)


def add_points(h1, h2):
    """ 添加两个椭圆曲线点 """
    x1, y1 = h1
    x2, y2 = h2
    p = SECP256k1.curve.p()
    a = SECP256k1.curve.a()

    if h1 == h2:  # 加倍情况
        m = (3 * x1 ** 2 + a) * mod_inverse(2 * y1, p) % p
    else:  # 加法情况
        m = (y2 - y1) * mod_inverse(x2 - x1, p) % p

    x3 = (m ** 2 - x1 - x2) % p
    y3 = (m * (x1 - x3) - y1) % p

    return (x3, y3)


def scalar_multiply(k, h):
    """ 对椭圆曲线点进行标量乘法 """
    if k == 0:
        return None
    if k == 1:
        return h

    R = None
    H = h

    while k > 0:
        if k % 2 == 1:
            if R is None:
                R = H
            else:
                R = add_points(R, H)
        H = add_points(H, H)
        k //= 2

    return R


def insert_results_to_db(cursor, results):
    """ 将计算结果批量插入数据库 """
    # 将结果转换为字符串形式以避免 OverflowError
    string_results = [(str(x_result), str(y_result)) for x_result, y_result in results]
    cursor.executemany('INSERT INTO query (x_result, y_result) VALUES (?, ?)', string_results)


def insert_encrypted_data_to_db(cursor, encrypted_data):
    """ 将加密的数据插入到 do_pk 表中 """
    cursor.execute('INSERT INTO do_pk (encrypted_data) VALUES (?)', (encrypted_data.decode(),))


def save_result_to_json(data, filename='results.json'):
    """ 将计算结果保存到 JSON 文件中 """
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def fetch_data_from_db(h_m_db_name):
    """ 从数据库中获取数据并计算通信量 """
    conn = sqlite3.connect(h_m_db_name)
    cursor = conn.cursor()

    cursor.execute('SELECT id,x_result,y_result FROM h_m')  # 查询数据
    rows = cursor.fetchall()  # 获取所有行

    # 计算获取的数据大小，跳过包含 None 的行
    total_size = sum(
        len(str(row[0])) + len(str(row[1])) + len(str(row[2]))
        for row in rows if row[0] is not None and row[1] is not None and row[2] is not None
    )  # 总字节数
    conn.close()

    return total_size

def main():
    check_and_remove_db('query.db')

    r = 86156499679442711534053242367151149324123977413554960226807478980590997969749
    print(f"Constant value r: {r}")

    h_m_db_name = 'h_m.db'
    look_table_db_name = 'look_table.db'

    total_size = fetch_data_from_db(h_m_db_name)
    print(f"查询client通信：{total_size}")
    # 连接数据库
    conn_h_m = sqlite3.connect(h_m_db_name)
    conn_look_table = sqlite3.connect(look_table_db_name)

    start = time.time()

    # 加载密钥并解密数据
    with open("key", "rb") as key_file:
        key = key_file.read()
    cipher_suite = Fernet(key)

    data_pk_values = fetch_data_pk(conn_h_m)

    results_for_json = []  # 用于保存结果以存入 JSON 文件
    results_to_insert = []  # 用于批量插入数据库

    for row in data_pk_values:
        encrypted_data = row[1]
        encrypted_data_bytes = base64.urlsafe_b64decode(encrypted_data)

        try:
            decrypted_data = cipher_suite.decrypt(encrypted_data_bytes)
            num = int(decrypted_data.decode())
            look_table_entry = fetch_look_table_entry(conn_look_table, num)

            if look_table_entry:
                b, b_index = look_table_entry
                print(f"b: {b}, b_index: {b_index}")

                # 生成椭圆曲线上的随机值 t
                t = generate_random_value()
                print(f"Random value t: {t}")

                sum_value = r + t
                h_m_values = fetch_h_m_values(conn_h_m)

                # 使用线程池来并行处理标量乘法
                with ThreadPoolExecutor(max_workers=8) as executor:
                    future_results = []
                    for i, row in enumerate(h_m_values):
                        x_result = int(row[1])
                        y_result = int(row[2])

                        if i + 1 == b_index:
                            future_results.append(executor.submit(scalar_multiply, sum_value, (x_result, y_result)))
                            result_t_future = executor.submit(scalar_multiply, t, (x_result, y_result))
                        else:
                            future_results.append(executor.submit(scalar_multiply, r, (x_result, y_result)))

                    for future in future_results:
                        result = future.result()
                        if result is not None:
                            results_to_insert.append(result)

                    # 处理 result_t
                    result_t = result_t_future.result()
                    results_for_json.append({"result_with_t": result_t})

        except Exception as e:
            print(f"Decryption failed: {e}")

        # 对server加密部分
        with open("key_bucket", "rb") as key_file:
            key_1 = key_file.read()
        cipher_suite = Fernet(key_1)

        # 加密数据并插入数据库
        num = b  # 获取用户输入的整数
        data = str(num).encode()
        encrypted_data = cipher_suite.encrypt(data)

        # 计算耗时
        elapsed_time = (time.time() - start) * 1000

    # 批量插入结果
    with conn_h_m:
        cursor = conn_h_m.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS query (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                x_result TEXT,
                y_result TEXT
            )
        ''')
        insert_results_to_db(cursor, results_to_insert)

    # 保存 JSON 文件
    save_result_to_json(results_for_json)

    # 将加密后的数据存入 do_pk 表
    with conn_h_m:
        cursor = conn_h_m.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS do_pk (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                encrypted_data TEXT
            )
        ''')
        insert_encrypted_data_to_db(cursor, encrypted_data)

    print(f"Time taken for computation (excluding file I/O): {elapsed_time:.4f} seconds")
    # 关闭数据库连接
    conn_h_m.close()
    conn_look_table.close()




if __name__ == '__main__':
    main()