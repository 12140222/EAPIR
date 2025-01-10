import sqlite3
from cryptography.fernet import Fernet
from ecdsa import SECP256k1
import concurrent.futures
import json  # 导入 json 模块
import time


def fetch_query_data(conn):
    """ 从 query 表中获取所有数据 """
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM query')
    query_data = cursor.fetchall()  # 获取所有行
    cursor.close()
    return query_data


def fetch_do_pk_data(conn):
    """ 从 do_pk 表中获取所有加密数据 """
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM do_pk')
    do_pk_data = cursor.fetchall()  # 获取所有行
    cursor.close()
    return do_pk_data


def load_key_from_file(filename='key_bucket'):
    """ 从密钥文件中加载密钥 """
    with open(filename, 'rb') as key_file:
        return key_file.read()  # 返回密钥字节


def decrypt_data(cipher_suite, encrypted_data):
    """ 解密数据 """
    decrypted_data = cipher_suite.decrypt(encrypted_data)
    return decrypted_data.decode()


def fetch_look_table_values(conn, num):
    """ 从 look_table 表中获取 b = num 的所有 b_index 和 value 值 """
    cursor = conn.cursor()
    cursor.execute('SELECT b_index, value FROM look_table WHERE b = ?', (num,))
    values = cursor.fetchall()  # 获取所有匹配的记录
    cursor.close()
    return [(value[0], value[1]) for value in values]  # 返回 (b_index, value) 的元组列表


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
    return pow(k, p - 2, p)  # Fermat's little theorem


def scalar_multiply(k, h, curve):
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
                R = add_points(R, H, curve.a(), curve.p())
        H = add_points(H, H, curve.a(), curve.p())  # Double the point
        k //= 2  # Shift right

    return R

def fetch_data_from_db(query_db_name):
    """ 从数据库中获取数据并计算通信量 """
    conn = sqlite3.connect(query_db_name)
    cursor = conn.cursor()

    cursor.execute('SELECT x_result,y_result FROM h_m')  # 查询数据
    rows = cursor.fetchall()  # 获取所有行

    # 计算获取的数据大小，跳过包含 None 的行
    total_size = sum(
        len(str(row[0])) + len(str(row[1]))
        for row in rows if row[0] is not None and row[1] is not None )  # 总字节数
    conn.close()

    return total_size

def main():
    # 加载密钥
    key = load_key_from_file()
    cipher_suite = Fernet(key)

    # 椭圆曲线参数
    curve = SECP256k1.curve
    p = curve.p()  # 获取有限域的素数 p
    a = curve.a()

    # 连接到 query 数据库
    query_db_name = 'h_m.db'
    look_table_db_name = 'look_table.db'  # look_table 数据库名称
    total_size = fetch_data_from_db(query_db_name)
    print(f"查询query通信：{total_size}")
    # 用户输入线程数量
    max_workers = 8



    try:
        # 连接到 query 数据库
        conn_query = sqlite3.connect(query_db_name)
        # 连接到 look_table 数据库
        conn_look_table = sqlite3.connect(look_table_db_name)

        # 获取 query 表的数据·
        query_data = fetch_query_data(conn_query)

        # for row in query_data:
            # print(f"id: {row[0]},x_result: {row[1]},y_result: {row[2]}")  # 打印 x_result


        # 获取 do_pk 表的数据
        do_pk_data = fetch_do_pk_data(conn_query)

        start = time.time()
        for row in do_pk_data:
            # 解密数据
            encrypted_data = row[1].encode()  # 假设加密数据在第二列
            decrypted_data = decrypt_data(cipher_suite, encrypted_data)
            num = int(decrypted_data)


            look_table_values = fetch_look_table_values(conn_look_table, num)
            print(f"Values from look_table where b = {num}:")

            results = []  # 用于存储所有计算的结果

            # 使用 ThreadPoolExecutor 进行并行计算
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for b_index, value in look_table_values:

                    # print(f"b_index={b_index}, value={value}")  # 按照要求格式输出

                    # 进行椭圆曲线标量乘法
                    if b_index <= len(query_data):  # 确保有对应的 query 数据
                        query_value = int(query_data[b_index - 1][1])  # 获取对应的 query 值并转换为整数
                        y_result = int(query_data[b_index - 1][2])  # 获取对应的 y_result 值并转换为整数
                        point = (query_value, y_result)  # 椭圆曲线上的点
                        k = int(value)  # 将 value 作为标量并转换为整数

                        # 提交标量乘法任务
                        futures.append(executor.submit(scalar_multiply, k, point, curve))

                # 处理每个任务的结果
                total_result = None
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res is not None:  # 检查 res 是否为 None
                        if total_result is None:
                            total_result = res  # 第一个有效结果赋值给 total_result
                        else:
                            # 将 total_result 和 res 转换为整数进行加法操作
                            total_result = add_points(
                                (int(total_result[0]), int(total_result[1])),
                                (int(res[0]), int(res[1])),
                                a, p
                            )

                print(f"total_result: {total_result}")



                # 将最终结果写入 JSON 文件
                with open('total_result_me.json', 'w') as json_file:
                    json.dump({'total_result_me': total_result}, json_file)
                # 计算耗时
                elapsed_time = (time.time() - start) * 1000
                print(f"Time taken for computation (excluding file I/O): {elapsed_time:.4f} seconds")

            conn_look_table.close()  # 关闭 look_table 数据库连接

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn_query:
            conn_query.close()  # 关闭 query 数据库连接


if __name__ == '__main__':
    main()