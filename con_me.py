import json
import os
import sqlite3
from ecdsa import SECP256k1
import time


def load_total_result(filename='total_result_me.json'):
    """ 从 JSON 文件中加载 total_result 数据 """
    try:
        with open(filename, 'r') as json_file:
            data = json.load(json_file)
            return data.get('total_result_me')  # 返回 total_result 的值
    except FileNotFoundError:
        print(f"文件 {filename} 未找到。")
        return None
    except json.JSONDecodeError:
        print("JSON 解码错误，请检查文件格式。")
        return None
    except Exception as e:
        print(f"发生错误: {e}")
        return None


def load_results(filename='results.json'):
    """ 从 results.json 文件中加载结果数据 """
    try:
        with open(filename, 'r') as json_file:
            data = json.load(json_file)
            return data[0].get('result_with_t')  # 返回 result_with_t 的值
    except FileNotFoundError:
        print(f"文件 {filename} 未找到。")
        return None
    except json.JSONDecodeError:
        print("JSON 解码错误，请检查文件格式。")
        return None
    except Exception as e:
        print(f"发生错误: {e}")
        return None


def fetch_buck_digest_data(conn):
    """ 从 buck_digest 表中获取所有数据 """
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM buck_digest')
    buck_digest_data = cursor.fetchall()  # 获取所有行
    cursor.close()
    return buck_digest_data


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


def negate_point(point):
    """ 计算椭圆曲线点的负值 """
    if point is None:
        return None  # 无穷远点的负值仍然是无穷远点
    x, y = point
    return (x, -y % SECP256k1.curve.p())  # 计算负值



def main():

    m =91602926915902652544035644827613154392180534334876014906674056 # 定义 m 的值

    print(f"m = {m}")

    # 获取 total_result
    total_result = load_total_result()
    if total_result is not None:
        # print(f"server响应值 : {total_result}")
        # 获取 total_result 的 x 和 y 值
        total_x = total_result[0]  # 第一个数作为 x 值
        total_y = total_result[1]  # 第二个数作为 y 值
        total_point = (total_x, total_y)  # 总结果作为椭圆曲线上的点


    # 连接到 look_table 数据库以获取 buck_digest 数据
    look_table_db_name = 'look_table.db'  # 数据库名称

    try:
        conn_look_table = sqlite3.connect(look_table_db_name)

        # 获取 buck_digest 表中的所有数据
        buck_digest_data = fetch_buck_digest_data(conn_look_table)

        # 椭圆曲线参数
        curve = SECP256k1.curve
        p = curve.p()  # 获取有限域的素数 p
        a = curve.a()
        start = time.time()
        for row in buck_digest_data:
            # print(f"桶摘要: {row}")  # 打印桶摘要
            if row[1] is None:
                print("x=0")  # 如果 row 为 None，输出 x=0
                continue  # 跳过当前循环，继续下一次迭代
            # 假设 row 的格式为 (id, b_value, x, y)，获取 x 和 y 值
            x = int(row[1])  # 第1列为 x 值
            y = int(row[2])  # 第2列为 y 值
            point = (x, y)  # 椭圆曲线上的点

            # 计算标量乘法
            result = scalar_multiply(m, point, curve)
            # print(f" d^m : {result}")

            # 计算结果的负值
            negated_result = negate_point(result)
            # print(f"d^m的负值: {negated_result}")

            # 计算 negated_result 与 total_result 的加法
            summed_result = add_points(negated_result, total_point, a, p)
            # print(f"Summed result : {summed_result}")

            # 加载 results.json 数据
            results_data = load_results()
            # 计算文件的字节数并保存
            if results_data is not None:
                file_size = os.path.getsize('results.json')
            if results_data is not None:
                # 取出 result_with_t 的 x 和 y 值
                results_point = (results_data[0], results_data[1])  # 取出 x 和 y 值

                # 计算 total_point 的负值
                neg = negate_point(total_point)

                # 检查条件
                if neg == negated_result:
                    print("找到满足条件: x=0")
                    break  # 停止后续操作
                elif summed_result == results_point:
                    print("找到满足条件: x=1")
                    break  # 停止后续操作
                else:
                    print("evail")



    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        elapsed_time = (time.time() - start) * 1000
        print(f"Time taken for computation (excluding file I/O): {elapsed_time:.4f} seconds")
        if results_data is not None:
            print(f"server 的字节数: {file_size} bytes")  # 在最后打印字节数
        if conn_look_table:
            conn_look_table.close()  # 关闭 look_table 数据库连接

# 计算耗时


if __name__ == '__main__':
    main()