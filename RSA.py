from cryptography.fernet import Fernet

# 生成密钥client——DO
key = Fernet.generate_key()
with open("key","wb") as key_file:
    key_file.write(key)
print("生成的密钥：",key.decode())

# # 生成密钥DO-server部分
# 生成密钥
key = Fernet.generate_key()
with open("key_bucket","wb") as key_file:
    key_file.write(key)
print("生成的密钥：",key.decode())