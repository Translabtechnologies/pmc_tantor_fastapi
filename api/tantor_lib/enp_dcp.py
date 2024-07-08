from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

MY_KEY = b'01234567890123456789012345678901'


def decrypt(text, key):
    print("---------decrypt---------------------------")
    print("------------------------",text)
    iv, ciphertext = text.split(':')
    iv = bytes.fromhex(iv)
    ciphertext = bytes.fromhex(ciphertext)
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    decrypted = unpad(cipher.decrypt(ciphertext), AES.block_size)
    decrypted_text = decrypted.decode('utf-8')
    print("-----------after decrypt",decrypted_text )
    return decrypted_text

