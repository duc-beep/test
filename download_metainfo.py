import socket
import struct

print("DOWNLOAD METAINFO FILE")
print("----------------------")

def send_msg(sock, msg):
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)

def recv_msg(sock):
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    print(msglen)
    return recvall(sock, msglen)

def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

HOST = input("Enter IP: ")
PORT = int(input("Enter Port: "))

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    file_data = recv_msg(s)
    if file_data:
        with open('metainfo.torrent', 'wb') as f:
            f.write(file_data)
        print("Successful! File 'metainfo.torrent' received.")
    else:
        print("No data received from the server.")

input()
