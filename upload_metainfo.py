import socket
import threading
import struct

print("----- UPLOAD METAINFO -----")

HOST = '0.0.0.0'
PORT = 9999

FILE_NAME = 'metainfo.torrent'

BUFFER_SIZE = 4096

def load_file():
    try:
        with open(FILE_NAME, 'rb') as file:
            return file.read()
    except FileNotFoundError:
        print(f"File {FILE_NAME} not found!")
        return None

def send_msg(sock, msg):
    print(len(msg))
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)

def recv_msg(sock):
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    return recvall(sock, msglen)

def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

def handle_client(conn, addr, file_data):
    print(f"Connected to {addr}")
    try:
        send_msg(conn, file_data)
        print(f"File {FILE_NAME} sent to {addr}")
    except Exception as e:
        print(f"Error while sending data to {addr}: {e}")
    finally:
        conn.close()

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

file_data = load_file()

if file_data is None:
    print("Unable to load file. Exiting program.")
else:
    print(f"Server is listening on {PORT}...")
    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr, file_data))
        client_thread.start()
