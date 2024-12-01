import socket
import threading
import json
import struct
from concurrent.futures import ThreadPoolExecutor

BUFFER_SIZE = 1024
BUFFER_SIZE_HASH_DICT = 1024 * 1024
peers = []
DATA = {}

data_lock = threading.Lock()

def send_msg(sock, msg):
    # Preprocess: send the length of the message first, followed by the message itself
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)

# Function to receive a message from the socket
def recv_msg(sock):
    # Read the message length
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the actual message data based on the length received
    return recvall(sock, msglen)

# Helper function to receive n bytes or return None if EOF is reached
def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

#########################

def handle_register(client_ip, client_port, client_socket):
    peer_info = f"{client_ip}:{client_port}"
    if peer_info not in peers:
        peers.append(peer_info)
        client_socket.send(f"Registered successfully.:{client_ip}:{client_port}".encode())
    else:
        client_socket.send(f"You have already registered.:{client_ip}:{client_port}".encode())

def request_hash_list(client_ip, client_port, client_socket):
    client_addr = f"{client_ip}:{client_port}"
    client_socket.send("REQUEST_HASH_LIST".encode())
    data = recv_msg(client_socket).decode()
    if data == "BLANK":
        DATA[client_addr] = {}
    else:
        try:
            list_piece = json.loads(data)         
            with data_lock:
                DATA[client_addr] = list_piece
        except json.JSONDecodeError as e:
            print (data)
            print(f"Failed to parse JSON data: {e}")
    client_socket.send("DONE".encode())

def handle_update_piece(client_ip, client_port, hash_value, flag):

    client_addr = f"{client_ip}:{client_port}"
    with data_lock:
        i = 0
        for hash in DATA[client_addr]:
            if hash == hash_value:
                DATA[client_addr][i] = flag
            i = i+1

def handle_get_list_peer(client_socket):
    with data_lock:
        if DATA:
            data = json.dumps(DATA).encode()
            send_msg(client_socket, data)
        else:
            bank_data = "BLANK".encode()
            client_socket.send(bank_data)

def handle_unregister(client_ip, client_port, client_socket):
    client_addr = f"{client_ip}:{client_port}"
    if client_addr in DATA:
        with data_lock:
            DATA.pop(client_addr, None)
        client_socket.send(b"Unregistered successfully.")
    else:
        client_socket.send(b"You were not registered.")
    client_socket.close()

def handle_client(client_socket, addr, peers):
    try:
        while True:
            request = client_socket.recv(1024).decode()
            if not request:
                break

            command, *args = request.split()
            print(f"Command received: {command}")

            if command == "REGISTER":
                handle_register(addr[0], int(args[1]), client_socket)
                response = client_socket.recv(BUFFER_SIZE).decode()
                if response == "DONE":
                    request_hash_list(addr[0], int(args[1]), client_socket)
                else:
                    print("Unexpected response from {addr} while handle registing!")
    

            elif command == "UPDATE_PIECE":
                handle_update_piece(addr[0], int(args[1]), args[2], 1)

            elif command == "GET_PEERS_DICT":
                handle_get_list_peer(client_socket)

            elif command == "UNREGISTER":
                handle_unregister(addr[0], int(args[1]), client_socket)
                break
    except Exception as e:
        print(f"Unexpected error with {addr}: {e}")
    finally:
        client_socket.close()

def start_tracker(host='0.0.0.0', port=5000):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(100)
    print(f"Tracker running on {host}:{port}")
    
    with ThreadPoolExecutor(max_workers=100) as executor:
        while True:
            client_sock, addr = server.accept()
            print(f"Connection from {addr}")
            executor.submit(handle_client, client_sock, addr, peers)

if __name__ == '__main__':
    start_tracker()
