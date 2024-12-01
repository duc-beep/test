import socket
import threading
import time
import bencodepy 
import os
import sys
import hashlib
import signal
import logging
import queue
import json
import keyboard
import struct
import random
from concurrent.futures import ThreadPoolExecutor


##
#with open('config.json', 'r') as config_file:
    #config = json.load(config_file)

#THIS_IP = config['this_ip']
#THIS_PORT = config['this_port']
##METAINFO_PATH = config['metainfo_path']
#TRACKER_URL = config['tracker_url']
#BUFFER_SIZE = config['buffer_size']
#MAX_THREADS_LISTENER = config['max_threads']
#

THIS_IP = '0.0.0.0'
THIS_PORT = 8888
METAINFO_PATH = 'metainfo.torrent'
TRACKER_URL = ''
BUFFER_SIZE = 512 * 1024
BUFFER_SIZE_HASH_DICT = 1024 * 1024
MAX_THREADS_LISTENER = 100

#lock
pieces_downloaded_count_lock = threading.Lock()
hash_dict_lock = threading.Lock()
hash_dict_vailable_count_lock = threading.Lock()
peer_dict_lock = threading.Lock()
download_rate_dict_lock = threading.Lock()

file_lock = threading.Lock()

lock = threading.Lock()

#Global
PROGRAM_IS_RUNNING = True
PIECES_COUNT = 0
PIECES_DOWNLOADED_COUNT = 0 

HASH_DICT = {} 
HASH_DICT_AVAILABLE_COUNT = {} 
PEER_DICT = {}

CHOKED_QUEUE = queue.Queue()
UNCHOKE = []
DOWNLOAD_RATE_DICT = {}

def load_file(file_path):
    try:
        with file_lock:
            with open(file_path, 'rb') as file:
                return file.read()  # Read the entire file into memory
    except FileNotFoundError:
        print(f"File {file_path} not found!")
        return None

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

###############################################
## furthur function
def check_for_exit():
    global PROGRAM_IS_RUNNING
    while True:
        if keyboard.is_pressed('x'): 
            print("Exiting network...")
            PROGRAM_IS_RUNNING = False
            unregister()
            time.sleep(15)
            break

def check_existing_pieces():
    global PIECES_DOWNLOADED_COUNT
    existing_pieces = os.listdir('list_pieces')
    for file in existing_pieces:
        if file.endswith('.bin'):
            piece_hash = file[:-4]
            HASH_DICT[piece_hash] = 1
            PIECES_DOWNLOADED_COUNT += 1

def update_downloaded_count_and_print():
    global PIECES_DOWNLOADED_COUNT
    with pieces_downloaded_count_lock:
        PIECES_DOWNLOADED_COUNT += 1
        percent_completed = (PIECES_DOWNLOADED_COUNT / PIECES_COUNT) * 100
        print(f"Downloading... {percent_completed:.2f}%")
       
# Read info from metainfo.torrent
def read_torrent_file(torrent_file_path):
    try:
        if not os.path.exists(torrent_file_path):
            print(f"File {torrent_file_path} not found.")
            sys.exit()

        start_time = time.time()
        while True:
            try:
                with open(torrent_file_path, 'rb') as torrent_file:
                    torrent_data = bencodepy.decode(torrent_file.read())
                break
            except IOError:
                if time.time() - start_time > 3:
                    raise TimeoutError(f"File {torrent_file_path} is still in use after {3} seconds.")
                time.sleep(0.5)  # Chờ một lát trước khi thử lại
            

        tracker_URL = torrent_data.get(b'announce', b'').decode()  # x.x.x.x:y
        info = torrent_data.get(b'info')
        file_name = info.get(b'name')
        piece_length = info.get(b'piece length', 0)  # 512KB
        pieces = info.get(b'pieces')  # list hash       
        file_length = info.get(b'length')
        pieces_count = len(pieces)
        # default bitfield 0 indicate client has not had this piece 
        hash_dict = {piece_hash.decode(): 0 for piece_hash in pieces.keys()} 
    except Exception as e:
        print(f"Error when dealing with torrent file: {e}")
    return hash_dict, tracker_URL, file_name, piece_length, pieces, file_length, pieces_count        

###############################################
## connect tracker
def upload_hash_list_TO_TRACKER(tracker_socket):
    try:
        response = tracker_socket.recv(BUFFER_SIZE).decode()
        if response == "REQUEST_HASH_LIST":
            with hash_dict_lock:
                list_piece = [piece for piece, bitfield in HASH_DICT.items() if bitfield == 1]
                if list_piece:
                    data = json.dumps(list_piece).encode()
                else:
                    data = "BLANK".encode()
                send_msg(tracker_socket, data)
        else:
            print(f"Unexpected response from tracker while upload hash_dict: {response}")
    except Exception as e:
        print(f"Unexpected error while upload hash_dict to tracker: {e}")

def update_new_piece_TO_TRACKER(client_ip, client_port, piece):
    tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        tracker_socket.connect((TRACKER_URL.split(':')[0], int(TRACKER_URL.split(':')[1])))
        tracker_socket.send(f"UPDATE_PIECE {THIS_IP} {THIS_PORT} {piece}".encode())

    except socket.timeout:
        print(f"Timeout error while connecting to tracker {TRACKER_URL}")
        return None
    except socket.error as e:
        print(f"Socket error occurred during registration: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error during registration: {e}")
        return None
    finally:
        tracker_socket.close()

def register_peer():
    global PEER_DICT, DOWNLOAD_RATE_DICT
    tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        tracker_socket.connect((TRACKER_URL.split(':')[0], int(TRACKER_URL.split(':')[1])))
        tracker_socket.send(f"REGISTER {THIS_IP} {THIS_PORT}".encode())
        ADDR = tracker_socket.recv(BUFFER_SIZE).decode()
        print(ADDR.split(":")[0])
        tracker_socket.send("DONE".encode())
        upload_hash_list_TO_TRACKER(tracker_socket)
        response = tracker_socket.recv(BUFFER_SIZE).decode()
        if response == "DONE":
            tracker_socket.send("GET_PEERS_DICT".encode())
        else:
            print("Error: Unexpected response from Tracker while GET_PEERS_DICT")
        dict = recv_msg(tracker_socket)
        if dict == "BANK":
            return ADDR
        else:
            with peer_dict_lock:
                PEER_DICT = json.loads(dict)
                with download_rate_dict_lock:
                    DOWNLOAD_RATE_DICT = {peer: 0 for peer in PEER_DICT.keys()}
        return ADDR

    except socket.timeout:
        print(f"Timeout error while connecting to tracker {TRACKER_URL}")
        return None
    except socket.error as e:
        print(f"Socket error occurred during registration: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error during registration: {e}")
        return None
    finally:
        tracker_socket.close()

def unregister():

    tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        tracker_socket.connect((TRACKER_URL.split(':')[0], int(TRACKER_URL.split(':')[1])))
        tracker_socket.send(f"UNREGISTER {THIS_IP} {THIS_PORT}".encode())
        response = tracker_socket.recv(BUFFER_SIZE).decode()
        print("Unregistered.")
    except socket.timeout:
        print(f"Timeout error while unregistering from tracker {TRACKER_URL}")
    except socket.error as e:
        print(f"Socket error occurred while unregistering: {e}")
    except Exception as e:
        print(f"Unexpected error during unregistration: {e}")
    finally:
        time.sleep(2)
        tracker_socket.close()

###############################################
## handle connect from leecher
def unchoke_for_peer(client_ip, client_socket):
    try:  
        while True:  
            if client_ip in UNCHOKE:
                client_socket.send("UNCHOKED".encode())
                break
    except socket.error as e:
        print(f"Socket error during communication with peer: {e}")
    except Exception as e:
        print(f"Unexpected error while handling leecher 2: {e}")
        
def handle_leecher(client_socket, peer_ip):
    try:
        while PROGRAM_IS_RUNNING:
            if peer_ip not in UNCHOKE:
                client_socket.send("CHOKED".encode())
            #another thread send unchoke, then peer reach, then peer start request
            request = client_socket.recv(BUFFER_SIZE).decode()
            if not request:
                break
            if request.startswith("REQUEST_PIECE"):
                piece_hash = request.split()[1]
                piece_file_path = f'list_pieces/{piece_hash}.bin'
                file_data = load_file(piece_file_path)
                total_sent = len(file_data) 
                try:
                    send_msg(client_socket, file_data)
                    #print(f"Sent {total_sent} bytes of piece {piece_hash} to {client_socket.getpeername()}")
                except Exception as e:
                    print(f"Unexpected error while sending data: {e}")
    except socket.error as e:
        print(f"Socket error during communication with peer: {e}")
    except Exception as e:
        print(f"Unexpected error while handling leecher: {e}")
    finally:
        client_socket.close()

def this_client_is_listening():
    with ThreadPoolExecutor(max_workers=MAX_THREADS_LISTENER) as listener:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((THIS_IP, THIS_PORT))
        server.listen(MAX_THREADS_LISTENER)
        server.settimeout(10)
        active_clients = []
        print(f"You are listening on {THIS_IP}:{THIS_PORT}.")
        try:
            while PROGRAM_IS_RUNNING:
                try:
                    client_sock, addr = server.accept()
                    active_clients.append(client_sock)
                    print(f"Connection from {addr}")
                    choke_algorithm(addr[0])
                    listener.submit(handle_leecher,client_sock, addr[0])
                    listener.submit(unchoke_for_peer, addr[0], client_sock)
                    listener.submit(unchoke_periodly)
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"Error while accepting connection: {e}")
                    break
        finally:
            print(f"You stop listening on {THIS_IP}:{THIS_PORT}.")
            for client_sock in active_clients:
                try:
                    client_sock.close()
                except Exception as e:
                    print(f"Error closing client socket: {e}")        

def choke_algorithm(peer_ip):
    global DOWNLOAD_RATE_DICT, CHOKED_QUEUE, UNCHOKE

    with lock:
        CHOKED_QUEUE.put(peer_ip)
        # Sort DOWNLOAD_RATE_DICT theo downloading rate 
        sorted_ips = sorted(DOWNLOAD_RATE_DICT.items(), key=lambda x: x[1], reverse=True)
        # Chọn 2 IP có tốc độ cao nhất
        unchoke_ips = [ip for ip, _ in sorted_ips[:2]]
        # Xóa các IP được unchoke ra khỏi hàng đợi CHOKED_QUEUE
        for ip in unchoke_ips:
            if ip in list(CHOKED_QUEUE.queue):
                CHOKED_QUEUE.queue.remove(ip)
        # Chọn thêm 1 IP từ hàng đợi CHOKED_QUEUE (theo FIFO)
        if not CHOKED_QUEUE.empty():
            extra_ip = CHOKED_QUEUE.get()
            unchoke_ips.append(extra_ip)
        # Cập nhật danh sách UNCHOKE
        UNCHOKE = unchoke_ips  

def unchoke_periodly():
    while PROGRAM_IS_RUNNING:
        time.sleep(30)
        with lock:
            if not CHOKED_QUEUE.empty():
                UNCHOKE.append(CHOKED_QUEUE.get())

###############################################
## connect seeder
def start_download_piece(seeder_socket, piece, seeder_ip, seeder_port):
    try:
        request = f"REQUEST_PIECE {piece}".encode()
        seeder_socket.send(request)

        # Tính toán tốc độ tải xuống
        total_bytes_downloaded = 0
        starting_time = time.time()
        #download
        file_data = recv_msg(seeder_socket)
        if file_data:
            total_bytes_downloaded = len(file_data)
            with file_lock:
                with open(f'list_pieces/{piece}.bin', 'wb') as f:
                    f.write(file_data)
        else:
            print("No data received from the server.")
              
        #cal & update speed
        elapsed_time = time.time() - starting_time
        if elapsed_time > 0:
            download_speed = total_bytes_downloaded / elapsed_time  # Tính tốc độ tải xuống (bytes/s)
        else:
            download_speed = 0
        
        with lock :
            if seeder_ip not in DOWNLOAD_RATE_DICT:
                DOWNLOAD_RATE_DICT[seeder_ip] = download_speed
            else:
                DOWNLOAD_RATE_DICT[seeder_ip] = (DOWNLOAD_RATE_DICT[seeder_ip] + download_speed) / 2

        #check hash
            try:
                piece_path = f'list_pieces/{piece}.bin'
                piece_ = load_file(piece_path)
                piece_hash_test = hashlib.sha1(piece_).hexdigest()
                if piece_hash_test == piece:
                    print(f"Received piece {piece} from {seeder_ip}:{seeder_port}")
                    with hash_dict_lock:
                        HASH_DICT[piece] = 1
                    update_downloaded_count_and_print()
                    update_new_piece_TO_TRACKER(THIS_IP, THIS_PORT, piece)
                else:
                    print(f"Error! Expected: {piece}, but received: {piece_hash_test}")
                    os.remove(f'list_pieces/{piece}.bin')
                    with hash_dict_lock:
                        HASH_DICT[piece] = 0     
            except Exception as e: 
                print(f"Unexpected Error in checking hash: {e}")
    except socket.error as e:
        print(f"Disconnection with {seeder_ip}:{seeder_port}: {e}")
        if os.path.exists(f'list_pieces/{piece}.bin'):
            os.remove(f'list_pieces/{piece}.bin')
        with hash_dict_lock:
            HASH_DICT[piece] = 0
    except Exception as e:
        print(f"Unexpected Error during receive data for piece {piece}: {e}")
        if os.path.exists(f'list_pieces/{piece}.bin'):
            os.remove(f'list_pieces/{piece}.bin')
        with hash_dict_lock:
            HASH_DICT[piece] = 0
           
def request_pieces_from_peer(peer_addr, list_pieces):
    start_time = time.time()
    peer_info = peer_addr.split(':')
    peer_ip = peer_info[0]
    peer_port = int(peer_info[1])

    request_pieces_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        request_pieces_socket.connect((peer_ip, peer_port))
        command = request_pieces_socket.recv(BUFFER_SIZE).decode()
        if command == "CHOKED":
            print("no choked")
            print(f"CHOKED by {peer_ip}:{peer_port}")
            command = request_pieces_socket.recv(BUFFER_SIZE)
        if command == "UNCHOKED":   
            downloaded_pieces = []
            for piece in list_pieces:
                start_download_piece(request_pieces_socket, piece, peer_ip, peer_port)
                downloaded_pieces.append(piece)
                interval = time.time() - start_time
                if interval > 5:
                    for remaining_piece in list_pieces:
                        if remaining_piece not in downloaded_pieces:
                            with hash_dict_lock:
                                HASH_DICT[remaining_piece] = 0
                    break
    
    except socket.error as e:
        print(f"Disconnection with 2 {peer_ip}:{peer_port}: {e}")
    except Exception as e:
        print(f"Unexpected error while requesting pieces from peer {peer_ip}:{peer_port}: {e}")
    finally:
        request_pieces_socket.close()

###############################################
# distrubute #run
def distribute_request_to_threads():
    global PEER_DICT,HASH_DICT
    while PROGRAM_IS_RUNNING:
            if PIECES_COUNT == PIECES_DOWNLOADED_COUNT:
                print("Finish downloading!")
                break

            pieces_to_request = []
            # Tạo danh sách các pieces cần request (bit_field = 0)
            with hash_dict_lock:
                hash_dict_items = list(HASH_DICT.items())

            random.shuffle(hash_dict_items)
            
            for piece, bit_field in hash_dict_items:
                if bit_field == 0:  # Only request pieces that are not downloaded
                    # Based on PEER_DICT, get peers that have this piece
                    with peer_dict_lock:
                        peer_list = []
                        this_info = f"{THIS_IP}:{THIS_PORT}"
                        try:
                            if PEER_DICT:
                                # Filter peers that have the current piece
                                peer_list = [peer for peer, pieces in PEER_DICT.items() if piece in pieces]
                            if this_info in peer_list:
                                peer_list.remove(this_info)  # Remove own peer from the list
                        except Exception as e:
                            print(f"Error filtering peers: {e}")

                        # Sort peer list by download rate, highest first
                        with download_rate_dict_lock:
                            peer_list_sorted = sorted(peer_list, key=lambda x: DOWNLOAD_RATE_DICT.get(x, 0), reverse=True)

                    # Choose the peer with the random top 2 highest download speed
                    if peer_list_sorted:
                        with hash_dict_lock:
                            HASH_DICT[piece] = -1  # Mark as downloading
                        top_peers = random.sample(peer_list_sorted, min(2, len(peer_list_sorted)))                
                        best_peer = top_peers[0] 
                        pieces_to_request.append((best_peer, piece))
            
            # Tạo dict mới với key là peer và value là list các pieces cần request
            request_dict = {}
            for peer, piece in pieces_to_request:
                if peer not in request_dict:
                    request_dict[peer] = []
                request_dict[peer].append(piece)
            # Chia công việc cho mỗi peer thành các thread
            threads = []
            for peer, pieces in request_dict.items():
                thread = threading.Thread(target=request_pieces_from_peer, args=(peer, pieces))
                threads.append(thread)
                thread.start()

            # Đợi tất cả threads hoàn thành
            for thread in threads:
                print("check")
                thread.join()
            
            if PIECES_COUNT == PIECES_DOWNLOADED_COUNT:
                print("Finish downloading!")
                break

            time.sleep(1)
            tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                tracker_socket.connect((TRACKER_URL.split(':')[0], int(TRACKER_URL.split(':')[1])))
                tracker_socket.send("GET_PEERS_DICT".encode())
                dict = recv_msg(tracker_socket)
                if dict == "BANK":
                    continue
                else:
                    with peer_dict_lock:
                        PEER_DICT = json.loads(dict)
            except socket.timeout:
                print(f"Timeout error while connecting to tracker 2 {TRACKER_URL}")
            except socket.error as e:
                print(f"Socket error occurred during get peer dict: {e}")
            except json.JSONDecodeError as e:
                print (dict)
                print(f"Failed to parse JSON data: {e}")
            except Exception as e:
                print(f"Unexpected error during get peer dict: {e}")
            finally:
                tracker_socket.close()

def run():
    distribute_request_to_threads()

if __name__ == '__main__':

    HASH_DICT, TRACKER_URL, file_name, piece_length, pieces, file_length, PIECES_COUNT = read_torrent_file(METAINFO_PATH)
    HASH_DICT_AVAILABLE_COUNT = HASH_DICT.copy()
    if not os.path.exists('list_pieces'):
        os.makedirs('list_pieces')
    check_existing_pieces()
    
    response = register_peer()
    if response == None:
        print("None")
        exit()
    
    ip_info = response.split(":")
    THIS_IP = ip_info[1]
    
 #   threading.Thread(target=check_for_exit, daemon=True).start()

    with ThreadPoolExecutor(max_workers=2) as ex:
        ex.submit(this_client_is_listening) 
        ex.submit(run)
    
    while True:
        pass
    