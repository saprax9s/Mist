import socket, os, argparse, threading, zlib
from protocol import *

parser = argparse.ArgumentParser()
parser.add_argument("--id", required=True)
parser.add_argument("--active", default="true")
parser.add_argument("--corrupt", default="false")  # Flag to simulate corruption
args = parser.parse_args()

SENDER_ID = args.id
IS_ACTIVE = args.active.lower() == "true"
IS_CORRUPT = args.corrupt.lower() == "true"  # Boolean for corruption
INPUT_DIR = "input"

def listen_for_request():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', PORT))
        print(f"[Sender {SENDER_ID}] Listening for REQUEST on UDP {PORT}...")
    except OSError as e:
        print(f"[Sender {SENDER_ID}] UDP bind failed: {e}")
        return

    while True:
        data, addr = s.recvfrom(BUFFER_SIZE)
        try:
            msg, filename = data.decode().split(":")
            if msg == MSG_REQUEST:
                filepath = os.path.join(INPUT_DIR, filename)
                if not os.path.exists(filepath):
                    print(f"[Sender {SENDER_ID}] File '{filename}' not found.")
                    continue
                file_size = os.path.getsize(filepath)
                ack = f"{MSG_ACK}:{SENDER_ID}:{filename}:{file_size}"
                s.sendto(ack.encode(), addr)
                print(f"[Sender {SENDER_ID}] ACK sent for '{filename}'")
        except Exception as e:
            print(f"[Sender {SENDER_ID}] Error handling request: {e}")

def receive_assignment():
    port = PORT + int(SENDER_ID)
    try:
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', port))
        s.listen()
        print(f"[Sender {SENDER_ID}] Waiting for assignment on TCP {port}...")
    except OSError as e:
        print(f"[Sender {SENDER_ID}] TCP bind failed: {e}")
        return

    while True:
        conn, _ = s.accept()
        data = conn.recv(BUFFER_SIZE).decode()
        try:
            header, sender_id, filename, start, end, receiver_ip = data.split(":")
            if header == MSG_ASSIGN and sender_id == SENDER_ID:
                filepath = os.path.join(INPUT_DIR, filename)
                with open(filepath, "rb") as f:
                    f.seek(int(start))
                    chunk = f.read(int(end) - int(start))
                
                # Compute CRC-32 on the ORIGINAL chunk (before corruption)
                original_crc = hex(zlib.crc32(chunk) & 0xFFFFFFFF)[2:].zfill(8)
                
                # Simulate corruption if flag is set (after CRC computation)
                if IS_CORRUPT:
                    chunk = bytearray(chunk)  # Convert to mutable bytearray
                    for i in range(min(10, len(chunk))):  # Flip first 10 bytes (or fewer if chunk is small)
                        chunk[i] ^= 0xFF  # Bitwise NOT (flip all bits)
                    chunk = bytes(chunk)  # Convert back to bytes
                    print(f"[Sender {SENDER_ID}] Corrupting chunk (flipped first 10 bytes).")
                
                # Include ORIGINAL CRC in the message (format: MSG_CHUNK:sender_id:filename:start:end:crc:chunk_data)
                msg = f"{MSG_CHUNK}:{SENDER_ID}:{filename}:{start}:{end}:{original_crc}:".encode() + chunk
                out = socket.socket()
                out.connect((receiver_ip, PORT))
                out.send(msg)
                out.close()
                print(f"[Sender {SENDER_ID}] Chunk sent ({start}-{end}) with CRC {original_crc}")
        except Exception as e:
            print(f"[Sender {SENDER_ID}] Error sending chunk: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    if not IS_ACTIVE:
        print(f"[Sender {SENDER_ID}] Inactive. Exiting.")
        exit()

    threading.Thread(target=listen_for_request, daemon=True).start()
    threading.Thread(target=receive_assignment, daemon=True).start()

    # Keep main thread alive
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print(f"[Sender {SENDER_ID}] Shutting down.")