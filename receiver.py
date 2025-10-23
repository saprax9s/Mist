import socket, time, os, zlib
from protocol import *
from logger import log, print_audit_log  # ✅ Logging module with audit

RECEIVER_IP = socket.gethostbyname(socket.gethostname())
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

chunks = {}

def discover_senders(filename, timeout=5):
    broadcast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast.settimeout(timeout)

    log(f"Broadcasting request for '{filename}'...")
    broadcast.sendto(f"{MSG_REQUEST}:{filename}".encode(), ('<broadcast>', PORT))

    senders = []
    start = time.time()
    while time.time() - start < timeout:
        try:
            data, addr = broadcast.recvfrom(BUFFER_SIZE)
            parts = data.decode().split(":")
            if parts[0] == MSG_ACK:
                sender_id, fname, file_size = parts[1:]
                senders.append({
                    "id": int(sender_id),
                    "ip": addr[0],
                    "filename": fname,
                    "file_size": int(file_size)
                })
        except socket.timeout:
            break

    log(f"Found {len(senders)} sender(s).")
    return senders

def assign_chunks(senders):
    file_size = senders[0]["file_size"]
    chunk_count = len(senders)
    chunk_size = file_size // chunk_count

    assignments = []
    for i, sender in enumerate(senders):
        start = i * chunk_size
        end = file_size if i == chunk_count - 1 else (i + 1) * chunk_size
        assignments.append({
            "sender_id": str(sender["id"]),
            "ip": sender["ip"],
            "filename": sender["filename"],
            "start": str(start),
            "end": str(end)
        })
    return assignments

def send_assignments(assignments):
    for a in assignments:
        msg = f"{MSG_ASSIGN}:{a['sender_id']}:{a['filename']}:{a['start']}:{a['end']}:{RECEIVER_IP}"
        try:
            s = socket.socket()
            s.connect((a["ip"], PORT + int(a["sender_id"])))
            s.send(msg.encode())
            s.close()
            log(f"Assignment sent to sender {a['sender_id']}")
        except Exception as e:
            log(f"Failed to connect to sender {a['sender_id']}: {e}")

def receive_chunks(expected_count):
    s = socket.socket()
    s.bind((RECEIVER_IP, PORT))
    s.listen()
    log("Listening for chunks...")

    received = 0
    valid_chunks = {}  # Store only valid chunks
    bar_width = 30

    def show_progress(received, total):
        filled = int((received / total) * bar_width)
        bar = "[" + "#" * filled + "-" * (bar_width - filled) + "]"
        print(f"\r[Receiver] Progress: {bar} {received}/{total} chunks received", end="", flush=True)

    while received < expected_count:
        conn, _ = s.accept()
        data = b""
        while True:
            part = conn.recv(BUFFER_SIZE)
            if not part: break
            data += part
        conn.close()

        # Split into header and chunk data (now 6 parts before chunk)
        header_parts = data.split(b":", 6)  # Split on first 6 ':' to separate header from chunk
        if len(header_parts) < 7:  # Expect 6 header parts + chunk
            log("Malformed chunk received.")
            continue

        sender_id = int(header_parts[1].decode())
        filename = header_parts[2].decode()
        start = header_parts[3].decode()
        end = header_parts[4].decode()
        sent_crc = header_parts[5].decode()
        chunk = header_parts[6]  # The actual chunk data

        # Compute CRC-32 on received chunk
        computed_crc = hex(zlib.crc32(chunk) & 0xFFFFFFFF)[2:].zfill(8)
        
        if computed_crc == sent_crc:
            valid_chunks[sender_id] = chunk
            log(f"Chunk from sender {sender_id} verified (CRC match).")
        else:
            log(f"Chunk from sender {sender_id} corrupted (CRC mismatch: sent {sent_crc}, computed {computed_crc}). Discarding.")

        received += 1
        show_progress(received, expected_count)

    print()  # Move to next line after progress bar

    # Check if we have all valid chunks
    if len(valid_chunks) == expected_count:
        global chunks  # Update the global chunks dict with valid ones
        chunks = valid_chunks
        log("All chunks verified successfully.")
        return True
    else:
        log(f"Only {len(valid_chunks)}/{expected_count} chunks are valid. Aborting assembly.")
        return False

def assemble_file(filename):
    output_path = os.path.join(OUTPUT_DIR, filename)
    with open(output_path, "wb") as f:
        for i in sorted(chunks):
            f.write(chunks[i])
    log(f"File assembled → {output_path}")

if __name__ == "__main__":
    filename = input("Enter filename to request: ")
    senders = discover_senders(filename)
    if not senders:
        log("No senders found. Exiting.")
        print_audit_log()
        exit()

    assignments = assign_chunks(senders)
    send_assignments(assignments)
    success = receive_chunks(len(senders))
    if success:
        assemble_file(filename)
    else:
        output_path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(output_path):
            os.remove(output_path)
        log("File corrupted, discarding, exiting.")
        print_audit_log()
        exit()
    
    print_audit_log()  # Print audit at the end if successful