"""MIST sender: wire-compatible streaming service and original terminal CLI."""
import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import os
from pathlib import Path
import signal
import socket
import stat
import threading
import time
import uuid
import zlib

from protocol import PORT, BUFFER_SIZE, MSG_REQUEST, MSG_ACK, MSG_ASSIGN, MSG_CHUNK
from logger import log, configure

BLOCK_SIZE = 64 * 1024


def validate_id(value):
    value = str(value)
    if not value.isascii() or not value.isdecimal() or not 0 <= int(value) <= 65535 - PORT:
        raise ValueError('Sender ID must be an integer from 0 to 60535')
    return str(int(value))


def validate_filename(name):
    if (not isinstance(name, str) or not name or len(name.encode('utf-8')) > 255
            or name in ('.', '..') or any(c in name for c in '/\\:')
            or any(ord(c) < 32 or ord(c) == 127 for c in name)):
        raise ValueError('Filename must be a plain filename without separators or control characters')
    return name


class SenderService:
    def __init__(self, sender_id, input_dir='input', corrupt=False, workers=4,
                 timeout=3.0, retries=5, backoff=0.15):
        self.sender_id = validate_id(sender_id)
        self.root = Path(input_dir).expanduser().resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError('Input path must be a directory')
        if not 1 <= workers <= 16 or not 1 <= retries <= 10 or timeout <= 0 or backoff < 0:
            raise ValueError('Invalid service limits')
        self.corrupt, self.workers = bool(corrupt), workers
        self.timeout, self.retries, self.backoff = timeout, retries, backoff
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.slots = threading.BoundedSemaphore(workers)
        self.sockets = set()
        self.threads = []
        self.pool = None
        self.root_fd = None
        self.state = 'stopped'
        self.error = None
        self.active = {}
        self.history = deque(maxlen=500)
        self.samples = deque(maxlen=240)
        self.peers = deque(maxlen=32)
        self.total_bytes = 0
        self.failures = 0

    def event(self, message, level='INFO', **fields):
        return log(message, level, sender_id=self.sender_id, **fields)

    def open_file(self, name):
        validate_filename(name)
        # Anchor the open to a directory descriptor; reject symlinks and nonregular files.
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY) if self.root_fd is None else os.dup(self.root_fd)
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        finally:
            os.close(directory)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise ValueError('Only regular files are eligible')
        return os.fdopen(fd, 'rb')

    def files(self):
        result = []
        for entry in sorted(self.root.iterdir(), key=lambda p: p.name.lower()):
            try:
                with self.open_file(entry.name) as source:
                    result.append({'name': entry.name, 'size': os.fstat(source.fileno()).st_size})
            except (OSError, ValueError):
                continue
        return result

    def _track(self, sock):
        with self.lock:
            if self.stop_event.is_set():
                sock.close()
                raise InterruptedError('Sender is stopping')
            self.sockets.add(sock)
        return sock

    def _close(self, sock):
        with self.lock:
            self.sockets.discard(sock)
        sock.close()

    def start(self):
        with self.lock:
            if self.state != 'stopped':
                raise ValueError('Create a new service after stopping or an error')
            try:
                self.root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
                udp = self._track(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                udp.bind(('', PORT))
                udp.settimeout(0.25)
                tcp = self._track(socket.socket())
                tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                tcp.bind(('', PORT + int(self.sender_id)))
                tcp.listen(self.workers)
                tcp.settimeout(0.25)
                self.pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix='mist-transfer')
                self.state = 'listening'
                self.threads = [threading.Thread(target=fn, args=(sock,), name=name)
                                for fn, sock, name in [(self._discover, udp, 'mist-discovery'),
                                                       (self._accept, tcp, 'mist-assignments')]]
                for thread in self.threads:
                    thread.start()
            except Exception as exc:
                for sock in list(self.sockets):
                    self._close(sock)
                if self.root_fd is not None:
                    os.close(self.root_fd)
                    self.root_fd = None
                self.state, self.error = 'error', str(exc)
                self.event('Could not start sender', 'ERROR', error=str(exc))
                raise
        self.event('Sender listening', tcp_port=PORT + int(self.sender_id), udp_port=PORT)
        if self.sender_id == '0':
            self.event('ID 0 conflicts with a receiver on the same host; use ID 1 locally', 'WARNING')

    def stop(self):
        with self.lock:
            if self.state == 'stopped':
                return
            self.state = 'stopping'
            self.stop_event.set()
            for sock in list(self.sockets):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self._close(sock)
        for thread in self.threads:
            thread.join()
        if self.pool:
            self.pool.shutdown(wait=True)
        with self.lock:
            if self.root_fd is not None:
                os.close(self.root_fd)
                self.root_fd = None
            self.state = 'stopped'
        self.event('Sender stopped')

    def _discover(self, sock):
        while not self.stop_event.is_set():
            try:
                data, peer = sock.recvfrom(BUFFER_SIZE)
                kind, name = data.decode('utf-8').split(':')
                if kind != MSG_REQUEST:
                    raise ValueError('Expected REQUEST')
                with self.open_file(name) as source:
                    size = os.fstat(source.fileno()).st_size
                sock.sendto(f'{MSG_ACK}:{self.sender_id}:{name}:{size}'.encode(), peer)
                self.event('Discovery reply sent', file=name, peer=peer[0], bytes=size)
            except socket.timeout:
                continue
            except (OSError, ValueError, UnicodeError) as exc:
                if not self.stop_event.is_set():
                    self.event('Discovery request rejected', 'WARNING', error=str(exc))

    def _accept(self, sock):
        while not self.stop_event.is_set():
            try:
                conn, peer = sock.accept()
                self._track(conn)
                if not self.slots.acquire(blocking=False):
                    self.event('Assignment rejected: all workers busy', 'WARNING', peer=peer[0])
                    self._close(conn)
                    continue
                try:
                    self.pool.submit(self._assignment, conn, peer)
                except Exception:
                    self.slots.release()
                    self._close(conn)
                    raise
            except socket.timeout:
                continue
            except OSError as exc:
                if not self.stop_event.is_set():
                    self.event('Assignment listener failed', 'ERROR', error=str(exc))
                    with self.lock:
                        self.state, self.error = 'error', str(exc)
                    self.stop_event.set()

    def read_assignment(self, conn):
        data = bytearray()
        deadline = time.monotonic() + self.timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Assignment must finish with EOF before deadline')
            conn.settimeout(remaining)
            block = conn.recv(min(BUFFER_SIZE, 4097 - len(data)))
            if not block:
                break
            data.extend(block)
            if len(data) > 4096:
                raise ValueError('Assignment exceeds 4096 bytes')
        fields = data.decode('utf-8').split(':')
        if len(fields) != 6:
            raise ValueError('Expected ASSIGN:id:filename:start:end:IPv4 followed by EOF')
        kind, sid, name, start, end, receiver = fields
        if kind != MSG_ASSIGN or sid != self.sender_id:
            raise ValueError('Invalid assignment type or sender ID')
        validate_filename(name)
        if not start.isascii() or not start.isdecimal() or not end.isascii() or not end.isdecimal():
            raise ValueError('Offsets must be nonnegative decimal integers')
        start, end = int(start), int(end)
        if start > end:
            raise ValueError('Range must satisfy start <= end')
        ipaddress.IPv4Address(receiver)
        return name, start, end, receiver

    def _assignment(self, conn, peer):
        record = {'id': uuid.uuid4().hex[:12], 'sender_id': self.sender_id, 'file': None,
                  'start': None, 'end': None, 'total': None, 'bytes_sent': 0,
                  'wire_bytes_sent': 0, 'peer': peer[0], 'status': 'receiving assignment',
                  'started_at': time.time(), 'started_monotonic': time.monotonic(), 'duration': 0, 'throughput': 0,
                  'attempts': 0, 'corrupt': self.corrupt, 'receiver_confirmed': False}
        began = time.monotonic()
        with self.lock:
            self.active[record['id']] = record
        try:
            name, start, end, receiver = self.read_assignment(conn)
            self._close(conn)
            with self.lock:
                record.update(file=name, start=start, end=end, total=end-start, receiver=receiver, status='checksumming')
            self.event('Assignment accepted', transfer=record['id'], file=name, start=start, end=end, peer=receiver)
            with self.open_file(name) as source:
                before = os.fstat(source.fileno())
                if not 0 <= start <= end <= before.st_size:
                    raise ValueError('Range must satisfy 0 <= start <= end <= file size')
                source.seek(start)
                crc = 0
                for block in self._blocks(source, end-start):
                    crc = zlib.crc32(block, crc)
                checksum = f'{crc & 0xffffffff:08x}'
                header = f'{MSG_CHUNK}:{self.sender_id}:{name}:{start}:{end}:{checksum}:'.encode()
                with self.lock:
                    record.update(crc=checksum, status='connecting')
                out = self._connect(receiver, record)
                try:
                    # From this point forward, never retry: sendall errors have unknown partial progress.
                    out.sendall(header)
                    with self.lock:
                        record.update(wire_bytes_sent=len(header), status='sending')
                    source.seek(start)
                    offset = 0
                    send_started = time.monotonic()
                    for block in self._blocks(source, end-start):
                        if self.corrupt and offset < 10:
                            block = bytes(b ^ 255 if offset+i < 10 else b for i, b in enumerate(block))
                        out.sendall(block)
                        offset += len(block)
                        with self.lock:
                            elapsed = time.monotonic() - send_started
                            record.update(bytes_sent=offset, wire_bytes_sent=len(header)+offset,
                                          duration=time.monotonic()-began, throughput=offset/max(elapsed, 1e-9))
                            self.total_bytes += len(block)
                            self.samples.append({'time': time.time(), 'bytes': self.total_bytes})
                    after = os.fstat(source.fileno())
                    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        raise OSError('Source changed during transfer; receiver outcome unknown')
                    with self.lock:
                        record['status'] = 'sender complete'
                    self.event('Sender transmission complete; receiver confirmation unavailable', transfer=record['id'],
                               file=name, start=start, end=end, bytes_sent=offset, crc=checksum,
                               duration=time.monotonic()-began, result='sender complete', corrupt=self.corrupt)
                finally:
                    self._close(out)
        except Exception as exc:
            with self.lock:
                record.update(status='cancelled' if self.stop_event.is_set() else 'failed', error=str(exc))
                self.failures += 1
            self.event('Transfer did not complete; no payload replay', 'ERROR', transfer=record['id'],
                       file=record['file'], error=str(exc), result=record['status'],
                       bytes_sent=record['bytes_sent'])
        finally:
            self._close(conn)
            with self.lock:
                record['duration'] = time.monotonic() - began
                record['finished_at'] = time.time()
                record.pop('started_monotonic', None)
                self.history.append(dict(record))
                self.active.pop(record['id'], None)
            self.slots.release()

    def _blocks(self, source, length):
        while length:
            if self.stop_event.is_set():
                raise InterruptedError('Transfer cancelled')
            block = source.read(min(BLOCK_SIZE, length))
            if not block:
                raise OSError('Source truncated while reading assigned range')
            length -= len(block)
            yield block

    def _connect(self, receiver, record):
        for attempt in range(1, self.retries + 1):
            sock = self._track(socket.socket())
            sock.settimeout(self.timeout)
            with self.lock:
                record['attempts'] = attempt
            try:
                sock.connect((receiver, PORT))
                with self.lock:
                    if receiver not in self.peers:
                        self.peers.append(receiver)
                return sock
            except OSError:
                self._close(sock)
                if attempt == self.retries or self.stop_event.is_set():
                    raise
                with self.lock:
                    record['status'] = 'retrying connection'
                delay = self.backoff * 2 ** (attempt-1)
                self.event('Receiver not listening; retrying connection before transmission', 'WARNING',
                           transfer=record['id'], attempt=attempt, delay=delay)
                if self.stop_event.wait(delay):
                    raise InterruptedError('Transfer cancelled during retry')

    def snapshot(self):
        with self.lock:
            return {'state': 'sending' if self.active and self.state == 'listening' else self.state,
                    'error': self.error, 'sender_id': self.sender_id, 'input_dir': str(self.root),
                    'corrupt': self.corrupt, 'tcp_port': PORT+int(self.sender_id), 'udp_port': PORT,
                    'active': [{k: v for k, v in dict(r, duration=time.monotonic()-r['started_monotonic']).items() if k != 'started_monotonic'} for r in self.active.values()], 'history': list(self.history),
                    'samples': list(self.samples), 'peers': list(self.peers),
                    'bytes_sent': self.total_bytes, 'failures': self.failures}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--id', required=True)
    parser.add_argument('--active', default='true')
    parser.add_argument('--corrupt', default='false')
    parser.add_argument('--input-dir', default='input')
    parser.add_argument('--json-logs', action='store_true')
    args = parser.parse_args()
    if args.active.lower() != 'true':
        print(f'[Sender {args.id}] Inactive. Exiting.')
        return 0
    configure(json_logs=args.json_logs)
    try:
        service = SenderService(args.id, args.input_dir, args.corrupt.lower() == 'true')
        quit_event = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: quit_event.set())
        service.start()
        try:
            while not quit_event.wait(0.5):
                if service.stop_event.is_set():
                    break
        finally:
            service.stop()
        return 0 if service.error is None else 1
    except (OSError, ValueError) as exc:
        log(str(exc), 'ERROR')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
