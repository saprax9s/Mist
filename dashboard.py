"""Local-only MIST control plane. No external dependencies."""
import argparse
import csv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import secrets
import signal
import threading
from urllib.parse import urlsplit

import logger
from sender import SenderService

WEB = Path(__file__).parent / 'web'


class Console:
    def __init__(self, directory):
        self.lock = threading.RLock()
        self.service = SenderService('1', directory)
        self.token = secrets.token_urlsafe(32)
        self.archived = []

    def state(self):
        with self.lock:
            state = self.service.snapshot()
            state['history'] = (self.archived + state['history'])[-500:]
            state['events'] = logger.snapshot()
            return state

    def control(self, body):
        with self.lock:
            if body.get('action') == 'stop':
                self.service.stop()
            elif body.get('action') == 'start':
                if self.service.state not in ('stopped', 'error'):
                    raise ValueError('Stop the sender before changing configuration')
                if not isinstance(body.get('corrupt', False), bool):
                    raise ValueError('Corruption must be a boolean')
                new = SenderService(body.get('sender_id', '1'), body.get('input_dir', 'input'), body.get('corrupt', False))
                self.archived = (self.archived + self.service.snapshot()['history'])[-500:]
                self.service.stop()
                self.service = new
                self.service.start()
            else:
                raise ValueError('Unknown control action')


class Server(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, console):
        self.console = console
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(5)

    def log_message(self, *_):
        pass

    def reply(self, status, data, mime='application/json', attachment=None):
        if not isinstance(data, bytes):
            data = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
        if attachment:
            self.send_header('Content-Disposition', f'attachment; filename="{attachment}"')
        self.end_headers()
        self.wfile.write(data)

    def trusted(self):
        host = self.headers.get('Host', '')
        port = self.server.server_port
        if host not in (f'127.0.0.1:{port}', f'localhost:{port}'):
            return False
        origin = self.headers.get('Origin')
        return (origin is None or origin == f'http://{host}') and self.headers.get('Sec-Fetch-Site') not in ('cross-site',)

    def do_GET(self):
        if not self.trusted():
            return self.reply(403, {'error': 'Local same-origin access required'})
        path = urlsplit(self.path).path
        app = self.server.console
        try:
            if path == '/favicon.ico':
                return self.reply(204, b'', 'image/x-icon')
            if path == '/api/state':
                return self.reply(200, app.state())
            if path == '/api/session':
                return self.reply(200, {'token': app.token})
            if path == '/api/files':
                with app.lock:
                    return self.reply(200, app.service.files())
            if path in ('/api/export/events.json', '/api/export/transfers.json', '/api/export/transfers.csv', '/api/export/events.txt'):
                state = app.state()
                rows = state['events' if 'events' in path else 'history']
                filename = path.rsplit('/', 1)[1]
                if path.endswith('.csv'):
                    output = io.StringIO()
                    keys = ['id', 'sender_id', 'file', 'start', 'end', 'status', 'bytes_sent', 'duration', 'throughput', 'attempts', 'error', 'receiver_confirmed']
                    writer = csv.DictWriter(output, fieldnames=keys, extrasaction='ignore')
                    writer.writeheader()
                    # Prevent spreadsheet formulas in untrusted filenames and error fields.
                    for row in rows:
                        writer.writerow({k: "'"+v if isinstance(v, str) and v.startswith(('=', '+', '-', '@', '\t', '\r')) else v for k,v in row.items()})
                    return self.reply(200, output.getvalue().encode(), 'text/csv; charset=utf-8', filename)
                if path.endswith('.txt'):
                    output = '\n'.join(f"{r['timestamp']} {r['level']} {r['message']} {json.dumps(r)}" for r in rows)
                    return self.reply(200, output.encode(), 'text/plain; charset=utf-8', filename)
                return self.reply(200, rows, attachment=filename)
            assets = {'/': ('index.html', 'text/html'), '/app.js': ('app.js', 'text/javascript'), '/style.css': ('style.css', 'text/css')}
            if path in assets:
                name, mime = assets[path]
                return self.reply(200, (WEB/name).read_bytes(), mime+'; charset=utf-8')
            self.reply(404, {'error': 'Not found'})
        except (OSError, ValueError) as exc:
            self.reply(400, {'error': str(exc)})

    def do_POST(self):
        if not self.trusted() or not secrets.compare_digest(self.headers.get('X-MIST-Token', ''), self.server.console.token):
            return self.reply(403, {'error': 'Same-origin session token required'})
        if self.path != '/api/control':
            return self.reply(404, {'error': 'Not found'})
        try:
            if self.headers.get('Content-Type') != 'application/json':
                raise ValueError('Expected application/json')
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 8192:
                raise ValueError('Invalid request length')
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError('Expected an object')
            self.server.console.control(body)
            self.reply(200, self.server.console.state())
        except (OSError, ValueError, TypeError) as exc:
            self.reply(400, {'error': str(exc)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', default='input')
    parser.add_argument('--port', default=8765, type=int)
    parser.add_argument('--json-logs', action='store_true')
    args = parser.parse_args()
    logger.configure(json_logs=args.json_logs)
    try:
        app = Console(args.input_dir)
        server = Server(('127.0.0.1', args.port), app)
    except (OSError, ValueError, OverflowError) as exc:
        parser.error(str(exc))
    def shutdown(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)
    print(f'NetWeave — Distributed File Transfer System: http://127.0.0.1:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    finally:
        app.service.stop()
        server.server_close()


if __name__ == '__main__':
    main()
