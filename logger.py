"""Bounded, thread-safe audit events; legacy receiver API remains supported."""
import json
import logging
from logging.handlers import RotatingFileHandler
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
import threading

_lock = threading.RLock()
log_history = deque(maxlen=2000)
_events = deque(maxlen=2000)
_sequence = 0
_writer = logging.getLogger('mist.audit')
_writer.setLevel(logging.DEBUG)
_writer.propagate = False
_configured = False
_console = True


def configure(directory='logs', json_logs=False, max_bytes=2_000_000, backups=3, console=True):
    global _configured, _console
    with _lock:
        for handler in _writer.handlers[:]:
            handler.close()
            _writer.removeHandler(handler)
        Path(directory).mkdir(parents=True, exist_ok=True)
        for filename in ['mist.log'] + (['mist.jsonl'] if json_logs else []):
            handler = RotatingFileHandler(Path(directory) / filename, maxBytes=max_bytes,
                                          backupCount=backups, encoding='utf-8')
            handler.setFormatter(logging.Formatter('%(message)s'))
            handler.structured = filename.endswith('jsonl')
            _writer.addHandler(handler)
        _configured, _console = True, console


def log(message, level='INFO', **context):
    global _sequence
    level = level.upper()
    if level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR'):
        raise ValueError('Unknown log level')
    with _lock:
        if not _configured:
            configure()
        _sequence += 1
        event = dict(context, id=_sequence, timestamp=datetime.now(timezone.utc).isoformat(),
                     level=level, message=str(message))
        # JSON round-trip creates a detached, serializable snapshot.
        event = json.loads(json.dumps(event, default=str))
        line = f"[{event['timestamp']}] {level:<7} {message}"
        if context:
            line += ' ' + json.dumps(context, default=str, ensure_ascii=True)
        line = line.replace('\n', '\\n').replace('\r', '\\r')
        _events.append(event)
        log_history.append(line)
        for handler in _writer.handlers:
            record = logging.LogRecord('mist.audit', getattr(logging, level), '', 0,
                                       json.dumps(event) if handler.structured else line, (), None)
            handler.handle(record)
        if _console:
            print(line, flush=True)
        return event.copy()


def snapshot():
    with _lock:
        return json.loads(json.dumps(list(_events)))


def print_audit_log():
    with _lock:
        print('\n--- AUDIT LOG ---\n' + '\n'.join(log_history) + '\n--- END AUDIT LOG ---\n')
