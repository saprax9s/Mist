# NetWeave — Distributed File Transfer System

Python 3.10+ on Linux. Standard library only; no dependency installation required.

## Launch

```bash
./launch.sh
```

Open http://127.0.0.1:8765. Add files to `input/`, choose sender ID 1, then click **Start sender**. The dashboard provides real status, file listing, logs, transfer history and a throughput chart. No demo records are injected.

Direct dashboard and original terminal workflow:

```bash
mkdir -p input
python3 dashboard.py --input-dir input --port 8765
python3 sender.py --id 1 --active true --corrupt false --input-dir input
# In a second terminal:
python3 receiver.py
```

Run either dashboard or CLI for a given sender port. Optional `--json-logs` enables structured persistent logs in either mode. Stop with the dashboard button or Ctrl+C.

## Focused tests

```bash
PYTHONPATH=tests:. python3 -m unittest -v test_mist.MistTest.test_normal_fragmented_and_corrupt_wire test_mist.MistTest.test_logger_legacy_thread_safety_rotation test_mist.MistTest.test_dashboard_controls_security_exports_real_events test_mist.MistTest.test_protected_sources
sha256sum -c tests/protected.sha256
```

Full available suite: `python3 -m unittest discover -s tests -v`.

## Implementation and boundaries

- Sender: blocking lifecycle, bounded workers, timeouts, reliable `sendall()`, connection-only retries, graceful cleanup, directory-restricted regular-file access, validated ranges and fragmented assignments collected until EOF.
- CRC is calculated in bounded 64 KiB blocks before streaming and before optional corruption. No payload replay after a partial-send error.
- Logger retains `log(message)` and `print_audit_log()`, adds thread-safe bounded snapshots, rotating text logs and optional JSON logs. Rotation is process-local; separate processes should use separate working/log directories.
- Dashboard binds to localhost, operates the actual sender and exports retained logs/history. Local control requests have same-origin checks; the transfer protocol remains unchanged.

**Sender completion does not prove receiver verification or assembly.** Byte counts represent successful local send calls; failed calls can have additional unknown partial progress. History retains 500 attempts, audit memory 2,000 events and the chart 240 samples. Disk logs rotate at 2 MB with three backups. History is not restored after process exit.

`receiver.py` and `protocol.py` are unchanged; their original hashes are in `tests/protected.sha256`. Existing ports and wire formats remain intact. ID 0 conflicts with receiver TCP 5000 on the same host; use ID 1 locally. The receiver listens after assigning chunks, assembles by sender ID rather than byte offset, buffers chunks and provides no acknowledgment. Connection retries mitigate only its listener startup race. Assignment framing requires connection EOF because the protocol has no length or delimiter. Use a trusted network: CRC detects accidental corruption, not malicious modification.
