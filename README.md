# Distributed File Transfer System

A Python-based distributed file transfer system that allows a receiver to request and assemble a file from multiple senders over a network. Features include automatic sender discovery, chunk-based parallel transfer, CRC-32 integrity verification, corruption simulation, and audit logging.

## Features
- **Sender Discovery**: Receiver broadcasts requests via UDP to find available senders.
- **Chunk Assignment**: Receiver divides the file into chunks and assigns them to senders.
- **Parallel Transfer**: Senders transmit chunks via TCP for reliability.
- **Integrity Checks**: Uses CRC-32 to verify chunk integrity; corrupted chunks are discarded.
- **Corruption Simulation**: Senders can simulate data corruption for testing.
- **Audit Logging**: Full log history printed at the end for review.
- **Error Handling**: Aborts and deletes partial files on corruption detection.

## Requirements
- Python 3.x
- Standard libraries: `socket`, `os`, `time`, `threading`, `argparse`, `zlib` (for CRC-32), `datetime`

No external dependencies needed beyond Python's built-in modules.

## Project Structure
- `logger.py`: Logging utility with audit history.
- `protocol.py`: Constants for ports, buffer sizes, and message types.
- `receiver.py`: Main receiver script for discovery, assignment, chunk reception, verification, and assembly.
- `sender.py`: Sender script for listening to requests, receiving assignments, and sending chunks.
- `input/`: Directory for files to be sent (place on senders).
- `output/`: Directory for assembled files (on receiver).

## Installation
1. Clone or download the project files.
2. Ensure Python 3.x is installed.
3. Create `input/` on senders and `output/` on the receiver.

## Usage
### Running Senders
Start multiple senders on different machines or ports (use unique `--id` values).

- Normal sender: `python sender.py --id 0 --active true --corrupt false`
- Corrupted sender (for testing): `python sender.py --id 1 --active true --corrupt true`

Senders will listen for requests and assignments.

### Running Receiver
Run the receiver on a separate machine or terminal.

1. `python receiver.py`
2. Enter the filename to request (e.g., `song.mp3`).
3. The receiver will discover senders, assign chunks, receive and verify them, and assemble the file if all chunks are valid.

### Example Workflow
1. Place `song.mp3` in `input/` on senders.
2. Start senders as above.
3. Run receiver and enter `song.mp3`.
4. If all chunks are valid, the file assembles in `output/`. If corrupted, it logs "File corrupted, discarding, exiting." and prints the audit log.

### Command-Line Options
- **Sender (`sender.py`)**:
  - `--id`: Unique sender ID (required).
  - `--active`: Whether the sender is active (default: `true`).
  - `--corrupt`: Simulate corruption by flipping bytes (default: `false`).

- **Receiver (`receiver.py`)**: No options; prompts for filename.

## Testing
- **Normal Transfer**: Run multiple normal senders and a receiver. Verify the file assembles correctly.
- **Corruption Test**: Run one corrupted sender and one normal. Receiver should detect mismatch, discard, and exit with audit log.
- **Edge Cases**: Test with missing files, network issues, or single sender.

## Notes
- Uses UDP (port 5000) for discovery and TCP (port 5000 + sender_id) for transfers.
- CRC is computed on original data; corruption happens afterward for accurate detection.
- Audit log provides a full timestamped history of the run.
- For production, consider adding encryption or authentication.

## License
This project is open-source. Modify and distribute as needed.