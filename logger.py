from datetime import datetime

log_history = []  # Global list to store all log messages for audit

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted_message = f"[{timestamp}] {message}"
    print(formatted_message)
    log_history.append(formatted_message)  # Store for audit

def print_audit_log():
    print("\n--- AUDIT LOG ---")
    for entry in log_history:
        print(entry)
    print("--- END AUDIT LOG ---\n")