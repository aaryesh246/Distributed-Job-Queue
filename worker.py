import socket
import ssl
import json
import sys
import time
from collections import Counter

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
DISPATCHER_HOST = '127.0.0.1'  # change to dispatcher's IP if on different machine
WORKER_PORT     = 6000
CERT_FILE       = 'cert.pem'   # path to SSL certificate

# ─────────────────────────────────────────────
#  SSL/TLS CONFIGURATION
# ─────────────────────────────────────────────
def create_ssl_context():
    """Create SSL context for worker connection (no verification for self-signed certs)."""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def send_json(sock, data):
    msg = json.dumps(data).encode()
    length = len(msg).to_bytes(4, 'big')
    sock.sendall(length + msg)

def recv_json(sock):
    raw_len = _recv_exact(sock, 4)
    if not raw_len:
        return None
    length = int.from_bytes(raw_len, 'big')
    raw_msg = _recv_exact(sock, length)
    if not raw_msg:
        return None
    return json.loads(raw_msg.decode())

def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

# ─────────────────────────────────────────────
#  TASK PROCESSORS
# ─────────────────────────────────────────────
def count_words(text):
    """Count total number of words."""
    words = text.split()
    return len(words)

def to_lowercase(text):
    """Convert all uppercase characters to lowercase."""
    return text.lower()

def word_frequency(text, top_n=5):
    """Find the top N most frequent words."""
    words = text.lower().split()
    # Remove basic punctuation from words
    cleaned = []
    for w in words:
        w = w.strip('.,!?;:\'"()[]{}')
        if w:
            cleaned.append(w)
    counter = Counter(cleaned)
    return counter.most_common(top_n)

def process_job(job):
    """Run all requested tasks and return results."""
    filedata = job['filedata']
    tasks    = job['tasks']
    result   = {}

    if 'wordcount' in tasks:
        result['wordcount'] = count_words(filedata)

    if 'lowercase' in tasks:
        result['lowercase'] = to_lowercase(filedata)

    if 'frequency' in tasks:
        top_words = word_frequency(filedata)
        # Convert to list of [word, count] for JSON serialisation
        result['top_words'] = [[w, c] for w, c in top_words]

    return result

# ─────────────────────────────────────────────
#  MAIN WORKER LOOP
# ─────────────────────────────────────────────
def run_worker(worker_name):
    print("=" * 52)
    print(f"   DISTRIBUTED JOB QUEUE — {worker_name}")
    print("=" * 52)
    print(f"  Connecting to dispatcher at {DISPATCHER_HOST}:{WORKER_PORT} ...")

    while True:   # auto-reconnect loop
        try:
            context = create_ssl_context()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = context.wrap_socket(sock, server_hostname=DISPATCHER_HOST)
            sock.connect((DISPATCHER_HOST, WORKER_PORT))
            print(f"  Connected securely (SSL/TLS)! Waiting for jobs...\n")

            while True:
                job = recv_json(sock)
                if job is None:
                    print("  Connection to dispatcher lost.")
                    break

                job_id = job['job_id']
                tasks  = job['tasks']
                print(f"  [RECV]   {job_id} | tasks: {tasks}")

                # Process
                result = process_job(job)
                print(f"  [DONE]   {job_id} processed successfully")

                # Send result back
                send_json(sock, {'job_id': job_id, 'result': result})

        except ConnectionRefusedError:
            print("  Dispatcher not reachable. Retrying in 3 seconds...")
            time.sleep(3)
        except ssl.SSLError as e:
            print(f"  [ERROR] SSL/TLS connection failed: {e}. Retrying in 3 seconds...")
            time.sleep(3)
        except Exception as e:
            print(f"  [ERROR] {e}. Retrying in 3 seconds...")
            time.sleep(3)

# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == '__main__':
    # Optional: pass a worker name as argument e.g. python worker.py Worker-1
    name = sys.argv[1] if len(sys.argv) > 1 else 'WORKER'
    run_worker(name)