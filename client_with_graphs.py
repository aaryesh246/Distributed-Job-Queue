#!/usr/bin/env python3
"""
Enhanced Client with Per-Job Performance Graphing

This script submits jobs and generates real-time graphs showing:
- Job timeline (status transitions)
- Response time breakdown
- Processing latency per job
- Cumulative performance metrics

Uses actual execution data, no assumptions.
"""

import socket
import ssl
import json
import time
import os
import sys
from pathlib import Path
from datetime import datetime
import statistics

# Try to import matplotlib for graphing
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("WARNING: matplotlib not available. Install with: pip install matplotlib")

# Configuration
DISPATCHER_HOST = '10.1.20.35'
DISPATCHER_PORT = 5000
CERT_FILE = 'cert.pem'
POLL_INTERVAL = 0.5  # Faster polling for accurate timing

# ─────────────────────────────────────────────
#  JOB TRACKING
# ─────────────────────────────────────────────
class JobMetrics:
    """Track real metrics for each job"""
    def __init__(self, job_id):
        self.job_id = job_id
        self.submitted_time = None
        self.accepted_time = None
        self.queued_time = None
        self.in_progress_time = None
        self.done_time = None
        self.status_history = []
        self.last_status = None
        self.result = None

    def record_status(self, status, timestamp=None):
        """Record status change with timestamp"""
        if timestamp is None:
            timestamp = time.time()
        
        if self.submitted_time is None:
            self.submitted_time = timestamp
        
        self.status_history.append({
            'status': status,
            'time': timestamp
        })
        
        if status == 'QUEUED' and self.queued_time is None:
            self.queued_time = timestamp
        elif status == 'IN_PROGRESS' and self.in_progress_time is None:
            self.in_progress_time = timestamp
        elif status == 'DONE' and self.done_time is None:
            self.done_time = timestamp
        
        self.last_status = status

    def get_metrics(self):
        """Calculate performance metrics"""
        if not self.submitted_time or not self.done_time:
            return None
        
        metrics = {
            'job_id': self.job_id,
            'total_time': self.done_time - self.submitted_time,
            'queued_duration': (self.queued_time - self.submitted_time) if self.queued_time else 0,
            'wait_duration': (self.in_progress_time - self.queued_time) if self.queued_time and self.in_progress_time else 0,
            'processing_duration': (self.done_time - self.in_progress_time) if self.in_progress_time else 0,
            'status_changes': len(self.status_history),
        }
        return metrics

# ─────────────────────────────────────────────
#  SSL/TLS CONFIGURATION
# ─────────────────────────────────────────────
def create_ssl_context():
    """Create SSL context for client connection."""
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
#  GRAPHING FUNCTIONS
# ─────────────────────────────────────────────
def create_timeline_graph(metrics):
    """Create a timeline visualization for a single job"""
    if not MATPLOTLIB_AVAILABLE or not metrics:
        return
    
    job_id = metrics['job_id']
    total_time = metrics['total_time']
    queued_time = metrics['queued_duration']
    wait_time = metrics['wait_duration']
    processing_time = metrics['processing_duration']
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 2))
    
    # Draw timeline
    y_pos = 0
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']  # Red, Teal, Blue
    labels = ['Queued', 'Waiting', 'Processing']
    
    x_start = 0
    
    # Queued phase
    if queued_time > 0:
        ax.barh(y_pos, queued_time, left=x_start, height=0.5, color=colors[0], label=labels[0])
        ax.text(x_start + queued_time/2, y_pos, f'{queued_time:.2f}s', ha='center', va='center', fontweight='bold')
        x_start += queued_time
    
    # Waiting phase
    if wait_time > 0:
        ax.barh(y_pos, wait_time, left=x_start, height=0.5, color=colors[1], label=labels[1])
        ax.text(x_start + wait_time/2, y_pos, f'{wait_time:.2f}s', ha='center', va='center', fontweight='bold')
        x_start += wait_time
    
    # Processing phase
    if processing_time > 0:
        ax.barh(y_pos, processing_time, left=x_start, height=0.5, color=colors[2], label=labels[2])
        ax.text(x_start + processing_time/2, y_pos, f'{processing_time:.2f}s', ha='center', va='center', fontweight='bold')
    
    # Labels and formatting
    ax.set_ylim(-0.5, 0.5)
    ax.set_xlim(0, total_time * 1.1)
    ax.set_xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title(f'Job {job_id} Timeline - Total: {total_time:.2f}s', fontsize=14, fontweight='bold')
    ax.set_yticks([])
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(axis='x', alpha=0.3)
    
    # Save figure
    filename = f'job_{job_id}_timeline.png'
    plt.tight_layout()
    plt.savefig(filename, dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Timeline graph saved: {filename}")

def create_breakdown_pie_chart(metrics):
    """Create a pie chart showing time breakdown"""
    if not MATPLOTLIB_AVAILABLE or not metrics:
        return
    
    job_id = metrics['job_id']
    queued_time = metrics['queued_duration']
    wait_time = metrics['wait_duration']
    processing_time = metrics['processing_duration']
    
    # Only create if we have meaningful data
    if queued_time <= 0 and wait_time <= 0 and processing_time <= 0:
        return
    
    # Create figure
    fig, ax = plt.subplots(figsize=(8, 6))
    
    # Data
    sizes = [queued_time, wait_time, processing_time]
    labels = [f'Queued\n{queued_time:.3f}s', f'Waiting\n{wait_time:.3f}s', f'Processing\n{processing_time:.3f}s']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
    explode = (0.05, 0.05, 0.1)  # Explode processing slice
    
    # Create pie chart
    ax.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
           shadow=True, startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
    
    ax.set_title(f'Job {job_id} Time Breakdown\nTotal: {metrics["total_time"]:.2f}s', 
                 fontsize=14, fontweight='bold')
    
    # Save figure
    filename = f'job_{job_id}_breakdown.png'
    plt.tight_layout()
    plt.savefig(filename, dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Breakdown chart saved: {filename}")

def create_status_timeline(job_metrics):
    """Create a timeline showing status transitions"""
    if not MATPLOTLIB_AVAILABLE or not job_metrics:
        return
    
    job_id = job_metrics.job_id
    history = job_metrics.status_history
    
    if not history or len(history) < 2:
        return
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 4))
    
    # Extract times and statuses
    times = [0]
    statuses = [history[0]['status']]
    base_time = history[0]['time']
    
    for entry in history[1:]:
        elapsed = entry['time'] - base_time
        times.append(elapsed)
        statuses.append(entry['status'])
    
    # Status colors
    status_colors = {
        'QUEUED': '#FF6B6B',
        'IN_PROGRESS': '#45B7D1',
        'DONE': '#51CF66'
    }
    
    # Plot status changes
    y_positions = {'QUEUED': 1, 'IN_PROGRESS': 2, 'DONE': 3}
    
    for i in range(len(times) - 1):
        x_vals = [times[i], times[i+1]]
        y_val = y_positions.get(statuses[i], 0)
        color = status_colors.get(statuses[i], '#999')
        
        ax.plot(x_vals, [y_val, y_val], color=color, linewidth=3, marker='o', markersize=8)
        ax.text(times[i], y_val + 0.15, statuses[i], ha='left', fontsize=9, fontweight='bold')
    
    # Final point
    ax.plot([times[-1]], [y_positions.get(statuses[-1], 0)], 'o', 
            color=status_colors.get(statuses[-1], '#999'), markersize=8)
    
    # Formatting
    ax.set_ylim(0.5, 3.5)
    ax.set_xlim(0, max(times) * 1.1)
    ax.set_xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Job Status', fontsize=12, fontweight='bold')
    ax.set_yticks([1, 2, 3])
    ax.set_yticklabels(['QUEUED', 'IN_PROGRESS', 'DONE'])
    ax.set_title(f'Job {job_id} Status Transitions', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    # Save figure
    filename = f'job_{job_id}_status.png'
    plt.tight_layout()
    plt.savefig(filename, dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Status timeline saved: {filename}")

# ─────────────────────────────────────────────
#  JOB SUBMISSION
# ─────────────────────────────────────────────
def submit_job(sock, filedata, tasks):
    """Submit job and get job_id"""
    send_json(sock, {
        'action': 'submit',
        'tasks': tasks,
        'filedata': filedata,
    })
    response = recv_json(sock)
    if response and response.get('status') == 'ACCEPTED':
        return response['job_id']
    return None

def poll_status(sock, job_id, metrics):
    """Poll for job status with real-time tracking"""
    spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    spin_i = 0

    while True:
        send_json(sock, {'action': 'status', 'job_id': job_id})
        response = recv_json(sock)

        if not response:
            print("\n  [ERROR] Lost connection to dispatcher.")
            return None

        status = response.get('status')
        metrics.record_status(status)

        print(f"\r  {spinner[spin_i % len(spinner)]}  {job_id}  :  {status}          ", end='', flush=True)
        spin_i += 1

        if status == 'DONE':
            print()
            metrics.record_status('DONE')
            return response.get('result')

        if status == 'NOT_FOUND':
            print(f"\n  [ERROR] Job {job_id} not found on dispatcher.")
            return None

        time.sleep(POLL_INTERVAL)

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 54)
    print("   DISTRIBUTED JOB QUEUE — CLIENT WITH GRAPHING")
    print("=" * 54)

    all_metrics = []

    while True:
        # ── Step 1: Load file ─────────────────────
        while True:
            filepath = input("\n  Enter path to your .txt file: ").strip()
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    filedata = f.read()
                size = os.path.getsize(filepath)
                print(f"  File loaded: {filepath}  ({size} bytes, {len(filedata.split())} words)")
                break
            else:
                print(f"  File not found: '{filepath}'. Try again.")

        # ── Step 2: Choose tasks ──────────────────
        print()
        tasks = ['wordcount', 'lowercase', 'frequency']
        print(f"  Tasks selected: {tasks}")

        # ── Step 3: Connect to dispatcher ─────────
        print(f"\n  Connecting to dispatcher at {DISPATCHER_HOST}:{DISPATCHER_PORT} (SSL/TLS) ...")
        try:
            context = create_ssl_context()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = context.wrap_socket(sock, server_hostname=DISPATCHER_HOST)
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            print("  Connected securely (SSL/TLS)!")
        except ConnectionRefusedError:
            print("  [ERROR] Could not connect to dispatcher. Is it running?")
            return
        except ssl.SSLError as e:
            print(f"  [ERROR] SSL/TLS connection failed: {e}")
            return

        # ── Step 4: Submit job ────────────────────
        print("\n  Submitting job ...")
        job_id = submit_job(sock, filedata, tasks)
        if not job_id:
            print("  [ERROR] Dispatcher rejected the job.")
            sock.close()
            return
        print(f"  Job accepted!  ID: {job_id}")

        # ── Step 5: Create metrics tracker ───────
        metrics = JobMetrics(job_id)
        metrics.record_status('SUBMITTED')

        # ── Step 6: Poll for result ───────────────
        print(f"\n  Waiting for result ...\n")
        result = poll_status(sock, job_id, metrics)

        # ── Step 7: Display result ────────────────
        if result:
            print()
            print("=" * 54)
            print(f"   RESULTS FOR {job_id}")
            print("=" * 54)
            
            if 'wordcount' in result:
                print(f"  Word count     : {result['wordcount']}")
            if 'lowercase' in result:
                text = result['lowercase']
                preview = text[:80] + ('...' if len(text) > 80 else '')
                print(f"  Lowercase text : {preview}")
            if 'top_words' in result:
                words = result['top_words']
                formatted = '  '.join(f"{w}({c})" for w, c in words)
                print(f"  Top words      : {formatted}")
            
            print("=" * 54)
            print()

            # ── Step 8: Generate graphs ──────────────
            job_metrics = metrics.get_metrics()
            if job_metrics:
                print("\n  Generating performance graphs...")
                create_timeline_graph(job_metrics)
                create_breakdown_pie_chart(job_metrics)
                create_status_timeline(metrics)
                
                print(f"\n  Job Metrics:")
                print(f"    Total time: {job_metrics['total_time']:.2f}s")
                print(f"    Queued time: {job_metrics['queued_duration']:.2f}s")
                print(f"    Waiting time: {job_metrics['wait_duration']:.2f}s")
                print(f"    Processing time: {job_metrics['processing_duration']:.2f}s")
                
                all_metrics.append(job_metrics)
        else:
            print("  No result received.")

        sock.close()

        # ── Step 9: Run another job? ──────────────
        again = input("  Submit another job? (y/n): ").strip().lower()
        if again != 'y':
            break

    # ── Summary of all jobs ───────────────────
    if all_metrics and len(all_metrics) > 1:
        print("\n" + "=" * 54)
        print("   SUMMARY - ALL JOBS")
        print("=" * 54)
        
        total_times = [m['total_time'] for m in all_metrics]
        print(f"\n  Number of jobs: {len(all_metrics)}")
        print(f"  Average response time: {statistics.mean(total_times):.2f}s")
        print(f"  Min response time: {min(total_times):.2f}s")
        print(f"  Max response time: {max(total_times):.2f}s")
        if len(total_times) > 1:
            print(f"  Std deviation: {statistics.stdev(total_times):.2f}s")

    print("\n  Goodbye!\n")

if __name__ == '__main__':
    main()
