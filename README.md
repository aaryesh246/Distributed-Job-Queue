📦 Distributed Job Queue

A secure distributed job queue system built using Python, enabling asynchronous task processing across multiple components with encrypted communication.

🚀 Overview

This project implements a distributed architecture consisting of three core components:

Dispatcher – Manages incoming job requests and assigns them to workers
Client – Submits jobs to the dispatcher
Worker – Processes jobs and returns results

All communication between components is secured using SSL/TLS over sockets, ensuring encrypted data transfer across processes.

🧠 Key Features
Unique Job ID assignment for tracking tasks
Load distribution across available workers
Asynchronous job handling
Secure communication using SSL/TLS
Modular design with clear separation of components

🏗️ Architecture

Client → Dispatcher → Worker → Dispatcher → Client

Client sends a job request
Dispatcher assigns a unique job ID
Dispatcher forwards the job to an available worker
Worker processes the task
Result is sent back through the dispatcher to the client

⚙️ Tech Stack
Python
Socket Programming
SSL/TLS Encryption
Networking Concepts

📂 Project Structure
project-root/
│
├── dispatcher.py      # Handles job distribution
├── client.py          # Sends job requests
├── worker.py          # Executes jobs
├── certs/             # SSL certificates and keys
└── README.md

🔐 Security

All communication channels are encrypted using SSL/TLS, ensuring:

Data confidentiality
Protection against interception
Secure multi-process communication

▶️ Getting Started
1. Clone the repository
git clone https://github.com/your-username/distributed-job-queue.git
cd distributed-job-queue
2. Generate SSL Certificates
openssl req -new -x509 -days 365 -nodes -out cert.pem -keyout key.pem

Place them inside the certs/ directory.

3. Run the Components (in separate terminals)
Start Dispatcher
python dispatcher.py
Start Worker(s)
python worker.py
Run Client
python client.py

🧪 Example Workflow
Start dispatcher
Launch multiple workers
Submit a job using client
Worker processes and returns result
Client receives output

📌 Use Cases
Distributed task processing
Background job execution
Scalable systems with multiple workers
Learning distributed systems and networking

🔮 Future Improvements
Add job prioritization
Implement worker health monitoring
Introduce message queues (e.g., RabbitMQ)
Add a web-based dashboard