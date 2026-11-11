# NetRescue – An Intelligent Server Failure Detection and Automatic Recovery System

## About the Project

Imagine a server suddenly stops responding while users are connected to it. This can interrupt services and cause inconvenience.

**NetRescue** is a Python-based project developed to address this problem. It monitors the health of virtual servers, detects failures and overload conditions, and helps maintain service availability by redirecting clients to an available server.

The main idea behind this project is simple: instead of waiting for a server failure to affect users, NetRescue continuously monitors servers and responds when something goes wrong.

## What Can NetRescue Do?

Here are some of the main features implemented in this project:

- **Server Monitoring:** Keeps track of registered servers and their availability.
- **Heartbeat Mechanism:** Servers regularly send heartbeat messages to show that they are active.
- **Failure Detection:** Identifies servers that stop responding within the configured time.
- **Health Monitoring:** Evaluates server health using monitoring information.
- **Overload Detection:** Identifies when a server approaches its configured capacity threshold.
- **Automatic Recovery:** Finds an available server when a failure occurs.
- **Client Redirection:** Reassigns clients to an available server.
- **Live Dashboard:** Displays server status and system activity through a graphical interface.
- **Event Logs:** Shows important events such as server registration, overload warnings and recovery actions.
- **Graph History:** Displays monitoring information over time.

## Technologies Used

| Technology | How it is used |
|---|---|
| Python | Main programming language |
| Tkinter | Creating the graphical dashboard |
| Socket Programming | Communication between servers and clients |
| JSON | Exchanging messages and information |
| SymPy | Supporting mathematical operations |
| Python Libraries | Handling networking and other system operations |

## How the System Works

NetRescue follows a simple monitoring and recovery process:

1. Virtual servers connect and register with the central monitoring server.
2. Each server periodically sends heartbeat messages.
3. The central server checks whether the registered servers are responding.
4. If a server stops responding, the system identifies it as a failed server.
5. The system looks for another available server.
6. Clients are reassigned to the available server.
7. All important activities are displayed on the dashboard.

## Project Structure

The project is divided into different Python files, with each file handling a specific responsibility.

| File | Description |
|---|---|
| `netrescue_server.py` | Handles server registration, monitoring and recovery decisions |
| `virtual_server.py` | Simulates individual virtual servers |
| `client_sim.py` | Simulates clients connecting to servers |
| `common.py` | Contains shared communication functions |
| `run_all.py` | Starts the complete project |
| `launch_demo.py` | Starts the project demonstration |

## How to Run the Project

Follow these steps to run NetRescue on your computer.

### Requirements

- Python 3.10 or later
- pip package manager

### 1. Clone the Repository

```bash
git clone https://github.com/Nanmaran-M/NetRescue.git
```

### 2. Open the Project Folder

```bash
cd NetRescue
```

### 3. Install the Required Packages

```bash
pip install -r requirements.txt
```

### 4. Start the Project

```bash
python run_all.py
```

To start the demonstration version, you can also use:

```bash
python launch_demo.py
```

## Project Output

The screenshots included in this repository show different stages of the project:

- Normal server monitoring
- Server overload detection
- Server failure and recovery
- Recovery decision factors
- Graph history
- Event log
- Network configuration

These outputs help demonstrate how the system behaves under different simulated conditions.

## Current Limitations

NetRescue is currently developed as an academic prototype for a controlled local simulation. It demonstrates the basic concepts of server monitoring and automatic recovery, but it is not yet a production-ready system.

Some limitations include:

- It currently runs in a local simulation environment.
- Runtime information is stored in memory; a persistent database is not implemented.
- The central monitoring server can become a single point of failure.
- Authentication and encrypted communication are not currently included.
- Deployment across multiple physical machines is not yet implemented.

## Future Improvements

There are several ways this project can be improved in the future:

- Adding database support to store monitoring history.
- Implementing secure communication between servers.
- Introducing distributed monitoring to avoid a single point of failure.
- Deploying the system across multiple machines or cloud platforms.
- Using machine learning techniques for better failure prediction.
- Improving the dashboard with more real-time monitoring features.

## Project Details

- **Project Name:** NetRescue
- **Domain:** Server Monitoring and Fault Tolerance
- **Language:** Python
- **Interface:** Tkinter GUI
- **Communication:** TCP Socket Programming

---

### NetRescue

*Detect failures. Recover automatically. Keep services running.*

Developed as an academic project to explore server monitoring, failure detection and automatic recovery using Python.
