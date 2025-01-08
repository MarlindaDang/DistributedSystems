import socket
import threading
import uuid
import random
import time

# --- Konfigurationsvariablen ---
HEADER = 64
TCP_PORT = random.randint(5000, 6000)
BROADCAST_PORT = random.randint(6001, 7000)
FORMAT = 'utf-8'
DISCONNECTED_MESSAGE = "!LEAVE"
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 10
coordinator = None
server_id = str(uuid.uuid4())  # Eindeutige Server-ID
global servers
servers = []

udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
udp_server.bind(("", BROADCAST_PORT))

tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_server.bind(("", TCP_PORT))

clients = []
heartbeat_tracker = {}

# ----------------------- Server-Funktionen -----------------------

def handle_client(conn, addr):
    """
    Verwaltet die Kommunikation mit einem einzelnen Chat-Client.
    Unterstützt HEARTBEAT, ELECTION und COORDINATOR-Nachrichten.
    """
    global coordinator
    print(f"[NEW CONNECTION] {addr} ist dem Chat beigetreten.")
    clients.append(conn)
    heartbeat_tracker[conn] = (0, time.time())

    connected = True
    while connected:
        try:
            # Nachricht lesen
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)

                if msg.startswith("HEARTBEAT"):
                    _, lamport_time = msg.split("|")
                    lamport_time = int(lamport_time)
                    heartbeat_tracker[conn] = (lamport_time, time.time())
                    print(f"[HEARTBEAT] {addr}: Logische Zeit = {lamport_time}")

                elif msg.startswith("ELECTION"):
                    sender_rank = int(msg.split("|")[1])
                    if sender_rank < int(server_id.split('-')[0], 16):
                        print(f"[ELECTION] Wahl-Nachricht von Server mit Rang {sender_rank} erhalten.")
                        start_election()

                elif msg.startswith("COORDINATOR"):
                    coordinator = msg.split("|")[1]
                    print(f"[NEW COORDINATOR] Neuer Koordinator: {coordinator}")

                elif msg == DISCONNECTED_MESSAGE:
                    connected = False
                    print(f"[LEAVE] {addr} hat den Chat verlassen.")

                else:
                    print(f"[{addr}] {msg}")
                    broadcast(f"[{addr}] {msg}".encode(FORMAT), conn)
        except ConnectionResetError:
            connected = False
            print(f"[ERROR] Verbindung zu {addr} unterbrochen.")

    conn.close()
    if conn in clients:
        clients.remove(conn)
    if conn in heartbeat_tracker:
        del heartbeat_tracker[conn]
    print(f"[DISCONNECTED] {addr} Verbindung geschlossen.")


def broadcast(message, sender_conn):
    for client in clients:
        if client != sender_conn:
            client.send(message)

def monitor_heartbeats():
    global coordinator
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        if coordinator:
            coordinator_conn = next((conn for conn in clients if conn.getpeername()[1] == int(coordinator)), None)
            if coordinator_conn and time.time() - heartbeat_tracker.get(coordinator_conn, (0, 0))[1] > HEARTBEAT_TIMEOUT:
                print(f"[COORDINATOR FAILURE] Koordinator {coordinator} nicht erreichbar.")
                start_election()

        current_time = time.time()
        for conn, (last_lamport_time, last_time) in list(heartbeat_tracker.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu {conn.getpeername()} abgebrochen.")
                conn.close()
                if conn in clients:
                    clients.remove(conn)
                del heartbeat_tracker[conn]


def start_election():
    global coordinator
    print(f"[ELECTION] Server {server_id} startet eine Wahl.")
    higher_servers = [s for s in servers if s > server_id]

    if not higher_servers:
        coordinator = server_id
        print(f"[COORDINATOR] Server {server_id} ist der neue Koordinator.")
        broadcast_coordinator()
    else:
        for server in higher_servers:
            send_election_message(server)


def send_election_message(target_server):
    try:
        print(f"[ELECTION MESSAGE] Nachricht an {target_server} gesendet.")
    except Exception:
        print(f"[ELECTION] Keine Antwort von Server {target_server}.")


def broadcast_coordinator():
    for server in servers:
        try:
            print(f"[BROADCAST] Koordinator an {server} gesendet.")
        except Exception:
            print(f"[COORDINATOR] Server {server} konnte nicht erreicht werden.")

if __name__ == "__main__":
    print(f"[STARTING] Server {server_id} wird gestartet.")
    servers.append(server_id)
    threading.Thread(target=monitor_heartbeats, daemon=True).start()
    print("Server ready.")
