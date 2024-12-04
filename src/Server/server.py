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

udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
udp_server.bind(("", BROADCAST_PORT))

tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_server.bind(("", TCP_PORT))

clients = []
heartbeat_tracker = {}
server_id = str(uuid.uuid4())


def handle_client(conn, addr):
    """
    Verwaltet die Kommunikation mit einem einzelnen Chat-Client.
    """
    print(f"[NEW CONNECTION] {addr} ist dem Chat beigetreten.")
    clients.append(conn)
    heartbeat_tracker[conn] = (0, time.time())

    connected = True
    while connected:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)

                if msg.startswith("HEARTBEAT"):
                    _, lamport_time = msg.split("|")
                    lamport_time = int(lamport_time)
                    heartbeat_tracker[conn] = (lamport_time, time.time())
                    print(f"[HEARTBEAT] {addr}: Logische Zeit = {lamport_time}")
                elif msg == DISCONNECTED_MESSAGE:
                    connected = False
                    print(f"[LEAVE] {addr} hat den Chat verlassen.")
                else:
                    print(f"[{addr}] {msg}")
                    broadcast(f"[{addr}] {msg}".encode(FORMAT), conn)
        except ConnectionResetError:
            connected = False

    conn.close()
    if conn in clients:
        clients.remove(conn)
    if conn in heartbeat_tracker:
        del heartbeat_tracker[conn]
    print(f"[DISCONNECTED] {addr} Verbindung geschlossen.")


def broadcast(message, sender_conn):
    """
    Sendet eine Nachricht an alle verbundenen Clients außer dem Absender.
    """
    for client in clients:
        if client != sender_conn:
            client.send(message)


def monitor_heartbeats():
    """
    Überwacht die Heartbeats der Clients und entfernt inaktive Verbindungen.
    """
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        current_time = time.time()
        for conn, (last_lamport_time, last_time) in list(heartbeat_tracker.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu {conn.getpeername()} abgebrochen.")
                conn.close()
                if conn in clients:
                    clients.remove(conn)
                del heartbeat_tracker[conn]


def listen_for_broadcast():
    """
    Hört auf Broadcast-Anfragen und antwortet mit Serverinformationen.
    """
    print(f"[BROADCAST LISTENING] Server wartet auf Broadcast-Anfragen auf Port {BROADCAST_PORT}...")
    while True:
        data, addr = udp_server.recvfrom(1024)
        if data.decode(FORMAT) == "DISCOVER_CHAT_SERVER":
            response = f"SERVER_ID:{server_id},SERVER_IP:{socket.gethostbyname(socket.gethostname())},PORT:{TCP_PORT}"
            udp_server.sendto(response.encode(FORMAT), addr)
            print(f"[BROADCAST RESPONSE] Antwort gesendet an {addr}")


def start_tcp_server():
    """
    Startet den TCP-Server und verwaltet eingehende Verbindungen.
    """
    tcp_server.listen()
    print(f"[TCP LISTENING] Server lauscht auf Port {TCP_PORT}")
    while True:
        conn, addr = tcp_server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


if __name__ == "__main__":
    print(f"[STARTING] Chat-Server wird gestartet... Server-ID: {server_id}")
    print(f"[SERVER INFO] TCP-Port: {TCP_PORT}, Broadcast-Port: {BROADCAST_PORT}")
    broadcast_thread = threading.Thread(target=listen_for_broadcast, daemon=True)
    broadcast_thread.start()
    heartbeat_thread = threading.Thread(target=monitor_heartbeats, daemon=True)
    heartbeat_thread.start()
    start_tcp_server()
