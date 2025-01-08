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
server_rank = int(server_id.split('-')[0], 16)  
servers = [] 

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

                elif msg.startswith("ELECTION"):  # *NEU*
                    sender_rank = int(msg.split("|")[1])  # *NEU* Extrahiere den Rang des sendenden Servers
                    if sender_rank < server_rank:  # *NEU* Starte Wahl, wenn der eigene Rang höher ist
                        print(f"[ELECTION] Wahl-Nachricht von Server mit Rang {sender_rank} erhalten.")  # *NEU*
                        start_election()  # *NEU*

                elif msg.startswith("COORDINATOR"):  # *NEU*
                    coordinator = msg.split("|")[1]  # *NEU* Aktualisiere den globalen Koordinator
                    print(f"[NEW COORDINATOR] Neuer Koordinator: {coordinator}")  # *NEU*

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
    """
    Sendet eine Nachricht an alle verbundenen Clients außer dem Absender.
    """
    for client in clients:
        if client != sender_conn:
            client.send(message)


def monitor_heartbeats():
    """
    Überwacht die Heartbeats der Clients und startet eine Wahl bei Koordinatorausfall.
    """
    global coordinator
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        if coordinator:  # *NEU* Prüft, ob ein Koordinator existiert
            if time.time() - heartbeat_tracker.get(coordinator, (0, 0))[1] > HEARTBEAT_TIMEOUT:  # *NEU*
                print(f"[COORDINATOR FAILURE] Koordinator {coordinator} nicht erreichbar.")  # *NEU*
                start_election()  # *NEU* Starte eine Wahl, da der Koordinator ausgefallen ist

        # Überprüfe Heartbeats anderer Verbindungen
        current_time = time.time()
        for conn, (last_lamport_time, last_time) in list(heartbeat_tracker.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu {conn.getpeername()} abgebrochen.")  # *NEU*
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

def start_election():
    """
    Initiiert eine Wahl nach dem Bully-Algorithmus.
    """
    global coordinator
    print(f"[ELECTION] Server {server_id} startet eine Wahl.")
    higher_servers = [s for s in servers if s > server_rank]

    if not higher_servers:
        coordinator = server_id
        print(f"[COORDINATOR] Server {server_id} ist der neue Koordinator.")
        broadcast_coordinator()
    else:
        for server in higher_servers:
            send_election_message(server)

def send_election_message(target_server):
    """
    Sendet eine Wahl-Nachricht an einen anderen Server.
    """
    try:
        # Beispielcode für Senden über TCP-Verbindung
        target_server.send(f"ELECTION|{server_rank}".encode(FORMAT))  # *NEU*
    except Exception:
        print(f"[ELECTION] Keine Antwort von Server {target_server}.")  # *NEU*

def broadcast_coordinator():
    """
    Informiert alle Server über den neuen Koordinator.
    """
    for server in servers:  # *NEU*
        try:
            # Beispielcode für TCP-Broadcast
            server.send(f"COORDINATOR|{server_id}".encode(FORMAT))  # *NEU*
        except Exception:
            print(f"[COORDINATOR] Server {server} konnte nicht erreicht werden.")  # *NEU*
            
if __name__ == "__main__":
    print(f"[STARTING] Chat-Server wird gestartet... Server-ID: {server_id}")
    print(f"[SERVER INFO] TCP-Port: {TCP_PORT}, Broadcast-Port: {BROADCAST_PORT}")
    broadcast_thread = threading.Thread(target=listen_for_broadcast, daemon=True)
    broadcast_thread.start()
    heartbeat_thread = threading.Thread(target=monitor_heartbeats, daemon=True)
    heartbeat_thread.start()
    start_tcp_server()
