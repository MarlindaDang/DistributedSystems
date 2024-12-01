import socket
import threading
import uuid
import random

# --- Dynamische Konfigurationsvariablen ---
HEADER = 64
TCP_PORT = random.randint(5000, 6000)  # Zufälliger TCP-Port
BROADCAST_PORT = random.randint(6001, 7000)  # Zufälliger Broadcast-Port
FORMAT = 'utf-8'
DISCONNECTED_MESSAGE = "!LEAVE"

# --- Initialisierung der Server-Sockets ---
udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
udp_server.bind(("", BROADCAST_PORT))

tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_server.bind(("", TCP_PORT))

clients = []  # Liste für alle verbundenen Clients
server_id = str(uuid.uuid4())  # Eindeutige Server-ID


def handle_client(conn, addr):
    """
    Verwaltet die Kommunikation mit einem einzelnen Chat-Client.
    """
    print(f"[NEW CONNECTION] {addr} ist dem Chat beigetreten.")
    clients.append(conn)

    connected = True
    while connected:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)

                if msg == DISCONNECTED_MESSAGE:
                    connected = False
                    print(f"[LEAVE] {addr} hat den Chat verlassen.")
                else:
                    print(f"[{addr}] {msg}")
                    broadcast(f"[{addr}] {msg}".encode(FORMAT), conn)
        except ConnectionResetError:
            print(f"[ERROR] Verbindung mit {addr} unterbrochen.")
            connected = False

    conn.close()
    clients.remove(conn)
    print(f"[DISCONNECTED] {addr} Verbindung geschlossen.")


def broadcast(message, sender_conn):
    """
    Sendet eine Nachricht an alle verbundenen Clients außer dem Absender.
    """
    for client in clients:
        if client != sender_conn:
            client.send(message)


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
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


if __name__ == "__main__":
    print(f"[STARTING] Chat-Server wird gestartet... Server-ID: {server_id}")
    print(f"[SERVER INFO] TCP-Port: {TCP_PORT}, Broadcast-Port: {BROADCAST_PORT}")
    broadcast_thread = threading.Thread(target=listen_for_broadcast, daemon=True)
    broadcast_thread.start()
    start_tcp_server()
