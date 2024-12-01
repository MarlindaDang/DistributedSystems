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
HEARTBEAT_TIMEOUT = 20

# --- Initialisierung ---
clients = {}
server_id = str(uuid.uuid4())
lamport_time = 0


def update_lamport_time(received_time=None):
    global lamport_time
    lamport_time = max(lamport_time, received_time + 1) if received_time else lamport_time + 1
    return lamport_time


def handle_client(conn, addr):
    print(f"[USER JOINED] {addr} hat den Chat betreten.")
    clients[conn] = time.time()

    while True:
        try:
            # Empfang der Nachrichtenlänge
            msg_length = conn.recv(HEADER).decode(FORMAT).strip()
            if not msg_length:
                break
            msg_length = int(msg_length)

            # Empfang der Nachricht
            message = conn.recv(msg_length).decode(FORMAT)
            parts = message.split("|")
            msg, received_time = parts[0], int(parts[1]) if len(parts) > 1 else None
            update_lamport_time(received_time)

            if msg == "HEARTBEAT":
                clients[conn] = time.time()
                print(f"[HEARTBEAT] Von {addr} empfangen. Zeit: {lamport_time}")
            elif msg == DISCONNECTED_MESSAGE:
                print(f"[USER LEFT] {addr} hat den Chat verlassen.")
                break
            else:
                print(f"[MESSAGE] {addr}: {msg} (Zeit: {lamport_time})")
                broadcast(conn, f"{msg}|{lamport_time}")
        except ConnectionResetError:
            print(f"[ERROR] Verbindung mit {addr} wurde unerwartet geschlossen.")
            break
        except Exception as e:
            print(f"[ERROR] Fehler beim Verarbeiten der Nachricht von {addr}: {e}")
            break

    conn.close()
    del clients[conn]
    print(f"[DISCONNECTED] {addr} Verbindung geschlossen.")


def broadcast(sender_conn, message):
    for conn in list(clients):
        if conn != sender_conn:
            try:
                conn.send(str(len(message)).encode(FORMAT).ljust(HEADER))
                conn.send(message.encode(FORMAT))
            except Exception:
                print(f"[ERROR] Senden an {conn.getpeername()} fehlgeschlagen.")
                conn.close()
                del clients[conn]


def monitor_heartbeats():
    while True:
        current_time = time.time()
        for conn, last_heartbeat in list(clients.items()):
            if current_time - last_heartbeat > HEARTBEAT_TIMEOUT:
                print(f"[TIMEOUT] {conn.getpeername()} ist inaktiv und wurde getrennt.")
                conn.close()
                del clients[conn]
        time.sleep(5)


def listen_for_broadcast():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_server:
        udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_server.bind(("", BROADCAST_PORT))
        print(f"[BROADCAST LISTENING] Server wartet auf Broadcast-Anfragen auf Port {BROADCAST_PORT}...")
        while True:
            try:
                data, addr = udp_server.recvfrom(1024)
                if data.decode(FORMAT) == "DISCOVER_CHAT_SERVER":
                    response = f"SERVER_ID:{server_id},SERVER_IP:{socket.gethostbyname(socket.gethostname())},PORT:{TCP_PORT}"
                    udp_server.sendto(response.encode(FORMAT), addr)
                    print(f"[BROADCAST RESPONSE] Antwort an {addr} gesendet.")
            except ConnectionResetError:
                print(f"[ERROR] Verbindungsproblem beim Empfang der Broadcast-Anfrage von {addr}. Ignoriere...")
            except Exception as e:
                print(f"[ERROR] Unerwarteter Fehler im Broadcast-Listener: {e}")



def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_server:
        tcp_server.bind(("", TCP_PORT))
        tcp_server.listen()
        print(f"[SERVER] Chat-Server läuft auf Port {TCP_PORT}")
        threading.Thread(target=monitor_heartbeats, daemon=True).start()

        while True:
            conn, addr = tcp_server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    print(f"[STARTING] Chat-Server wird gestartet...")
    threading.Thread(target=listen_for_broadcast, daemon=True).start()
    start_server()
