import socket
import threading
import time
import concurrent.futures

# --- Konfigurationsvariablen ---
HEADER = 64
BROADCAST_PORT_RANGE = range(6001, 7001)
FORMAT = 'utf-8'
DISCOVERY_MESSAGE = "DISCOVER_CHAT_SERVER"
DISCONNECTED_MESSAGE = "!LEAVE"
HEARTBEAT_INTERVAL = 10

# --- Lamport-Timestamp ---
lamport_time = 0


def update_lamport_time(received_time=None):
    global lamport_time
    lamport_time = max(lamport_time, received_time + 1) if received_time else lamport_time + 1
    return lamport_time


def discover_servers():
    server_list = []

    def broadcast_on_port(port):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            try:
                client_socket.sendto(DISCOVERY_MESSAGE.encode(FORMAT), ("<broadcast>", port))
                print(f"[CLIENT BROADCAST] Nachricht an Port {port} gesendet.")
            except Exception as e:
                print(f"[ERROR] Fehler beim Senden der Broadcast-Nachricht: {e}")

    print("[CLIENT] Suche nach Chat-Servern...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(broadcast_on_port, BROADCAST_PORT_RANGE)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        client_socket.settimeout(5)
        try:
            while True:
                data, addr = client_socket.recvfrom(1024)
                response = data.decode(FORMAT)
                if response not in server_list:
                    server_list.append(response)
                    print(f"[CLIENT BROADCAST] Antwort von {addr} erhalten: {response}")
        except socket.timeout:
            pass

    return server_list



def send_message(client, msg):
    timestamp = update_lamport_time()
    message = f"{msg}|{timestamp}".encode(FORMAT)
    client.send(str(len(message)).encode(FORMAT).ljust(HEADER))
    client.send(message)
    print(f"[YOU]: {msg} (Zeit: {timestamp})")


def send_heartbeat(client):
    while True:
        try:
            send_message(client, "HEARTBEAT")
        except Exception:
            print("[CLIENT] Verbindung unterbrochen.")
            break
        time.sleep(HEARTBEAT_INTERVAL)


def receive_messages(client):
    while True:
        try:
            msg_length = int(client.recv(HEADER).decode(FORMAT).strip())
            message = client.recv(msg_length).decode(FORMAT)
            parts = message.split("|")
            msg, received_time = parts[0], int(parts[1]) if len(parts) > 1 else None
            update_lamport_time(received_time)
            print(f"[SERVER]: {msg} (Logische Zeit: {lamport_time})")
        except Exception:
            print("[CLIENT] Verbindung zum Server verloren.")
            break


def main():
    server_list = discover_servers()
    if not server_list:
        print("[CLIENT] Keine Server gefunden.")
        return

    print("\n[CLIENT] Gefundene Server:")
    for idx, server in enumerate(server_list, 1):
        print(f"{idx}. {server}")

    try:
        choice = int(input("Server auswählen: ")) - 1
        server_info = server_list[choice].split(",")
        server_ip = server_info[1].split(":")[1]
        server_port = int(server_info[2].split(":")[1])
    except (IndexError, ValueError):
        print("[CLIENT] Ungültige Auswahl.")
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        try:
            client.connect((server_ip, server_port))
            print(f"[CLIENT] Verbunden mit: {server_ip}:{server_port}")
        except Exception:
            print("[CLIENT] Verbindung fehlgeschlagen.")
            return

        threading.Thread(target=receive_messages, args=(client,), daemon=True).start()
        threading.Thread(target=send_heartbeat, args=(client,), daemon=True).start()

        while True:
            msg = input("")
            if msg == DISCONNECTED_MESSAGE:
                print("[CLIENT] Chat wird verlassen.")
                send_message(client, msg)
                break
            elif msg.strip():
                send_message(client, msg)
            else:
                print("[CLIENT] Leere Nachrichten nicht erlaubt.")


if __name__ == "__main__":
    main()
