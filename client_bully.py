import socket
import threading
import time
import random  # *NEU* Für die Rangzuweisung

# --- Konfigurationsvariablen ---
HEADER = 64
BROADCAST_PORT = 6001
FORMAT = 'utf-8'
DISCOVERY_MESSAGE = "DISCOVER_CHAT_SERVER"
DISCONNECTED_MESSAGE = "!LEAVE"
HEARTBEAT_INTERVAL = 5  # Sekunden

lamport_time = 0  # Logische Uhr des Clients
coordinator = None  # *NEU* Aktueller Koordinator
client_rank = random.randint(1, 100)  # *NEU* Zufälliger Rang des Clients für den Bully-Algorithmus


def discover_servers():
    """
    Sucht alle Chat-Server im lokalen Netzwerk und gibt eine Liste mit deren Informationen zurück.
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client_socket.settimeout(5)

    server_list = []
    try:
        for port in range(6001, 7001):
            try:
                client_socket.sendto(DISCOVERY_MESSAGE.encode(FORMAT), ("<broadcast>", port))
            except Exception:
                continue

        print("[CLIENT] Chat-Server werden gesucht...")

        while True:
            try:
                data, addr = client_socket.recvfrom(1024)
                server_list.append(data.decode(FORMAT))
            except socket.timeout:
                break
    finally:
        client_socket.close()

    return server_list


def send(msg, client):
    """
    Sendet eine Nachricht an den Chat-Server.
    """
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)


def receive_messages(client):
    """
    Empfängt Nachrichten vom Server und verarbeitet sie.
    """
    global coordinator
    while True:
        try:
            message = client.recv(2048).decode(FORMAT)
            print(f"\n{message}")

            # *NEU* Verarbeitung von Wahl- und Koordinatornachrichten
            if message.startswith("ELECTION"):
                handle_election_message(message, client)
            elif message.startswith("COORDINATOR"):
                coordinator = message.split("|")[1]
                print(f"[CLIENT] Neuer Koordinator ist: {coordinator}")
        except OSError:
            break


def handle_election_message(message, client):  # *NEU*
    """
    Verarbeitet eine Wahl-Nachricht vom Server.
    """
    global client_rank
    sender_rank = int(message.split("|")[1])
    if client_rank > sender_rank:  # Falls der Client einen höheren Rang hat, sendet er zurück
        print(f"[ELECTION] Client mit Rang {client_rank} reagiert auf Wahl.")
        send(f"ELECTION|{client_rank}", client)
    else:
        print(f"[ELECTION] Client ignoriert Wahl von Rang {sender_rank}.")


def send_heartbeat(client):
    """
    Sendet regelmäßig Heartbeat-Nachrichten mit logischem Timestamp an den Server.
    """
    global lamport_time
    while True:
        try:
            lamport_time += 1  # Logische Uhr erhöhen
            heartbeat_message = f"HEARTBEAT|{lamport_time}"
            send(heartbeat_message, client)  # Heartbeat mit Lamport-Timestamp senden
            time.sleep(HEARTBEAT_INTERVAL)
        except Exception:
            print("[CLIENT] Verbindung verloren.")
            break


def main():
    """
    Hauptfunktion des Clients.
    """
    global coordinator

    server_list = discover_servers()
    if not server_list:
        print("[CLIENT] Keine Chat-Server gefunden. Beende Client.")
        return

    print("\nGefundene Server:")
    for idx, server_info in enumerate(server_list, start=1):
        print(f"{idx}. {server_info}")

    choice = int(input("Wählen Sie einen Server aus (Nummer eingeben): ")) - 1
    selected_server = server_list[choice]

    server_details = selected_server.split(",")
    server_ip = server_details[1].split(":")[1]
    server_port = int(server_details[2].split(":")[1])

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(10)

    try:
        client.connect((server_ip, server_port))
        print(f"[CLIENT] Mit dem Chat-Server verbunden: {server_ip}:{server_port}")
    except (ConnectionRefusedError, socket.timeout):
        print("[CLIENT] Verbindung zum Server fehlgeschlagen.")
        return

    # *NEU* Initialisierung des Koordinators
    coordinator = f"{server_ip}:{server_port}"
    print(f"[CLIENT] Aktueller Koordinator: {coordinator}")

    threading.Thread(target=receive_messages, args=(client,), daemon=True).start()
    threading.Thread(target=send_heartbeat, args=(client,), daemon=True).start()

    print("Du bist dem Chat beigetreten. Gib '!LEAVE' ein, um den Chat zu verlassen.")
    while True:
        message = input("Deine Nachricht: ")
        try:
            send(message, client)
        except BrokenPipeError:
            print("[CLIENT] Verbindung zum Server verloren.")
            break
        if message == DISCONNECTED_MESSAGE:
            print("[CLIENT] Verlasse den Chat.")
            break

    client.close()


if __name__ == "__main__":
    main()
