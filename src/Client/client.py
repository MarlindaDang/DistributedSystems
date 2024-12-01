import socket
import threading

# --- Konfigurationsvariablen ---
HEADER = 64
BROADCAST_PORT = 6001  # Annahme: Suchen beginnt ab einem festen Broadcast-Port
FORMAT = 'utf-8'
DISCOVERY_MESSAGE = "DISCOVER_CHAT_SERVER"
DISCONNECTED_MESSAGE = "!LEAVE"


def discover_servers():
    """
    Sucht alle Chat-Server im lokalen Netzwerk und gibt eine Liste mit deren Informationen zurück.
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client_socket.settimeout(5)

    server_list = []
    try:
        # Broadcast-Nachricht senden, um Server zu entdecken
        for port in range(6001, 7001):  # Sucht auf allen möglichen Broadcast-Ports
            try:
                client_socket.sendto(DISCOVERY_MESSAGE.encode(FORMAT), ("<broadcast>", port))
            except Exception:
                continue

        print("[CLIENT] Chat-Server werden gesucht...")

        # Mehrere Antworten vom Server sammeln
        while True:
            try:
                data, addr = client_socket.recvfrom(1024)
                server_list.append(data.decode(FORMAT))
            except socket.timeout:
                break  # Keine weiteren Antworten nach Timeout
    finally:
        # Socket schließen
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
    Empfängt Nachrichten vom Server und gibt sie in Echtzeit aus.
    """
    while True:
        try:
            message = client.recv(2048).decode(FORMAT)
            print(f"\n{message}")
        except OSError:
            break


def main():
    """
    Hauptfunktion des Clients.
    - Sucht alle Server
    - Ermöglicht die Auswahl eines Servers
    - Stellt die Verbindung her
    - Sendet und empfängt Nachrichten
    """
    # Server im lokalen Netzwerk suchen
    server_list = discover_servers()
    if not server_list:
        print("[CLIENT] Keine Chat-Server gefunden. Beende Client.")
        return

    # Gefundene Server anzeigen und Benutzer zur Auswahl auffordern
    print("\nGefundene Server:")
    for idx, server_info in enumerate(server_list, start=1):
        print(f"{idx}. {server_info}")

    choice = int(input("Wählen Sie einen Server aus (Nummer eingeben): ")) - 1
    selected_server = server_list[choice]

    # Server-Details extrahieren
    server_details = selected_server.split(",")
    server_ip = server_details[1].split(":")[1]
    server_port = int(server_details[2].split(":")[1])

    # Verbindung zum ausgewählten Server herstellen
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(10)

    try:
        client.connect((server_ip, server_port))
        print(f"[CLIENT] Mit dem Chat-Server verbunden: {server_ip}:{server_port}")
    except (ConnectionRefusedError, socket.timeout):
        print("[CLIENT] Verbindung zum Server fehlgeschlagen. Bitte später erneut versuchen.")
        return

    # Thread für das Empfangen von Nachrichten starten
    threading.Thread(target=receive_messages, args=(client,), daemon=True).start()

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

    # Verbindung schließen
    client.close()


if __name__ == "__main__":
    main()
