import socket
import threading
import uuid
import random
import time
import struct

# --- Konfigurationsvariablen ---
HEADER = 64
TCP_PORT = random.randint(5000, 6000)
BROADCAST_PORT = random.randint(6001, 7000)
BROADCAST_PORT_ZWEI = random.randint(6001, 7000)
BROADCAST_PORT_DREI = random.randint(6001, 7000)
MULTICAST_PORT = random.randint(6001, 7000)
MULTICAST_PORT_ZWEI = random.randint(6001, 7000)
MULTICAST_PORT_DREI = random.randint(6001, 7000)

MULTICAST_GROUP = "224.1.1.1"

FORMAT = 'utf-8'
DISCONNECTED_MESSAGE = "!LEAVE"
DISCOVERY_MESSAGE = "DISCOVER_CHAT_SERVER"
DISCOVERY_MESSAGE_SERVER = "DISCOVER_SERVER"
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15
global coordinator
coordinator = False 
server_id = str(uuid.uuid4())
server_rank = int(server_id.split('-')[0], 16)  
print(f"Serverrank: {server_rank}")
servers = {} 

udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
mreq = struct.pack("4sL", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
udp_server.bind(("", BROADCAST_PORT))

udp_server_zwei = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_server_zwei.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_server_zwei.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
udp_server_zwei.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
udp_server_zwei.bind(("", BROADCAST_PORT_ZWEI))

udp_server_drei = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_server_drei.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_server_drei.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
udp_server_drei.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
udp_server_drei.bind(("", BROADCAST_PORT_DREI))

tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_server.bind(("", TCP_PORT))

clients = []
heartbeat_tracker = {}
heartbeat_tracker_client = {}



def discover_servers():
    """
    Sucht alle Chat-Server im lokalen Netzwerk und gibt eine Liste mit deren Informationen zurück.
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client_socket.settimeout(5)

    try:
        for port in range(6001, 7000):  # Sende an mehrere Ports im definierten Bereich
            client_socket.sendto(DISCOVERY_MESSAGE_SERVER.encode(FORMAT), ("<broadcast>", port))
        print("[DISCOVER] Server werden gesucht...")

        while True:
            try:
                data, addr = client_socket.recvfrom(1024)
                decoded_data = data.decode(FORMAT)
                if decoded_data.startswith(f"SERVER_ID:{server_id}"):
                    continue  # Ignoriere eigene Nachrichten
                if addr not in servers.values():
                    servers[decoded_data] = addr
                    print(f"[SERVER] Neuer Server entdeckt: {decoded_data}")


            except socket.timeout:
                break
    finally:
        client_socket.close()
        print(f"[DISCOVER] Gefundene Server: {len(servers)}")
        print(servers)

    return servers  # Rückgabe sicherstellen

     




def listen_for_multicast(message, addr):
    global election_in_progress, new_coordinator, coordinator
    """
    Verarbeitung eingehender Nachrichten, inklusive Wahl- und Koordinator-Nachrichten.
    """
    if message.startswith("DISCOVER_SERVER"):
        response = f"SERVER_ID:{server_id},SERVER_IP:{addr[0]},PORT:{BROADCAST_PORT}"
        udp_server.sendto(response.encode(FORMAT), addr)
        print(f"[BROADCAST RESPONSE] Antwort {response} gesendet an {addr}")

    elif message.startswith("HEARTBEAT"):
        print(f"[HEARTBEAT] Empfangen von {addr}")
        # Speichere korrekte Struktur in `heartbeat_tracker`
        heartbeat_tracker[addr] = (0, time.time())

    elif message.startswith("ELECTION"):
        election_in_progress = True
        print("Election in progress")
        sender_rank = int(message.split('|')[1])
        print(f"[ELECTION] Wahl-Nachricht von Server mit Rang {sender_rank} erhalten.")

        if sender_rank < server_rank:
            print(f"[ELECTION] Server mit Rang {server_rank} übernimmt die Wahl.")
            udp_server.sendto(f"Rang hoeher".encode(FORMAT), addr)
            start_election()
        elif sender_rank > server_rank:
            print(f"Serverrank: {server_rank}")
            print(f"SenderranK: {sender_rank}")
            print(f"[ELECTION] Server mit Rang {sender_rank} akzeptiert. Stoppe lokale Wahl.")
            return

    elif message.startswith("COORDINATOR"):
        coordinator_msg = message.split('|')[1]
        print(f"[COORDINATOR] Neuer Koordinator, welcher die Wahl gewonnen hat: {coordinator_msg}")
        election_in_progress = False
    
    elif message.startswith("Rang"):
        print("Wahl verloren")
        new_coordinator = False
        coordinator = False
        time.sleep(20)
        main()





def handle_client(conn, addr):
    """
    Verwaltet die Kommunikation mit einem einzelnen Chat-Client.
    Unterstützt HEARTBEAT, ELECTION und COORDINATOR-Nachrichten.
    """
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
                    coordinator_msg = msg.split("|")[1]  # *NEU* Aktualisiere den globalen Koordinator
                    print(f"[NEW COORDINATOR] Neuer Koordinator: {coordinator_msg}")  # *NEU*

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



def handle_server():
    while True:
        try:
            # Nachricht lesen
            msg, msg_length = udp_server_drei.recvfrom(1024)
            if msg_length:
                msg_utf = msg.decode(FORMAT)
                #msg_length = int(msg_length)
                #msg = udp_server_drei.recvfrom(1024)
                print(msg_utf)
                if msg_utf.startswith("HEARTBEAT"):
                    _, lamport_time = msg_utf.split("|")
                    lamport_time = int(lamport_time)
                    heartbeat_tracker[msg_length] = (lamport_time, time.time())
                    print(f"[HEARTBEAT] {msg_length}: Logische Zeit = {lamport_time}")
                else:
                    print("Keine Heartbeat Message")
        except Exception as e:
            print(f"Server handeln ist fehlgeschlagen {e}")



def broadcast(message, sender_conn):
    """
    Sendet eine Nachricht an alle verbundenen Clients außer dem Absender.
    """
    for client in clients:
        if client != sender_conn:
            client.send(message)



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
"""
def monitor_heartbeats():
    global coordinator
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        current_time = time.time()
        for addr, last_time in list(heartbeat_tracker.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu {addr} abgebrochen. Starte nun Election")
                del heartbeat_tracker[addr]
                start_election()
"""
"""
def send_heartbeat_server():

    Sendet regelmäßig Heartbeat-Nachrichten mit logischem Timestamp an die Server.
        try:
        for port in range(6001, 7000):
            try:
                client_socket.sendto(DISCOVERY_MESSAGE_SERVER.encode(FORMAT), ("<broadcast>", port))



    global lamport_time
    lamport_time = 0
    while True:

        time.sleep(HEARTBEAT_INTERVAL)
        listing = discover_servers()
        print("ich will einen Heartbeat versenden")
        lamport_time += 1  # Logische Uhr erhöhen
        heartbeat_message = f"HEARTBEAT|{lamport_time}"
        for port in range(6001, 7000): #server in listing.values(): 
            try:
                
                server_details = server.split(",")
                server_ip = server_details[1].split(":")[1]
                server_port = int(server_details[2].split(":")[1])
                hallo = "Der Test vom Text"
                udp_server_drei.sendto(hallo.encode(FORMAT), (str(server_ip), server_port) )  # Heartbeat mit Lamport-Timestamp senden
                print( server_ip)
                print(server_port)
                
                udp_server_drei.sendto(heartbeat_message.encode(FORMAT), (MULTICAST_GROUP, port))
            except Exception as e:
                print(f"Fehler {e}")
                continue
        print("[KOORDINATOR] Heartbeat wurde an die anderen Server gesendet")
"""
def send_heartbeat():
    """
    Nur der Koordinator sendet regelmäßig Heartbeat-Nachrichten an alle bekannten Server.
    """
    while True:
        discover_servers()
        time.sleep(HEARTBEAT_INTERVAL)
        if coordinator:  # Nur senden, wenn dieser Server der Koordinator ist
            for server in servers.values():
                try:
                    server_ip, server_port = server
                    udp_server.sendto(f"HEARTBEAT|{server_rank}".encode(FORMAT), (server_ip, server_port))
                    print(f"[HEARTBEAT] Gesendet an {server_ip}:{server_port}")
                except Exception as e:
                    print(f"[ERROR] Fehler beim Senden der Heartbeat-Nachricht: {e}")

def monitor_heartbeats_clients():
    """
    Überwacht die Heartbeats der Clients und startet eine Wahl bei Koordinatorausfall.
    """
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        """
        if coordinator:  # *NEU* Prüft, ob ein Koordinator existiert
            if time.time() - heartbeat_tracker.get(coordinator, (0, 0))[1] > HEARTBEAT_TIMEOUT:  # *NEU*
                print(f"[COORDINATOR FAILURE] Koordinator {coordinator} nicht erreichbar.")  # *NEU*
                start_election()  # *NEU* Starte eine Wahl, da der Koordinator ausgefallen ist
 
        # Überprüfe Heartbeats anderer Verbindungen
        """
        current_time = time.time()
        for conn, (last_lamport_time, last_time) in list(heartbeat_tracker_client.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu Koordinator abgebrochen. Starte nun Election")  # *NEU*
                conn.close()
 
                if conn in clients:
                    clients.remove(conn)
                del heartbeat_tracker[conn]
"""        
def monitor_heartbeats_server():
    """"""
    Überwacht die Heartbeats des Koordinaten Servers und startet eine Wahl bei Koordinatorausfall.
   
    global coordinator
   
    while True:
        
        time.sleep(HEARTBEAT_INTERVAL)
        if not coordinator:  # *NEU* Prüft, ob ein Koordinator existiert
            if time.time() - heartbeat_tracker_server.get(coordinator, (0, 0))[1] > HEARTBEAT_TIMEOUT:  # *NEU*
                print(f"[COORDINATOR FAILURE] Koordinator {coordinator} nicht erreichbar.")  # *NEU*
                start_election()  # *NEU* Starte eine Wahl, da der Koordinator ausgefallen ist
    """"""
        # Überprüfe Heartbeats anderer Verbindungen
    while True:
        current_time = time.time()
        for conn, (last_lamport_time, last_time) in list(heartbeat_tracker_server.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu {conn} abgebrochen.")  # *NEU*
                print( current_time)
                print(last_time)
                print(current_time - last_time)
                if conn in servers:
                    servers.remove(conn)
                del heartbeat_tracker_server[conn]
                start_election()
"""


def monitor_heartbeats():
    """
    Überwacht die Heartbeats der Clients und startet eine Wahl bei Koordinatorausfall.
    """
    global coordinator, election_in_progress
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        current_time = time.time()
        for addr, (_, last_time) in list(heartbeat_tracker.items()):
            if current_time - last_time > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT TIMEOUT] Verbindung zu {addr} abgebrochen.")
                del heartbeat_tracker[addr]
                key_to_delete = None
                for key,value in servers.items():
                    if value == addr:
                        key_to_delete = key
                if key_to_delete:
                    del servers[key_to_delete]

                # Starte Wahl nur, wenn keine läuft
                if not election_in_progress:
                    election_in_progress = True
                    start_election()
                    #election_in_progress = False


def listen_for_messages():
    while True:
        try:
            message, addr = udp_server.recvfrom(1024)
            message = message.decode(FORMAT)
            print(f"[LISTENER] Nachricht erhalten: {message}")
            listen_for_multicast(message, addr)
        except Exception as e:
            print(f"[ERROR] Fehler beim Empfangen der Nachricht: {e}")
""" 
def listen_for_broadcast():
    """"""
    Hört auf Broadcast-Anfragen und antwortet mit Serverinformationen.
    """"""
    print(f"[BROADCAST LISTENING] Server wartet auf Broadcast-Anfragen auf Port {BROADCAST_PORT}...")
    while True:
        data, addr = udp_server.recvfrom(1024)
        if data.decode(FORMAT) == "DISCOVER_SERVER":
            print("DISCOVER SERVER Broadcast empfangen")
            response = f"SERVER_ID:{server_id},SERVER_IP:{socket.gethostbyname(socket.gethostname())},PORT:{BROADCAST_PORT}"
            udp_server.sendto(response.encode(FORMAT), addr)
            print(f"[BROADCAST RESPONSE] Antwort {response} gesendet an {addr}")

            # Sende aktuelle Koordinator-Informationen an den neuen Server
            if coordinator:
                coord_message = f"COORDINATOR|{coordinator}"
                udp_server.sendto(coord_message.encode(FORMAT), addr)
                print(f"[COORDINATOR INFO] Koordinator-Info an {addr} gesendet: {coord_message}")
"""

def filter_server_ip_port(server):
    try:
        # Splitte die Zeichenkette bei Kommas und suche nach dem Teil mit "SERVER_ID"
        parts = server.split(",")
        for part in parts:
            if part.startswith("SERVER_IP:"):
                split = str(part.split(":")[1])
            if part.startswith("PORT:"):
                split_1 = int(part.split(":")[1])
                # Extrahiere den Wert nach "SERVER_ID:"
        s = (split, split_1)
        return s
    except Exception as e:
        print(f"[ERROR] Fehler beim Extrahieren der SERVER_ID und Port: {e}")
        return None  # Falls keine SERVER_ID gefunden wird

def filter_server_rank(server):
    try:
        # Splitte die Zeichenkette bei Kommas und suche nach dem Teil mit "SERVER_ID"
        parts = server.split(",")
        for part in parts:
            if part.startswith("SERVER_ID:"):
                split = part.split(":")[1]
                split_1 = int(split.split("-")[0],16)
                # Extrahiere den Wert nach "SERVER_ID:"
                return split_1
    except Exception as e:
        print(f"[ERROR] Fehler beim Extrahieren der SERVER_ID: {e}")
        return None  # Falls keine SERVER_ID gefunden wird
"""
def listen_for_multicast():
    """"""
    Hört auf Broadcast-Anfragen und antwortet mit Serverinformationen.
    """"""
    print(f"[BROADCAST LISTENING] Server wartet auf Broadcast-Anfragen auf Port {BROADCAST_PORT}...")
    while True:

        data, addr = udp_server.recvfrom(1024)

        if data.decode(FORMAT) == "DISCOVER_SERVER":
                print("listen server broadcast ist weit")
                response = f"SERVER_ID:{server_id},SERVER_IP:{socket.gethostbyname(socket.gethostname())},PORT:{BROADCAST_PORT}"
                udp_server.sendto(response.encode(FORMAT), addr)
                print(f"[BROADCAST RESPONSE] Antwort gesendet an {addr}")
"""


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
    global coordinator,new_coordinator
    new_coordinator = True
    print(f"[ELECTION] Server mit Rang {server_rank} startet eine Wahl.")

    higher_servers = []

    # Sende Wahl-Nachricht an alle Server
    for server in servers.values():
        try:
            server_ip, server_port = server
            message = f"ELECTION|{server_rank}"
            udp_server.sendto(message.encode(FORMAT), (server_ip, server_port))
            print(f"[ELECTION] Nachricht an {server_ip}:{server_port} gesendet.")
        except Exception as e:
            print(f"[ERROR] Fehler beim Senden der Wahl-Nachricht: {e}")

    time.sleep(10)
    if new_coordinator == True:
        election_in_progress = False
        multicast_coordinator()




"""
    # Warte auf Antworten höherrangiger Server
    if higher_servers:
        time.sleep(10)  # Wartezeit auf Reaktion höherrangiger Server
        if coordinator:
            print(f"[ELECTION] Wahl beendet. Neuer Koordinator: {coordinator}")
            return

    # Werde Koordinator, wenn keine Antworten kommen
    coordinator_id = server_id
    #print(f"[COORDINATOR] Server mit Rang {server_rank} ist jetzt Koordinator.")
    multicast_coordinator() 
"""
def multicast_coordinator():
    """
    Sendet eine Nachricht, um den neuen Koordinator bekanntzugeben.
    """
    global coordinator
    print(f"[COORDINATOR] Bekanntgabe: Server mit Rang {server_rank} ist neuer Koordinator.")
    for server in servers.values():
        try:
            #server_ip, server_port = filter_server_ip_port(server)
            server_ip, server_port = server
            message = f"COORDINATOR|{server_id}"
            udp_server.sendto(message.encode(FORMAT), (server_ip, server_port))
        except Exception as e:
            print(f"[ERROR] Fehler beim Senden der Koordinator-Nachricht: {e}")
    coordinator = True
    main()
    

      
def main():
    global coordinator
    print(f"[STARTING] Chat-Server wird gestartet... Server-ID: {server_id}")
    print(f"[SERVER INFO] TCP-Port: {TCP_PORT}, Broadcast-Port: {BROADCAST_PORT}")
    print(f"[USP] Server lauscht auf {udp_server}")
    print("STARTE MAIN")
    global election_in_progress
    election_in_progress = False

    # Starte den Listener-Thread
    listener_thread = threading.Thread(target=listen_for_messages)
    listener_thread.start()

    # Starte den Broadcast-Listener-Thread
    #broadcast_thread = threading.Thread(target=listen_for_broadcast)
    #broadcast_thread.start()

    # Starte den Heartbeat-Monitor-Thread
    #heartbeat_monitor_thread = threading.Thread(target=monitor_heartbeats)
    #heartbeat_monitor_thread.start()

    # Starte den Wahl-Thread, falls nötig
    if True:
        
        listing = discover_servers()
        if not listing or coordinator:  # Falls discover_servers keine Server gefunden hat
            print("Keine Server gefunden. Ich werde Koordinator.")
            coordinator = True
            # Starte den Heartbeat-Thread
            heartbeat_thread = threading.Thread(target=send_heartbeat)
            heartbeat_thread.start()
            heartbeat_monitor_clients_thread = threading.Thread(target=monitor_heartbeats_clients)
            heartbeat_monitor_clients_thread.start()
            start_tcp_server()
        else:
            print("Server gefunden. Ich bin nicht der Koordinator.")
            coordinator = False
            handle_server_thread = threading.Thread(target=handle_server, daemon=True)
            handle_server_thread.start()
            monitor_heartbeats()



if __name__ == "__main__":
    main()
