from flask import Flask, jsonify, request
import time
import base64
import struct
import queue
import threading
import random
from first_lora_user.loramodem import LoRaModem
app = Flask(__name__)
KEY_PUBLICATION_CHNACE = 0.1
# users_registry stores the full object required by the /users endpoint
# key: key_id (string), value: dictionary with key_id, nick, name, phone, delay_secs, is_local
users_registry = {}
users_lock = threading.Lock()
#stores all incoming messages to local users
message_inbox = {}
inbox_lock = threading.Lock()

#All stats for /stats endpoint
server_stats = {
    "local_messages_sent": 0,
    "global_messages_sent": 0,
    "valid_alive_requests": 0,
    "users_requests_count": 0
}
stats_lock = threading.Lock()

# Messages waiting to be sent over the LoRa radio
lora_out_queue = queue.PriorityQueue()


#REST API BACKEND:
class ServerBE:
    """Contains all helper functions to the main HTTP requests recievers"""
    @staticmethod
    def route_http_message(msg_data : dict):
        """
        Routes a message based on the 'to' key_id.
        """
        target_key_id = msg_data.get('to')
        
        # Find if the user is local by checking if he's in the inbox
        with inbox_lock:
            if target_key_id in message_inbox:
                # LOCAL ROUTE: Put in the inbox
                message_inbox[target_key_id].append(msg_data)
                with stats_lock:
                    server_stats["local_messages_sent"] += 1
                return "local"
        # REMOTE ROUTE: Put in LoRa Priority Queue
        if not LoRaWriter.serialize_lora_message(msg_data):
            return "error"
        return "remote"
        
    @staticmethod
    def get_key_id_from_b64(key_b64):
        """
        Extracts the 4-byte key_id from the Base64 public key.
        The first 4 bytes are the key_id.
        """
        try:
            public_bytes = base64.b64decode(key_b64)
            # Unpack the first 4 bytes as a 32-bit big-endian integer
            key_id = struct.unpack("!I", public_bytes[:4])[0]
            return key_id
        except Exception:
            return None
        
    @staticmethod
    def handle_alive_calls(data):
        """Handles all operations to be executed by /alive call"""
        key_b64 = data["key_b64"]
        key_id = ServerBE.get_key_id_from_b64(key_b64)
        with stats_lock:
            server_stats["valid_alive_requests"] += 1

        # Update registry with all fields required by the /users GET request
        with users_lock:
            users_registry[key_id] = {
                "key_id": key_id,
                "key_b64": key_b64,
                "nick": data.get('nick', ""),
                "name": data.get('name', ""),
                "phone": data.get('phone', ""),
                "delay_secs": 0,      # Default for local check-in
                "is_local": True      # This user is hitting our REST API directly
            }
        with inbox_lock:
            message_inbox[key_id] = []
        if random.random() < KEY_PUBLICATION_CHNACE:
            LoRaWriter.publish_key(key_b64)
        # Retrieve messages for this specific user
        with inbox_lock:
            return message_inbox.get(key_id, [])


@app.route('/text', methods=['POST'])
@app.route('/plain', methods=['POST'])
def handle_message():
    data = request.get_json()
    
    # Validate all required fields appear
    required_fields = ['utime', 'sender', 'to']
    if not all(keys in data for keys in required_fields):
        return jsonify({"status": "error", "error": "Missing mandatory fields"}), 400

    # Ensure it has either encrypted or plain payload
    if 'crypt2_b64' not in data and 'plain2_b64' not in data:
        return jsonify({"status": "error", "error": "No message content found"}), 400

    # Route the message
    ServerBE.route_http_message(data)
    
    return jsonify({
        "status": "OK",
    })

@app.route('/alive', methods=['POST'])
def alive():
    """
    POST request representing the status of a single user.
    Updates user info and returns pending messages.
    """
    data = request.get_json()

    # key_b64 is the only mandatory field
    if not data or 'key_b64' not in data:
        return jsonify({
            "status": "error",
            "error": "Invalid key_b64"
        }), 400

    key_b64 = data.get('key_b64')
    key_id = ServerBE.get_key_id_from_b64(key_b64)
    if key_id is None:
        return jsonify({"status": "error", "error": "Malformed key_b64"}), 400

    #handle all operations related to /alive endpoint
    messages = ServerBE.handle_alive_calls(data)
    return jsonify({
        "status": "OK",
        "messages": messages
    })

@app.route('/users', methods=['GET'])
def get_users():
    """
    Returns a list of all known users (local and remote).
    As per documentation, this is a GET request returning a JSON array of objects.
    """
    with stats_lock:
        server_stats["users_requests_count"] += 1
    with users_lock:
        return jsonify({
            "status": "OK",
            "users": list(users_registry.values()), 
        })

@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Returns router statistics.
    """
    with users_lock, stats_lock:
        return jsonify({
            "status": "OK",
            "local_messages": server_stats["local_messages_sent"],
            "global_messages": server_stats["global_messages_sent"],
            "alive_requests": server_stats["valid_alive_requests"],
            "total_users": len(users_registry),
            "users_endpoint_calls": server_stats["users_requests_count"]
        })

#MESSAGE RECEPTION CODE
class LoRaListener:
    """Endlessly reads all incoming messages and manages them"""
    @staticmethod
    def handle_incoming_key_publish(packet):
        try:
            if len(packet) != 134:
                print(f"[ERROR] Received packet of length {len(packet)}, should be 134")
                return
            utime, sender_id = struct.unpack("!II", packet[2:10])
            # Extract the full 128-byte key (starts at index 6 in the packet)
            full_key_bytes = bytes(packet[6:134])
            key_b64 = base64.b64encode(full_key_bytes).decode('utf-8')
            # Update the registry as a REMOTE user
            # Remote users have is_local=False and no specific nick/name yet

            current_time = int(time.time())
            delay = max(0, current_time - utime)
            with users_lock:
                if sender_id not in users_registry:
                    users_registry[sender_id] = {
                        "key_id": sender_id,
                        "key_b64": key_b64,
                        "nick": "", 
                        "name": "",
                        "phone": "",
                        "delay_secs": delay,
                        "is_local": False 
                    }
                    print(f"[DEBUG] Registered new remote user: {sender_id}")
                else:
                    users_registry[sender_id]["delay_secs"] = delay
                    
        except Exception as e:
            print(f"[ERROR] Unexpected error parsing Key Publish: {e}")

    @staticmethod
    def handle_incoming_text(packet):
        try:
            # Unpack the fixed header part (14 bytes)
            # !B (Header) B (Kind) I (Utime) I (Sender) I (To)
            if len(packet) < 14:
                print(f"[ERROR] Received packet of length {len(packet)}, should be at least 14")
                return
            kind, utime, sender_id, to_id = struct.unpack("!BIII", packet[1:14])
            

            # The rest of the packet is the payload
            payload_bytes = packet[14:]
            payload_b64 = base64.b64encode(payload_bytes).decode('utf-8')

            # Build the message object
            msg_data = {
                "utime": utime,
                "sender": sender_id,
                "to": to_id,
            }

            if kind == 0x03:
                msg_data["crypt2_b64"] = payload_b64
            else:
                msg_data["plain2_b64"] = payload_b64
                print(payload_bytes.decode('utf-8'))

            print(f"[DEBUG] Received Text Kind {kind} from {sender_id} to {to_id}")
            
            # Route it (local inbox or remote queue)
            with users_lock:
                if to_id in users_registry:
                    with inbox_lock:
                        message_inbox[to_id].append(msg_data)

        except Exception as e:
            print(f"[ERROR] Unexpected error parsing incoming text: {e}")

    @staticmethod
    def read_incoming_messages(modem : LoRaModem):
        """
        Reads a packet from the LoRa modem and handles it.
        """
        packet = modem.read_bytes()
        if not packet:
            return
        # Check for the constant header '\xAE' and minimum length
        # Every incoming message must have: header (1), kind (1), utime (1), sender (1)
        if len(packet) < 10 or packet[0] != 0xAE:
            print("[ERROR] packet doesnt start with ae / packet is too short")
            return

        kind = packet[1]
        if kind == 0x01:  # Key publish
            LoRaListener.handle_incoming_key_publish(packet)
        elif kind in [0x03, 0x05]:
            LoRaListener.handle_incoming_text(packet)

    @staticmethod
    def lora_recieve_loop(device_path):
        with LoRaModem(device_path) as modem:
            while True:
                LoRaListener.read_incoming_messages(modem)
                time.sleep(0.1)

class LoRaWriter:
    """Handles inserting packets into the queue and sending the queue's elements"""
    def serialize_lora_message(msg_data):
        """
        Turns a JSON message from the API into a bytes object for LoRa transmission.
        Manages both kind 5 - plain, and kind 3 - encrypted text.
        """
        try:
            if 'crypt2_b64' in msg_data:
                kind = 0x03
                payload_b64 = msg_data['crypt2_b64']
            else:
                kind = 0x05
                payload_b64 = msg_data['plain2_b64']

            payload_bytes = base64.b64decode(payload_b64)

            # AE (1 byte), Kind (1 byte), UTime (4 bytes), Sender (4 bytes), To (4 bytes)
            header = struct.pack('!BBIII', 
                                0xAE, 
                                kind, 
                                int(msg_data.get('utime', time.time())), 
                                msg_data['sender'], 
                                msg_data['to'])

            print(f"[DEBUG] Sent text message from {msg_data['sender']} to {msg_data['to']}")
            lora_out_queue.put((2, header + payload_bytes))
            return True
        except Exception as e:
            print(f"[ERROR] Error serializing message: {e}")
            return False
        
    def publish_key(key_b64):
        """
        Composes key publication message and puts it in the queue.
        """
        try:
            public_bytes = base64.b64decode(key_b64)
            
            if len(public_bytes) != 128:
                print("[ERROR] Public key must be exactly 128 bytes")
                return False

            ae_const = 0xAE
            kind = 0x01  # Key Publish
            current_utime = int(time.time())

            sender_key_id = struct.unpack("!I", public_bytes[:4])[0]

            key2 = public_bytes[4:]
            packet = struct.pack("!BBII124s", 
                                ae_const, 
                                kind, 
                                current_utime, 
                                sender_key_id, 
                                key2)
            
            lora_out_queue.put((1, packet)) #change later
            print(f"[DEBUG] Key Publish packet for ID {sender_key_id} added to queue.")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to publish key: {e}")
            return False
        

    def lora_worker(device_path):
        """
        Worker who writes to the modem the first message in the queue
        """
        try:
            with LoRaModem(device_path) as modem:
                modem.configure_packet_mode()
                print(f"[DEBUG] LoRa Worker started on {device_path}")

                while True:
                    priority, raw_bytes = lora_out_queue.get()
                    print("[DEBUG] worker got a message")
                    if isinstance(raw_bytes, bytes):
                        modem.write_bytes(raw_bytes)
                        with stats_lock:
                            server_stats["global_messages_sent"] += 1
                        print(f"[DEBUG] Worker sent {len(raw_bytes)} bytes. Priority: {priority}. Message kind {int(raw_bytes[1])}")
                    else:
                        print("[ERROR] non byte message in queue")
                    lora_out_queue.task_done()
        except Exception as e:
            print(f"[ERROR] LoRa Worker crashed: {e}")

if __name__ == '__main__':
    threading.Thread(target=LoRaListener.lora_recieve_loop, args=("sim",), daemon=True).start()
    # threading.Thread(target=LoRaWriter.lora_worker, args=("sim",), daemon=True).start()
    app.run(host='127.0.0.1', port=8200)
