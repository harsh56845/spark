import socket
import json
import time
import random
from datetime import datetime

HOST = "localhost"
PORT = 9999

users = ["user1", "user2", "user3"]
actions = ["open_app", "view_restaurant", "add_to_cart", "order"]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

print("Sending data...")

while True:
    data = {
        "user_id": random.choice(users),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": random.choice(actions)
    }

    message = json.dumps(data)

    try:
        s.sendall((message + "\n").encode("utf-8"))
        print("Sent:", message)
    except Exception as e:
        print("Error:", e)
        break

    time.sleep(random.randint(1, 3))