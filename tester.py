import socket
import time
from datetime import datetime
import random
import json
from dotenv import load_dotenv
import os
import requests


# pools for generating random data
device_id_pool = [i for i in range(1, 20)]
nationality_pool = ["US", "CA", "UK", "IT", "FR", "DE", "ES", "JP", "KR", "CN"]
age_pool = [i for i in range(10, 100)]
random.seed(42)

def generate_json():
    res = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "participants": []
    }
    for _ in range(random.randint(1, 3)):
        participant = {
            "device_id": random.choice(device_id_pool),
            "nationality": random.choice(nationality_pool),
            "age": random.choice(age_pool)
        }
        res["participants"].append(participant)
    return res


def send_data():
    server_url = os.getenv('SERVER_URL')
    print("🔵 DEBUG: Server URL: ", server_url)
    headers = {'Content-Type': 'application/json'} # otherwise server returns 415 error
    while True:
        data = generate_json()
        msg = json.dumps(data)
        print(f"Sending: {msg.strip()}\n")
        response = requests.post(server_url, headers=headers, data=msg)

        if response.status_code == 200:
            print(f"Server Response: {response.json()}\n")
        else:
            print(f"Failed to send data. Status code: {response.status_code}\n")
        
        time.sleep(10) # send data every 30 seconds


if __name__ == "__main__":
    load_dotenv()
    send_data()