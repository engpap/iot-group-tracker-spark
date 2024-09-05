import socket
import time
from datetime import datetime
import random
import json
from dotenv import load_dotenv
import os
import requests
import csv 

PATH_CSV = '/Users/dre/Dev/polimi/iot-group-tracker-spark/data/test/test.csv'
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

def save_to_csv(data):

    with open(PATH_CSV, mode='a', newline='') as file:
        writer = csv.writer(file)
        for participant in data['participants']:
            writer.writerow([data['timestamp'], participant['device_id'], participant['nationality'], participant['age']])


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
            save_to_csv(data)
            print(f"Server Response: {response.json()}\n")
        else:
            print(f"Failed to send data. Status code: {response.status_code}\n")
        
        time.sleep(30) # send data every 30 seconds


if __name__ == "__main__":
    load_dotenv()
    # clear data in CSV
    if os.path.exists(PATH_CSV):
        os.remove(PATH_CSV)
        print(f"CSV file {PATH_CSV} cleared.\n\n")
    else:
        os.makedirs(os.path.dirname(PATH_CSV), exist_ok=True)
        print(f"Created directory for CSV file: {os.path.dirname(PATH_CSV)}\n\n")
    send_data()

