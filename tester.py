import socket
import time
from datetime import datetime

data = [
    "add,1,1,US,25",
    "add,1,2,US,30",
    "add,1,3,CA,22",
    "add,1,4,CA,26",
    "add,1,5,UK,29",
    "add,1,6,UK,27"
]

fixed_data = [
    # timestamp, action, group_id, device_id, nationality, age
    "2024-05-05 10:00:00,add,1,1,US,10",
    "2024-05-05 10:00:00,add,1,10,IT,100",
    "2024-05-06 10:00:00,add,1,2,US,20",
    "2024-05-06 10:00:00,add,1,20,IT,200",
    "2024-05-07 10:00:00,add,1,3,US,30",
    "2024-05-07 10:00:00,add,1,30,IT,300",
]

send_fixed_data = False


def send_data(host='localhost', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen(1)
        print(f"Listening on {host}:{port}")
        
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            while True:
                if send_fixed_data:
                    for entry in fixed_data:
                        message = f"{entry}\n"
                        print(f"Sending: {message.strip()}")
                        conn.sendall(message.encode())
                        time.sleep(5)  # send data every 5 seconds
                else:
                    for entry in data:
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{current_time},{entry}\n"
                        print(f"Sending: {message.strip()}")
                        conn.sendall(message.encode())
                        time.sleep(10)  # send data every 5 seconds

if __name__ == "__main__":
    send_data()

'''
EXPECTED
------------------------------------------- 
Batch: 6
-------------------------------------------
+------------------------------------------+-----------+--------+
|window                                    |nationality|avg(age)|
+------------------------------------------+-----------+--------+
|{2024-04-28 19:00:00, 2024-05-05 19:00:00}|IT         |100.0   |
|{2024-04-28 19:00:00, 2024-05-05 19:00:00}|US         |10.0    |
|{2024-04-29 19:00:00, 2024-05-06 19:00:00}|IT         |150.0   |
|{2024-04-29 19:00:00, 2024-05-06 19:00:00}|US         |15.0    |
|{2024-04-30 19:00:00, 2024-05-07 19:00:00}|IT         |200.0   |
|{2024-04-30 19:00:00, 2024-05-07 19:00:00}|US         |20.0    |
'''