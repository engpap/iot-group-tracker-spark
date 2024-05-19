from flask import Flask, request, jsonify
import json
import os
import threading


app = Flask(__name__)
counter = 0 
counter_lock = threading.Lock() 

@app.route('/updates', methods=['POST'])
def handle_updates():
    global counter  # to modify the global variable
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid data"}), 400
    print("DEBUG: Received data with timestamp: ", data.get("timestamp"), "\n")
    msg = json.dumps(data)
    #print(f"Writing: {msg.strip()}\n")

    # generate a unique filename
    with counter_lock:  # acquire the lock before modifying the counter
        filename = os.path.join(os.getenv("INPUT_DIR"), f"data_{counter}.json")
        with open(filename, 'w') as f:
            f.write(msg + '\n')
        counter += 1 
    
    return jsonify({"message": "Data received"}), 200