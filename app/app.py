from flask import Flask, request, jsonify
import json
import os
import threading
from top_n import TopNationalities
from moving_average import MovingAverage

app = Flask(__name__)
counter = 0 
counter_lock = threading.Lock() 

@app.route('/updates', methods=['POST'])
def handle_updates():
    print("\n\n")
    global counter  # to modify the global variable
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid data"}), 400
    print("DEBUG: Received data with timestamp: ", data.get("timestamp"))
    msg = json.dumps(data)

    # generate a unique filename
    with counter_lock:  # acquire the lock before modifying the counter
        input_dir = os.getenv("INPUT_DIR")
        os.makedirs(input_dir, exist_ok=True)  # create folder if it doesn't exist
        filename = os.path.join(input_dir, f"data_{counter}.json")
        with open(filename, 'w') as f:
            f.write(msg + '\n')
        print(f"DEBUG: Data file saved at: {filename}")
        counter += 1 
    
    return jsonify({"message": "Data received"}), 200

@app.route('/top_n')
def handle_top_n():
    TopNationalities.run()
    return jsonify({"message": "Top nationalities calculated"}), 200

@app.route('/moving_average')
def handle_moving_average():
    MovingAverage.run()
    return jsonify({"message": "Moving average calculated"}), 200