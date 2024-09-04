from tracker import Tracker
from config import CONFIG
from app import app
import threading

if __name__ == "__main__":
    # start the Flask app (SERVER)
    flask_thread = threading.Thread(target=lambda: app.run(debug=True, use_reloader=False))
    flask_thread.start()

    # start the Spark Tracker
    Tracker.run(CONFIG) 
