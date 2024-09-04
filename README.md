Start the simulation on a terminal:
```python
python3 test/tester.py
```

Run the server on a different terminal:
```python
python3 app/main.py
```
This will:
- Start the Flask server in `app.py` exposing the `/updates` endpoint.
- Start a Spark Streaming job in `tracker.py` that reads from the local storage in the `INPUT_DIR` directory and processes the data storing the output in `OUTPUT_DIR`

To get the moving average, run:
```python
python3 app/moving_average.py
```

To get the top nationalities, run:
```python
python3 app/top_n.py
```