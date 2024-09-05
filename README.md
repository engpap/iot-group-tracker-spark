## Running the simulation
Start the simulation on a terminal:
```python
python3 test/tester.py
```

## Running the main app
Run the server on a different terminal:
```python
python3 app/main.py
```
This will:
- Start the Flask server in `app.py` exposing the `/updates` endpoint.
- Start a Spark Streaming job in `tracker.py` that reads from the local storage in the `INPUT_DIR` directory and processes the data storing the output in `OUTPUT_DIR`

## Running the analytics
To get the moving average, run:
```python
python3 app/moving_average.py
```
This will process the data using Spark and store the output Parquet files in the `OUTPUT_DIR` directory.
To read the output data in a human-readable table format, run the REST file `test/GET_moving_average.rest`. This will create a `data/results/moving_avg_{timestamp}` file in the `RESULTS_DIR` directory.

To get the top nationalities, run:
```python
python3 app/top_n.py
```
This will read the ouput processed Parquet files in the `OUTPUT_DIR` directory and process them again (i.e., ranking) to get the top-N nationalities.
To read the data in a human-readable table format, run the REST file `test/GET_top_n.rest`. This will create a `data/results/top_nationalities_{timestamp}` file in the `RESULTS_DIR` directory.