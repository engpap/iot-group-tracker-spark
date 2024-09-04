# PySpark Docs

A batch is printed out every time a packet is received from the socket.

### withColumn
```DataFrame.withColumn(colName, col)```
- Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
- The col expression must be an expression over this DataFrame.


## NOTES
- 🔵 DEBUG: Error running TrackerTopN: [NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING] Window function is not supported in RANK(PERCENTAGE_INCREASE#115) (as column `rank`) on streaming DataFrames/Datasets. Structured Streaming only supports time-window aggregation using the WINDOW function. (window specification: (PARTITION BY END_TIME ORDER BY PERCENTAGE_INCREASE DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))

- Saving output of queries in csv format is not good because it stores only one row per file. It's better to use console format, capture it and store it in a file.

- To store the output of a PySpark Structured Streaming job in both Parquet and console mode, you need to create two different queries. However, you can't create two queries on the same stream. 