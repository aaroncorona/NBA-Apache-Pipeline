# NBA-Apache-Pipeline
A data pipeline that processes a CSV file of all NBA players from the 2015-16 season. 

This uses 3 frameworks from Apache:
1. Spark (Scala) for a batch job of extraction and processing.
2. Kafka (Scala) for streaming a subset of the data.
3. Airflow (Python) for triggering the job.

The data processed is from this CSV: https://github.com/laxmimerit/All-CSV-ML-Data-Files-Download/blob/master/nba.csv
