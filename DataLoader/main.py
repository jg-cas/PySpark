import json
from pyspark.sql import SparkSession
from ETL import CSVClass

print("Starting DataLoader...")

session = SparkSession.builder.appName("DataLoader").getOrCreate()
print("Spark Session created")

with open("configs.json") as source_file:
    data = json.load(source_file)
    for v in data.values():
        if v['class'] == "csv":
            try:
                print(f"Processing file with path: {v['path']}")
                CSVClass.execute(session, v['path'], v['header'], v['inferSchema'], v['delimiter'])
                break
            except Exception as ex:
                print("Error:", ex)

print(f"Stopping Spark Session...")
session.sparkContext.stop()
