import json
from pyspark.sql import SparkSession
from Configurations import CSVClass

# prints to be substituted by logging
print("Starting DataLoader...")

session = SparkSession.builder.appName("DataLoader").getOrCreate()
print("Spark Session created")

# Reading configurations from json for scalability
# Modify path to file as necessary
with open("source.json") as source_file:
    data = json.load(source_file)
    for v in data.values():
        # Factory
        if v['class'] == "csv":
            try:
                print(f"Processing file with path: {v['path']}")
                CSVClass.execute(session, v['path'], v['header'], v['inferSchema'], v['delimiter'])
                break
            except Exception as ex:
                print("Error:", ex)

print(f"Stopping Spark Session...")
session.sparkContext.stop()
