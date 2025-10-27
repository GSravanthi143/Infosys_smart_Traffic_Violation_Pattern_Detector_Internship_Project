from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, when, count, split
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Initialize Spark
spark = SparkSession.builder.appName("TrafficViolation_LargeDataset").getOrCreate()

# Define schema
schema = StructType([
    StructField("violation_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("violation_type", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("severity", IntegerType(), True)
])

# Simulate traffic violation data (10,000+ rows)
violation_types = ["Speeding", "Red Light", "Illegal Turn", "Illegal Parking", "Unknown"]
vehicle_types = ["Car", "Truck", "Motorcycle", "Bus", None]
locations = ["INT001", "INT002", "INT003", "INT004", "INT005",
             "40.7128,-74.0060", "34.0522,-118.2437", "51.5074,-0.1278", "48.8566,2.3522"]

num_rows = 10000
data = []

for i in range(num_rows):
    # Random timestamp within last 90 days
    ts = datetime.now() - timedelta(days=random.randint(0, 90), hours=random.randint(0,23), minutes=random.randint(0,59))
    # Introduce some malformed timestamps (~5%)
    if random.random() < 0.05:
        ts = "malformed"
    data.append({
        "violation_id": f"V{i:05d}",
        "timestamp": ts,
        "location": random.choice(locations),
        "violation_type": random.choice(violation_types),
        "vehicle_type": random.choice(vehicle_types),
        "severity": random.randint(1, 5)
    })

# Convert to pandas DataFrame and save CSV/JSON
df_pd = pd.DataFrame(data)
csv_file = "traffic_violations_large.csv"
json_file = "traffic_violations_large.json"
df_pd.to_csv(csv_file, index=False)
df_pd.to_json(json_file, orient="records", lines=True)

# Function to read CSV/JSON
def read_data(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    elif ext == ".json":
        df = spark.read.json(file_path, schema=schema)
    else:
        raise ValueError("Unsupported file format. Use CSV or JSON.")
    return df

# Read the generated CSV
df_spark = read_data(csv_file)

# Data cleaning
df_clean = df_spark.fillna({"vehicle_type": "Unknown"})
df_clean = df_clean.filter(col("violation_id").isNotNull())

# Convert timestamps, invalid ones become null
df_clean = df_clean.withColumn(
    "timestamp",
    to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
)

# Keep only valid violation types
valid_types = ["Speeding", "Red Light", "Illegal Turn", "Illegal Parking"]
df_clean = df_clean.filter(col("violation_type").isin(valid_types))

# Parse latitude/longitude if location contains coordinates
df_clean = df_clean.withColumn(
    "latitude",
    when(col("location").contains(","), split(col("location"), ",").getItem(0).cast(DoubleType()))
)
df_clean = df_clean.withColumn(
    "longitude",
    when(col("location").contains(","), split(col("location"), ",").getItem(1).cast(DoubleType()))
)

# Summary report
print("\n--- LARGE DATASET SUMMARY REPORT ---\n")
total_rows = df_clean.count()
print(f"Total rows after cleaning: {total_rows}")

print("\nMissing/Null values per column:")
df_clean.select([count(when(col(c).isNull(), c)).alias(c) for c in df_clean.columns]).show()

print("\nViolation type distribution:")
df_clean.groupBy("violation_type").count().show()

print("\nVehicle type distribution:")
df_clean.groupBy("vehicle_type").count().show()

lat_count = df_clean.filter(col("latitude").isNotNull()).count()
lon_count = df_clean.filter(col("longitude").isNotNull()).count()
print(f"\nRows with latitude populated: {lat_count}")
print(f"Rows with longitude populated: {lon_count}")

# Export cleaned data to Parquet
output_path = "cleaned_traffic_violations_large.parquet"
df_clean.write.mode("overwrite").parquet(output_path)

print("\nLarge dataset simulation, ingestion, cleaning, location parsing, summary report, and Parquet export complete!")
print("\nColumns in cleaned DataFrame:", df_clean.columns)

# Stop Spark
spark.stop()