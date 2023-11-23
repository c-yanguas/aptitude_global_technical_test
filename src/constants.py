import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

class DATASET:
    URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    ROOT_DIR = f"{os.path.dirname(os.path.realpath(__file__))}/../datasets"

    class YELLOW_TAXI:
        PATTERN = r'(yellow_tripdata)_(\d{4})-(\d{2})\.parquet'
        SQUEMA = StructType([
            StructField("VendorID",              IntegerType(), True),
            StructField("tpep_pickup_datetime",  TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("Passenger_count",       IntegerType(), True),
            StructField("Trip_distance",         FloatType(), True),
            StructField("RateCodeID",            IntegerType(), True),
            StructField("Store_and_fwd_flag",    StringType(), True),
            StructField("PULocationID",          IntegerType(), True),
            StructField("DOLocationID",          IntegerType(), True),
            StructField("Payment_type",          IntegerType(), True),
            StructField("Fare_amount",           FloatType(), True),
            StructField("Extra",                 FloatType(), True),
            StructField("MTA_tax",               FloatType(), True),
            StructField("Improvement_surcharge", FloatType(), True),
            StructField("Tip_amount",            FloatType(), True),
            StructField("Tolls_amount",          FloatType(), True),
            StructField("Total_amount",          FloatType(), True),
            StructField("Congestion_Surcharge",  FloatType(), True),
            StructField("Airport_fee",           FloatType(), True)
        ])
    
    
    class GREEN_TAXI:
        PATTERN = r'(green_tripdata)_(\d{4})-(\d{2})\.parquet'
    
    class HIGH_VOLUME_FOR_HIRE_VEHICLE:
        PATTERN = r'(fhvhv_tripdata)_(\d{4})-(\d{2})\.parquet'
    
    class FOR_HIRE_VEHICLE:
        PATTERN = r'(fhv_tripdata)_(\d{4})-(\d{2})\.parquet'



class JOKES:
    NUM_THREADS = 8