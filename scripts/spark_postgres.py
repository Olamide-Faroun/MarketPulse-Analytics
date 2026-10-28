
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

database_password = os.getenv("POSTGRES_PASSWORD")

if not database_password:
    raise ValueError("POSTGRES_PASSWORD is missing")

# Create Spark session
spark = (
    SparkSession.builder
    .appName("MarketPulsePostgreSQL")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.14"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# PostgreSQL connection settings
database_url = (
    "jdbc:postgresql://localhost:5433/marketpulse_db"
)

database_properties = {
    "user": "marketpulse_user",
    "password": database_password,
    "driver": "org.postgresql.Driver"
}

# Check database connection
stock_prices = spark.read.jdbc(
    url=database_url,
    table="stock_prices",
    properties=database_properties
)

stock_prices.printSchema()

print("Records in PostgreSQL:", stock_prices.count())

spark.stop()
