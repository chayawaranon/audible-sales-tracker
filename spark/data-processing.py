from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import pandas as pd
import requests

# API usd to thb conversion
URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

# data path in GCS
AUDIBLE_DATA = 'gs://asia-east2-simple-pipeline-8aa38b84-bucket/data/raw-data/audible_data.csv'
AUDIBLE_DATA_TRANSACTION = 'gs://asia-east2-simple-pipeline-8aa38b84-bucket/data/raw-data/audible_data_transaction.csv'

# create spark session
spark = SparkSession.builder.getOrCreate()

# read csv
sdf_data = spark.read.csv(AUDIBLE_DATA, header = True, inferSchema = True)
sdf_transaction = spark.read.csv(AUDIBLE_DATA_TRANSACTION, header = True, inferSchema = True)

# create temp view for SQL operation
sdf_data.createOrReplaceTempView("sdf_data")
sdf_transaction.createOrReplaceTempView("sdf_transaction")

# join table
sdf_joined = spark.sql("SELECT * FROM sdf_data d LEFT JOIN sdf_transaction t ON d.Book_ID = t.book_id")

# change data type string to timestamp
sdf_joined_clean = sdf_joined.withColumn("timestamp", f.to_timestamp(sdf_joined.timestamp, 'yyyy-MM-dd HH:mm:ss'))
# create new column as a key to join API table
sdf_joined_clean = sdf_joined_clean.withColumn("date", f.to_date(sdf_joined_clean.timestamp, 'yyyy-MM-dd'))

# get API
result_conversion_rate = requests.get(URL).json()
# create pandas dataframe
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})

# convert to spark dataframe
sdf_con_rate = spark.createDataFrame(conversion_rate)
sdf_con_rate = sdf_con_rate.withColumn("date", f.to_date(sdf_con_rate.date, 'yyyy-MM-dd'))

# join table
sdf_final = sdf_joined_clean.join(sdf_con_rate, sdf_joined_clean.date == sdf_con_rate.date, how='left') 
# change column name
sdf_final = sdf_final.withColumnRenamed('Total No. of Ratings', 'total_num_ratings')
# remove $ in column Price and cast to type double
sdf_final = sdf_final.withColumn("Price", f.translate(f.col("Price"), "$", "").cast("double"))
# create new column
sdf_final = sdf_final.withColumn("THBPrice", f.col("Price") * f.col("conversion_rate"))
# drop date column cuz it no need to use anymore
sdf_final = sdf_final.drop("date")

# rename column
column = ["Book Title", "Book Subtitle", "Book Author", "Book Narrator", "Audio Runtime"]
for col in column:
    sdf_final = sdf_final.withColumnRenamed(col, "_".join(col.split()))

# clean text
sdf_final = sdf_final.withColumn("Country", f.when(sdf_final['Country'] == 'Japane', 'Japan').otherwise(sdf_final['Country']))
sdf_final = sdf_final.filter(sdf_final["user_id"].rlike("^[a-z0-9]{8}$"))
sdf_final = sdf_final.withColumn("user_id", f.when(sdf_final['user_id'].isNull(), '00000000').otherwise(sdf_final['user_id']))

# replace NULL values
sdf_final = sdf_final.na.fill("N/A")
sdf_final = sdf_final.na.fill(0)

# save to parquet format and write to gcs
sdf_final.write.save("gs://asia-east2-simple-pipeline-8aa38b84-bucket/data/tranformed-data/parquet", format="parquet")