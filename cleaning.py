from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize a Spark session
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# Load the CSV data into DataFrames
listing_df = spark.read.csv('raw_data/listings.csv', header=True, inferSchema=True)
reviews_df = spark.read.csv('raw_data/reviews.csv', header=True, inferSchema=True)
calendar_df = spark.read.csv('raw_data/calendar.csv', header=True, inferSchema=True)

# Transform the data

# LISTINGS DATA

# Drop the 'summary' column
listing_df = listing_df.drop('summary')

# Convert 'host_is_superhost' to boolean
listing_df = listing_df.withColumn('host_is_superhost', when(col('host_is_superhost') == 't', True).otherwise(False))

# Drop 'country' and 'market' columns
listing_df = listing_df.drop('country', 'market')

# Drop rows with null values in the 'space' column
listing_df = listing_df.na.drop(subset=['space'])

# Fill null values in 'host_about' with "No description"
listing_df = listing_df.na.fill({'host_about': 'No description'})

# CALENDAR DATA

# Fill null values in 'price' column with "Booked"
calendar_df = calendar_df.na.fill({'price': 'Booked'})

# Convert 'available' to boolean
calendar_df = calendar_df.withColumn('available', when(col('available') == 't', True).otherwise(False))

# REVIEW DATA

# No transformation needed

# SAVE DATA

listing_df.write.csv('cleaned_listing_df.csv', header=True, mode='overwrite')
calendar_df.write.csv('cleaned_calendar_df.csv', header=True, mode='overwrite')
reviews_df.write.csv('cleaned_reviews_df.csv', header=True, mode='overwrite')

# Stop the Spark session
spark.stop()
