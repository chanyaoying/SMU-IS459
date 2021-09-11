import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName('RDD Quiz 2').getOrCreate()

# Load parquet file into a data frame
df = spark.read.load("/user/yaoying/parquet-input/hardwarezone.parquet")

# count content length
df = df.rdd.map(lambda x: (x[1], len(x[2].strip().split()), 1)).toDF(["author", "contentLength", "postCount"])

# group by author, summing the other values
df = df.groupby(["author"]).agg(
	sum("contentLength").alias("totalContentLength"),\
	sum("postCount").alias("totalPostCount")
)

# get average post length
df.rdd.map(lambda x: (x[0], x[1] / x[2])).toDF(["Name", "Average Post Lenght"]).show()
