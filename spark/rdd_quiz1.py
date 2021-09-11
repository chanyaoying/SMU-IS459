import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min

spark = SparkSession.builder.appName('RDD Exercise').getOrCreate()

# Load CSV file into a data frame
score_sheet_df = spark.read.load('/user/yaoying/parquet-input/score-sheet.csv', \
    format='csv', sep=';', inferSchema='true', header='true')

# Get max and min
maximum = score_sheet_df.select([max('Score')]).collect()[0][0]
minimum = score_sheet_df.select([min('Score')]).collect()[0][0]

# remove max and min from df
no_minmax = score_sheet_df.filter((score_sheet_df.Score > minimum) & (score_sheet_df.Score < maximum))

# project the second column of scores with an additional 1
no_minmax = no_minmax.rdd.map(lambda n: (n[1], 1))

# get the sum and the count by reduce
sum, count = no_minmax.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

print(f"Average score: {sum/count}")


