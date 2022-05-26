import sys
from pyspark.sql import SparkSession
noOfCores = sys.argv[1]
outFile = sys.argv[2]

spark = SparkSession \
    .builder \
    .appName("pysark") \
    .master("local[{}]".format(int(noOfCores))) \
    .getOrCreate()

df = spark.read.format("csv").load("airports.csv", header='true')


countByCountry = df.groupBy("COUNTRY").count()
#
# df1 = df.groupBy("COUNTRY").count()
# df1.orderBy(df1['count'].desc()).show(1)

countByCountry.toPandas().to_csv(outFile,index=False)