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

tmpDf = df.groupBy("COUNTRY").count()
maxCount = tmpDf.orderBy(tmpDf['count'].desc()).limit(1)

maxCount.toPandas().to_csv(outFile,index=False)