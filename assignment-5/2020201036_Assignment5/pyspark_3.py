import sys
from pyspark.sql import SparkSession
noOfCores = sys.argv[1]
outFile = sys.argv[2]

"""
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .getOrCreate()
print(spark.version)
"""
spark = SparkSession \
    .builder \
    .appName("pysark") \
    .master("local[{}]".format(int(noOfCores))) \
    .getOrCreate()
print(spark.version)


df = spark.read.format("csv").load("airports.csv", header='true')

lat1 = df.filter("LATITUDE >= 10")
lat2 = lat1.filter("LATITUDE <= 90")
long1 = lat2.filter("LONGITUDE <= -10")
long2 = long1.filter("LONGITUDE >= -90")
airportInRange = long2.select(long2["NAME"])

airportInRange.toPandas().to_csv(outFile,index=False)