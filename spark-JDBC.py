# locate to local pyspark
import findspark
findspark.init(r'C:\spark\spark-3.1.1-bin-hadoop3.2')

# spark program begins
import sys
import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, TimestampType, DateType

# User Inputs
url = "jdbc:<type>://<ip-server>:<port>/<dbname>"
user = "username"
password = "password"
table = "tablename"

keycolumn = "split-by-column"

driver = "connection_driver" 
jar_path = './jars/connection_jar'

# output
out_dir = "./Data/Output/JDBC/unload"

# 
def get_table_info():
    min_max_query = "select min(%s) as min, max(%s) as max from %s"%(keycolumn, keycolumn, table)

    min_max_df = spark.read.format("jdbc")\
                            .option("url", url)\
                            .option("query", min_max_query)\
                            .option("user", user)\
                            .option("password", password)\
                            .option("driver", driver)\
                            .load().cache()

    #min_max_df.printSchema()

    supported_types = (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, TimestampType, DateType)
    col_supported = ([f.dataType for f in min_max_df.schema.fields if isinstance(f.dataType, supported_types)])

    if col_supported:
        min_max = min_max_df.first() 
        return min_max[0], min_max[1]
    else:
        print("Partition column type should be numeric, date, or timestamp.")
        spark.stop()
        sys.exit(2)

if __name__ == '__main__':
    spark = SparkSession.builder\
                        .config("spark.driver.extraClassPath", jar_path)\
                        .config("spark.executor.extraclassPath", jar_path)\
                        .getOrCreate()

    min_val, max_val = get_table_info()

    print("Min value: %s, Max value : %s"%(min_val, max_val))

    unload_df = spark.read.format("jdbc")\
                        .option("url", url)\
                        .option("dbtable", table)\
                        .option("user", user)\
                        .option("password", password)\
                        .option("driver", driver)\
                        .option("numPartitions", 4)\
                        .option("partitionColumn", keycolumn)\
                        .option("fetchsize", 1000)\
                        .option("lowerBound", min_val)\
                        .option("upperBound", max_val)\
                        .load()

    #unload_df.printSchema ()

    print("Spark unload start : ", datetime.datetime.now())
    unload_df.write.format('csv').mode('overwrite').option("header", "true").save(out_dir)
    print("Spark unload end : ",datetime.datetime.now())

    spark.stop()