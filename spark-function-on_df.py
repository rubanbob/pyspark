# locate to local pyspark
import findspark
findspark.init(r'C:\spark\spark-3.1.1-bin-hadoop3.2')

# spark program begins
from pyspark.sql import SparkSession 
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate() 


def is_num_or_not(num_cols):
    return  F.concat_ws(',',
                    *[F.when(F.col(incol).cast('float').isNull(),F.concat(F.lit(incol),F.lit(" - not a number,")))
                    for incol in num_cols])

def is_unique(uniq_cols):
    return  F.concat_ws(',',
                    *[F.when((F.row_number().over(Window.partitionBy(F.col(incol)).orderBy(F.col('seq'))) != 1),F.concat(F.lit(incol),F.lit(" - not unique,")))
                    for incol in uniq_cols])

def is_valid_sex(track_details):
    return  F.concat_ws(',',
                    *[F.when(F.col(track_col).isin(track_values.split('-')),F.concat(F.lit(track_col),F.lit(" - not in list,")))
                    for track_col, track_values in track_details])


in_file = "./Data/Input/emp.csv"

in_df = spark.read.format("csv")\
            .option("header","true")\
            .load(in_file)
in_df.show()

key_cols = ['dept Id', 'emp no']
valid_sex = 'M-F'

new_df = in_df.withColumn('_dup_keys', F.row_number().over(Window.partitionBy([F.col(key_col) for key_col in key_cols]).orderBy(F.col('seq'))))

new_df = new_df.withColumn("messages",F.concat(is_num_or_not(['age','emp no']),
                                            is_unique(['emp no']),
                                            is_valid_sex([('sex',valid_sex.lower()),])))
new_df.show(truncate=False)