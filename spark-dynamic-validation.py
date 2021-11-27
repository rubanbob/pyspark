
# locate to local pyspark
import findspark
findspark.init('C:\opt\spark\spark-3.1.1-bin-hadoop2.7')

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import re

spark = SparkSession.builder.getOrCreate()

source_location =  "./data/emp_details.csv"

original_df = spark.read.format("csv")\
    .option("header", "true") \
    .option('encoding', 'UTF-8')\
    .option("multiline","true")\
    .option("delimiter", ",")\
    .option("quote", '"')\
    .option("escape", '"')\
    .load(source_location)

#original_df.show(10,False)
#original_df.printSchema()

cond1 = """case 
            when `dept_id` in (123,345) and dept IN ("finance", "payroll") then NULL
                when `dept_id` IN (155) and  COMMENTS is NULL then NULL
                    when dept_id =6 then NULL
                        else "failed for rule 1"
                    end"""
cond2 = """case 
            when `dept_id` in (123,345) and dept IN ("finance", "payroll") then NULL
                when `dept_id` IN (155) and  COMMENTS is NULL then NULL
                    when dept_id =6 then NULL
                        else "failed for rule 1"
                    end"""
# form list on all availabe case statement
list_case = [cond1,cond2]

exprs = [f.expr(x) for x in list_case]

# run all expression against given column
original_df.select("*",f.concat_ws(",",*exprs).alias('val_result')).show(10,False)          

# TO error line no
original_df.withColumn('val_result', f.when(f.col('val_result') != "",
                                 f.concat(f.col('line_no'), f.lit(':'), f.col('val_result')))).show(10,False)


