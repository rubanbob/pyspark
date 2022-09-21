# locate to local pyspark
import findspark
findspark.init('C:\opt\spark\spark-3.1.3-bin-hadoop2.7')
#findspark.init('C:\opt\spark\spark-2.4.7-bin-hadoop2.7')

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window


def scd2_delta_load(hist_df, temp_df, file_date):

    # Records to mark as deleted
    del_cond = 'row_md5'
    delete_records_df = hist_df.join(other=temp_df, on=del_cond, how="left_anti")\
                        .select("*")\
                        .withColumn("updated_on",
                            f.when(f.col("is_active") == 'Y', f.lit(file_date))
                                .otherwise(f.col("updated_on")))\
                        .withColumn("is_active",
                            f.when(f.col("is_active") == 'Y', f.lit("N"))
                                .otherwise(f.col("is_active")))\
                        .cache()
    #delete_records_df.show(truncate=False)

    # New incoming Records
    new_cond = [((temp_df.row_md5 == hist_df.row_md5) &\
                    (hist_df.is_active == 'Y') )]
    new_records_df = temp_df.filter("is_active  == 'Y'")\
                        .join(other=hist_df, on=new_cond, how="left_anti")\
                        .select("*")\
                        .cache()

    #new_records_df.show(truncate=False)

    
    # No changed Records
    noc_cond = [((temp_df.row_md5 == hist_df.row_md5) &\
                    (temp_df.is_active == 'Y') )]
    noc_records_df =   hist_df.alias('hist').join(other=temp_df, on=noc_cond, how="inner")\
                            .select("hist.*")\
                            .cache()
    #noc_records_df.show(truncate=False)

    # target Records

    target_hist_df = noc_records_df\
                        .unionByName(new_records_df)\
                        .unionByName(delete_records_df)

    target_hist_df.show(truncate=False)


spark = SparkSession.builder \
            .getOrCreate()

hist_df = spark.createDataFrame(
    [
        (0, "u0","D1","pk0","rh0","N","Day1","Day2"), # deleted on file2 
        (1, "u1","D1","pk1","rh1","N","Day1","Day2"), 
        (2, "u2","D1","pk2","rh2","Y","Day1","Day1"),  # will delete in file3
        (3, "u3","D1","pk3","rh3","Y","Day1","Day1"), 
        (6, "u3","D1","pk3","rh3","N","Day1","Day1"),  # dup inactive
        (4, "u4","D1","pk4","rh4","Y","Day1","Day1"), 
        (5, "u5","D1","pk5","rh5","Y","Day2","Day2"), # new on file2 
        (1, "u1","D2","pk1","rh6","Y","Day2","Day2"), # updated on file2
    ],
    ["row_no", "user","department","pk_md5","row_md5","is_active","created_on","updated_on"]  # column names 
)

#hist_df.show(truncate=False)

temp_df = spark.createDataFrame(
    [  
        (1, "u1","D2","pk1","rh6","Y","Day3","Day3"),
        (3, "u3","D1","pk3","rh3","Y","Day3","Day3"), 
        (4, "u4","D1","pk4","rh4","Y","Day3","Day3"), 
        (5, "u6","D1","pk6","rh7","Y","Day3","Day3"), # new rec - diff rowno
        (6, "u5","D2","pk5","rh8","Y","Day3","Day3"), # updated D2 - ins
        (7, "u4","D1","pk4","rh4","Y","Day3","Day3"), # exit rec dup
        (8, "u7","D1","pk7","rh9","Y","Day3","Day3"), # new rec dup - ins
        (9, "u7","D1","pk7","rh9","Y","Day3","Day3"), # new rec dup
        (10, "u0","D1","pk0","rh0","Y","Day3","Day3"), # re-inserted deleted rec
    ],
    ["row_no", "user","department","pk_md5","row_md5","is_active","created_on","updated_on"]  # column names 
)

#de-duplciating temp file
dup_win_spec  = Window.partitionBy(f.col('row_md5')).orderBy(f.col('row_no'))
temp_df = temp_df.withColumn("is_active",
                    f.when(f.row_number().over(dup_win_spec) != 1,"N")\
                        .otherwise("Y"))
#temp_df.show(truncate=False)

scd2_delta_load(hist_df,temp_df,"Day3")

