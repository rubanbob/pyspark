# locate to local pyspark
import findspark
findspark.init(r'C:\spark\spark-3.1.1-bin-hadoop3.2')

# dependencies
findspark.add_packages(["com.databricks:spark-xml_2.12:0.12.0"])

# spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 spark_code.py

# spark program begins
from pyspark.sql import SparkSession 
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate() 
spark.conf.set('spark.sql.caseSensitive', True)

# input xml file
source_location = "./Data/Input/complex-xml.xml"

# to create hive table
tableName = "db_name.tst_table_name"

def flatten_xml(unflattened_df): 
    print("Inside flattening function")

    main_df = unflattened_df
    for col_name, col_type in unflattened_df.dtypes:
        if col_type[:6] == 'struct': 
            main_df = flatten_df(main_df)

        if col_type[:5] == 'array':
            main_df = explode_array(main_df, col_name)

    # print to see change in schema for each iteration
    #main_df.printschema()

    struct_types = [c[0] for c in main_df.dtypes if c[1][:6] == 'struct']
    array_types  = [c[0] for c in main_df.dtypes if c[1][:5] == 'array']
    
    # struct or array still left in tree repeat opt
    if len(struct_types) > 0 or len(array_types) > 0:
        main_df.cache()
        flatten_xml(main_df)

    else:
        # when no more struct or array left in tree 
        main_df.cache() 
        main_df.printSchema()
        main_df.show()

        # create output file (overwrite will always delete the existing file) 
        main_df.coalesce(1).write.format('csv')\
                            .mode('overwrite')\
                            .option("header", "true")\
                            .save('./Data/Output/xml')
        
        # create hive table (overwrite will always tuncate the table) 
        # main_df.write.mode("overwrite").saveAsTable(tableName)

        return main_df


def flatten_df(nested_df):
    print("Visited flatten df")
    flat_cols   = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']   # get all non-struct colum_name 
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']   # get column names with struct type

    # for each child in struct flattern it
    flat_df = nested_df.select(flat_cols + [F.col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
    
    return flat_df

def explode_array(nested_df, column): 
    print("Visited array explode: ", column)
    explode_col_name = nested_df.select(column)

    # using explode_outer so that NULL column values are not dropped
    exploded_df = nested_df.withColumn(column, F.explode_outer(explode_col_name[0]))

    return exploded_df

if __name__ == "__main__":
    xml_data = spark.read.format("com.databricks.spark.xml")\
                    .option("rootTag", "xml").option("rowTag", "catalog")\
                    .load(source_location)

    # .option("valueTag", "_Default_value")\
    # .option("attributePrefix", "$")\

    # print source file tree structure
    xml_data.printSchema()

    final_df = flatten_xml(xml_data)