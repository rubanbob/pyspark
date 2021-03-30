# locate to local pyspark
import findspark
findspark.init(r'C:\spark\spark-3.1.1-bin-hadoop3.2')

# spark program begins
import datetime
from pyspark.sql import SparkSession
import jaydebeapi


def main(jdbc_driver, jdbc_url, jdbc_uname, jdbc_pass, query):
    try:
        # create JDBC connection
        conn = jaydebeapi.connect(jdbc_driver,
                                jdbc_url,
                                {'user' : jdbc_uname, "password": jdbc_pass},
                                )
        # init Cursor
        curs = conn.cursor()
        try:
            # execute update/delete query
            curs.execute(query)

            curs.close()
            conn.close()
        except Exception as e:
            conn.close()
            print("SQL query execution failed : ",e)

    except Exception as e:
        print("DB connection failed : ",e)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    print("Starting JDBC conneciton..")

    # inputs
    jdbc_driver = "org.jdbc.driver.OracleDriver"
    jdbc_url = "jdbc:oracle:thin:@<host>:<port>:<SID>"
    jdbc_uname = "oralce"
    jdbc_pass = "oracle-password"

    query =  "UPDATE table TABLENAME SET COL='VALUE' WHERE SEL_COL='SEL_VAL'"

    print("Spark update table start : ", datetime.datetime.now())
    main(jdbc_driver, jdbc_url, jdbc_uname, jdbc_pass, query)
    print("Spark update table  end : ",datetime.datetime.now())

    spark.stop()


# SPARK SUBMIT WITH BELOW commands
'''
spark2-submit --name app-name-ruban \
--master yarn \
--deploy-mode cluster \
--jars ./jars/ojdbc8.jar \
--conf spark.driver.extraClassPath=ojdbc8.jar \
--conf spark.executor.extraclassPath=ojdbc8.jar \
/path/to/code/spark-update-RDBMS.py
'''