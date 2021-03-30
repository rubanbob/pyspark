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
    jdbc_driver = "oracle.jdbc.driver.OracleDriver"
    jdbc_url = "jdbc:oracle:thin:@<host>:<port>:<SID>"
    jdbc_uname = "oralce"
    jdbc_pass = "oracle-password"

    query =  "UPDATE table TABLENAME SET COL='VALUE' WHERE SEL_COL='SEL_VAL'"

    print("Spark update table start : ", datetime.datetime.now())
    main(jdbc_driver, jdbc_url, jdbc_uname, jdbc_pass, query)
    print("Spark update table  end : ",datetime.datetime.now())

    spark.stop()


# SPARK SUBMIT WITH BELOW commands, install jaydebeapi in venv(my_venv) - and place packed venv in HDFS lcoation
'''
spark2-submit --name app-name-ruban \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./my_venv/bin/python \
--conf spark.yarn.dist.archives=hdfs:///my_hdfs_path/project/venv/my_venv.tar.gz#my_venv \
--master yarn \
--deploy-mode cluster \
--jars ./jars/ojdbc8.jar \
--conf spark.driver.extraClassPath=ojdbc8.jar \
--conf spark.executor.extraclassPath=ojdbc8.jar \
/path/to/code/spark-update-RDBMS.py
'''