#!/bin/bash
set -e # kill shell on any error

venv="/home/ruban/MyEnv"
cd $venv

echo "Check for existing python virtual environment"
if [ -d $venv/spark-env ]
then 
    echo "purge existing virtual env"
    rm -rf spark-env
fi

# create python virtual env
echo "Creating python virtual environment"
virtualenv -p /usr/bin/python spark-env
 
# install dependencies from requirements file
echo "Intalling dependencies"
source spark-env/bin/activate 
pip install -r requirements.txt 
deactivate

# zip virtual env for export
echo "Ziping venv" 
cd spark-env
tar -hzcf ../spark-env.tar.gz 
cd ..

# upload packed venv to HDFS 
echo "Uploading to HDFS"
hdfs dfs -put -f spark-env.tar.gz /home/ruban/my_project/venv
hdfs dfs -chmod 777 /home/ruban/my_project/venv/spark-env.tar.gz

echo "Removing Local zip file"
rm -rf "$venv"/spark-env.tar.gz

############## spark-submit with below command ############## 
# spark2-submit \
# --conf spark.yarn.dist.archives=hdfs:///home/ruban/my_project/venv/spark-env.tar.gz#spark-env \
# --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./spark-env/bin/python \
# --master yarn \
# --deploy-mode cluster \
# spark_JDBC.py

# GOOD article to refer : https://databricks.com/blog/2020/12/22/how-to-manage-python-dependencies-in-pyspark.html
