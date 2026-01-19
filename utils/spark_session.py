from pyspark.sql import SparkSession
import os

import sys

def get_spark():
    # This must be set before SparkSession is created or initialized fully
    current_dir = os.path.dirname(os.path.abspath(__file__))
    hadoop_bin_dir = os.path.join(current_dir, 'hadoop', 'bin')
    os.environ['HADOOP_HOME'] = os.path.join(current_dir, 'hadoop')
    os.environ['PATH'] = hadoop_bin_dir + os.pathsep + os.environ['PATH']

    # Set Python executables to ensure the same environment is used
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Initialize Spark Session
    spark = SparkSession.builder \
    .appName('Apple_ELT_Pipeline') \
    .config('spark.sql.warehouse.dir', os.path.join(current_dir, 'spark-warehouse')) \
    .config('spark.driver.host', 'localhost') \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .getOrCreate()

    return spark


