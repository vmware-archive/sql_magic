try:
    # Import PySpark modules here
    import findspark
    findspark.init()
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    pass

def create_spark_conn():
    # note: may need to clear lock: rm metastore_db/*lck
    conf = SparkConf()
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "1")
    conf.set("spark.app.name", "nosetest")
    SparkSession._instantiatedContext = None
    # need to enable hive support to create table for tests
    spark = SparkSession.builder\
                        .config(conf=conf)\
                        .enableHiveSupport()\
                        .getOrCreate()
    return spark
