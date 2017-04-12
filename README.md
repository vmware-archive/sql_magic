sql_magic
=========

Jupyter magic for writing SQL to interact with Spark and SQL databases. Query results are saved directly to a pandas dataframe.

* Execute SQL from a Jupyter cell, returning the result as a Pandas dataframe
* Python variables can be embedded directly inside SQL code using string formatting { }
* Asynchronous query execution
* Browser-based notifications for completed queries
* SQL syntax highlighting


```
%%readsql df_result
SELECT *
FROM table_name
WHERE age < {threshold}
```

See the included [Jupyter notebook](https://github.com/crawles/sql_magic/blob/master/sql_magic%20API.ipynb) for examples and a tutorial.

## Install

`pip install sql_magic`

## Supported backends

`sql_magic` works with both Apache Spark and common SQL databases. Supported objects are:

* Apache Spark: `SparkSession`, `SQLContext`, `HiveContext`
* SQLAlchemy Engine (supports PostgreSQL, MySQL, SQLite, Oracle, etc.)
* Any connection object that follows the Python DB API 2.0 specification (E.g., psycopg2)

## Usage

### Quick example
```
# example for connecting to spark
config SQLConn.output_result = 'spark'
```
```
%%readsql df_result
SELECT *
FROM table_name
```

### Connecting to Spark

```
val spark = SparkSession
   .builder()
   .appName("SparkSessionZipsExample")
   .config("spark.sql.warehouse.dir", warehouseLocation)
   .enableHiveSupport()
   .getOrCreate()

config SQLConn.output_result = 'spark'
```

### Connecting to a SQL database

```
from sqlalchemy import create_engine
from sqlite3 import dbapi2 as sqlite
sqllite_engine = create_engine('sqlite+pysqlite:///test.db', module=sqlite)

%config SQLConn.conn_object_name='sqllite_engine'
```

```
%%readsql
SELECT sqlite_version();
```


## Configuration 

%config SQLConn.output_result = False
%config SQLConn.notify_result = False

%config SQLConn

SQLConn options
-------------
SQLConn.conn_object_name=<Unicode>
    Current: u'conn'
    Object name for accessing computing resource environment
SQLConn.notify_result=<Bool>
    Current: True
    Notify query result to stdout
SQLConn.output_result=<Bool>
    Current: True
    Output query result to stdout
