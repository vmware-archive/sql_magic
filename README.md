sql_magic
=========

sql_magic is Jupyter magic for writing SQL to interact with Spark (or Hive) and relational databases. Query results are saved directly to a Pandas dataframe.

```
%%read_sql df_result
SELECT *
FROM table_name
WHERE age < {threshold}
```

<p>
  <img src="https://raw.githubusercontent.com/crawles/Logos/master/sql_magic_wide.png" width = 100%>
</p>


The sql_magic library expands upon existing libraries such as [ipython-sql](https://github.com/catherinedevlin/ipython-sql) with the following features: 
* Support for both Apache Spark and relational database connections
* Asynchronous execution (useful for long queries)
* Browser notifications for query completion
* Results directly returned as Pandas dataframes 

See the included [Jupyter notebook](https://github.com/crawles/sql_magic/blob/master/sql_magic%20API.ipynb) for examples and a tutorial.

## Installation

`pip install sql_magic`

## Usage: Execute SQL on a relational database

Relational databases can be accessed using [SQLAlchemy](https://www.sqlalchemy.org/) or libraries implementing the [Python DB 2.0 Specification](https://www.python.org/dev/peps/pep-0249/) (E.g., `psycopg2`, `sqlite3`, etc.).

~~~
# create SQLAlchemy engine for postgres
from sqlalchemy import create_engine
postgres_engine = create_engine('postgresql://{user}:{password}@{host}:5432/{database}'.format(**connect_credentials))
~~~

The sql_magic library is loaded using the `%load_ext` iPython extension syntax and is pointed to the connection object as follows: 

~~~
%load_ext sql_magic
%config SQL.conn_name = 'postgres_engine'
~~~

Python variables can be directly referenced in the SQL query using the string formatting syntax: 

~~~
# variables for use in SQL query
table_name = 'titanic'
cols = ','.join(['age','sex','fare'])
~~~

SQL code is executed with the %read_sql cell magic. A browser notification containing the execution time and result dimensions will automatically appear once the query is finished.

~~~
%%read_sql df_result
SELECT {cols}
FROM {table_name}
WHERE age < 10
~~~

SQL syntax is colored inside Jupyter: 

<img src='https://github.com/crawles/Logos/blob/master/sql_magic_syntax.png?raw=true'>

A browser notification is displayed upon query completion.

<img src='https://github.com/crawles/Logos/blob/master/notification_example.png?raw=true'>


Queries can be run again additional connection objects (Spark, Hive or relational db connections) with the `-c` or `--connection` flag:

~~~
#sql_magic supports libraries following Python DB 2.0 Specification
import psycopg2
conn2 = psycopg2.connect(**connect_credentials)
~~~

~~~
%%read_sql df_result -c conn2
SELECT {cols}
FROM {table_name}
WHERE age < 10
~~~


The code can be executed asynchronously using the -a flag. Asynchronous execution is particularly useful for running long queries in the background without blocking iPython kernel.

~~~
%%read_sql df_result -a
~~~

Since results are automatically saved as a Pandas dataframe, we can easily visualize our results using the built-in Pandas’ plotting routines:

~~~
df.plot('age', 'fare', kind='scatter')
~~~

<img src='https://github.com/crawles/Logos/blob/master/scatter.png?raw=true'>

Multi-line SQL statements are also supported:

~~~
%%read_sql
DROP TABLE IF EXISTS table123;
CREATE TABLE table123
AS
SELECT *
FROM table456;
~~~

Finally, line magic synatax is also available:

~~~
result = %read_sql SELECT * FROM table123;
~~~

## Using sql_magic with Spark or Hive

The syntax for connecting with Spark is the same as above; simply point the connection object to a SparkSession, SQLContext, or HiveContext object:

~~~
# spark 2.0+
#uses SparkContext
%config SQL.conn_name = 'spark'

# spark 1.6 and before
from pyspark.sql import HiveContext  # or SQLContext
hive_context = HiveContext(sc)
%config SQL.conn_name = 'hive_context'
~~~

## Configuration

Both browser notifications and displaying results to standard out are enabled by default. Either of these can be temporarily disabled with the `-n` and `-d` flags, respectively. They can also be disabled using the `%config` magic function.

### Flags

Notifications and auto-display can be temporarily disabled with flags:

~~~
positional arguments:
  table_name

optional arguments:
  -h, --help     show this help message and exit
  -n, --notify   Toggle option for notifying query result
  -a, --async    Run query in seperate thread. Please be cautious when
                 assigning result to a variable
  -d, --display  Toggle option for outputing query result
~~~

### Default values

Notifications and auto-display can be disabled by default using `%config`. If this is done for either option, the flags above will temporarily enable these features.

~~~
SQL options
-------------
SQL.conn_name=<Unicode>
    Current: u'conn'
    Object name for accessing computing resource environment
SQL.notify_result=<Bool>
    Current: True
    Notify query result to stdout
SQL.output_result=<Bool>
    Current: True
    Output query result to stdout
~~~

~~~
%config SQL.output_result = False  # disable browser notifications
%config SQL.notify_result = False  # disable output to std ou
~~~

That’s it! Give sql_magic a try and let us know what you think. Please submit a pull request for any improvements or bug fixes.

### Acknowledgements

Thank you to Scott Hajek, Greg Tam, and Srivatsan Ramanujam, along with the rest of the Pivotal Data Science team for their help in developing this library. Thank you to Lia and Jackie Ho for help with the diagram. This library was inspired from and aided by the work of the [ipython-sql](https://github.com/catherinedevlin/ipython-sql) library.

