# spark-example

Example `spark` code.

Spark's main abstraction is the `DataSet` (previously `RDD`). `DataSet`s support
two types of operations: `transformations` and `actions`.

Transformations (i.e., map, filter) are done in parallel on the cluster.
Transformations are lazy and not evaluated until an `action` is performed (i.e.,
count, collect). When an action is performed, the transformations occur on the
cluster and the results are delivered to the driver.

Functions that run in the cluster should be functional (do not modify external
state or close over variables that can mutate). Closures (functions + required
state) are copied to each cluster node. Thus, each node operates on a different
copy of closed over variables.

Accumulators are variables that can be safely shared among cluster nodes.


## Spark SQL / DataFrame / Dataset API

Spark SQL and the Dataset API are meant to do structured data processing (think
Pandas). Both the SQL and Dataset APIs use the same underlying execution engine
to process data.

IMPORTANT: Python does not support the Dataset API.



```shell
# Install pyspark as a pip package
poetry add pyspark

poetry run pyspark
```


## Databricks

"Lakehouse": A combination of ACID transactions and metadata of databases with
data stored in parquet files.

* Metadata (table names, schemas, etc) is stored in a "metastore".
* Table metadata and data are saved in cloud storage.

