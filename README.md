# Data Manipulation with PySpark in Docker

This repository contains a Jupyter notebook that showcases the use of PySpark with Docker for scalable data processing. The notebook covers several key concepts in PySpark, including broadcast variables, partitions, the withColumn function, and pivot & unpivot operations.

### Broadcast Variables
A broadcast variable in PySpark is a read-only variable that is cached on each node in a cluster. They are used to efficiently distribute large, read-only datasets to all nodes for use in parallel processing tasks. In this notebook, we create a broadcast variable broadcast_status to store a mapping of marital status codes to their corresponding full names. We then use a UDF (User Defined Function) to convert the status codes in our DataFrame to their full names using the broadcast variable.

### Partitions
Partitions in PySpark are logical divisions of data used to distribute data across nodes for parallel processing. The notebook covers the concepts of data shuffle and data skew, and demonstrates the use of the partitionBy, coalesce, and repartition functions to manage partitions.

### withColumn Function
The withColumn function in PySpark is used to add a new column to an existing DataFrame or replace an existing column. The notebook demonstrates several uses of this function, including updating a column based on another column, changing the datatype of a column, conditional updates, and renaming a column.

### Pivot & Unpivot Operations
The notebook also covers pivot and unpivot operations in PySpark. Pivot operations transform row values into column names, and unpivot operations do the reverse. The notebook demonstrates these operations using a DataFrame of Olympic medal counts.

For the full code and output, please refer to the Jupyter notebook.

Happy learning! 🚀
