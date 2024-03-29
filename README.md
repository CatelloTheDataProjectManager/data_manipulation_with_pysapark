# Data Manipulation with PySpark in Docker🐋

This repository contains a Jupyter notebook that showcases the use of PySpark with Docker for scalable data processing. The notebook covers several key concepts in PySpark, including broadcast variables, partitions, the withColumn function, and pivot & unpivot operations.

## ETL Data Transformation with AWS Glue and S3

***In real-world applications, I have effectively deployed ETL (Extract, Transform, Load) Data Transformation pipelines utilizing AWS Glue.***

This involved:

- Extracting data from various sources.
- Transforming the data using PySpark to clean, filter, aggregate, or enrich it as needed.
- Loading the transformed data into data warehouses or data lakes stored on S3.
- Additionally, I automated the execution of large-scale data processing tasks using AWS Glue, ensuring timely and efficient processing of data at scale.

    <img src="https://github.com/CatelloTheDataProjectManager/data_manipulation_with_pysapark/blob/main/AWS%20workflow.png" width="600">

***Using PySpark in ETL (Extract, Transform, Load) processes with AWS Glue and S3 offers significant advantages for building robust data transformation pipelines***

### Broadcast Variables
 In this notebook, we create a broadcast variable `broadcast_status` to store a mapping of marital status codes to their corresponding full names.

### Partitions
The notebook covers the concepts of data shuffle and data skew, and demonstrates the use of the `partitionBy`, `coalesce`, and `repartition` functions to manage partitions.

### `withColumn` Function
The notebook demonstrates several uses of this function, including updating a column based on another column, changing the datatype of a column, conditional updates, and renaming a column.

### Pivot & Unpivot Operations
Pivot operations transform row values into column names, and unpivot operations do the reverse. The notebook demonstrates these operations using a DataFrame of Olympic medal counts.

### PySpark's MapReduce

In the last section shows how to analyze sales data using PySpark's MapReduce capabilities. This example demonstrates how PySpark can be used for data analysis tasks with large datasets.

For the full code and output, please refer to the Jupyter notebook: [Data Manipulation with PySpark](https://github.com/CatelloTheDataProjectManager/data_manipulation_with_pysapark/blob/main/data_manipulation_with_pysapark.ipynb).

Happy learning and coding! 🚀

###### (If you want to learn more about Map-Reduce en Python and its functionalities, check out the my project on : [Map-Reduce en Python](https://github.com/CatelloTheDataProjectManager/Map-Reduce-en-Python/blob/main/README.md).)





