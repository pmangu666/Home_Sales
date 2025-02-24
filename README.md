# Spark and Home Sales Analysis Setup

## Prerequisites

- Install Python 3.x
- Install pip (Python package installer)

## Setup

1. **Find the latest version of Spark 3.x**
   - Visit [Apache Spark](http://www.apache.org/dist/spark/) and find the latest version of Spark 3.x
   - Set the Spark version:

    ```python
    import os
    spark_version = 'spark-3.5.4'
    os.environ['SPARK_VERSION'] = spark_version
    ```

2. **Install Spark and Java**

    ```bash
    !apt-get update
    !apt-get install openjdk-11-jdk-headless -qq > /dev/null
    !wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz
    !tar xf $SPARK_VERSION-bin-hadoop3.tgz
    !pip install -q findspark
    ```

3. **Set Environment Variables**

    ```python
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop3"
    ```

4. **Start a SparkSession**

    ```python
    import findspark
    findspark.init()

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import year, col, round, avg
    from pyspark.sql.types import StringType
    import time

    spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
    ```

## Load and Analyze the Data

1. **Read the `home_sales_revised.csv` from AWS S3 into a PySpark DataFrame**

    ```python
    from pyspark import SparkFiles

    url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
    dbutils.fs.cp(url, "dbfs:/FileStore/tables/home_sales_revised.csv")

    file_location = "dbfs:/FileStore/tables/home_sales_revised.csv"
    home_sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_location)
    ```

2. **Create a temporary view of the DataFrame**

    ```python
    home_sales_df.createOrReplaceTempView("home_sales")
    ```

3. **Analyze the data**

    - Average price for a four-bedroom house sold per year

      ```python
      four_bedroom_avg_price_df = home_sales_df.filter(home_sales_df.bedrooms == 4) \
          .groupBy(year(home_sales_df.date).alias("year")) \
          .agg(round(avg("price"), 2).alias("avg_price")) \
          .orderBy("year")

      four_bedroom_avg_price_df.show()
      ```

    - Average price of a home for each year the home was built, with 3 bedrooms and 3 bathrooms

      ```python
      home_sales_df = home_sales_df.withColumn("date_built_str", col("date_built").cast(StringType()))

      avg_price_df = home_sales_df.filter((home_sales_df.bedrooms == 3) & (home_sales_df.bathrooms == 3)) \
          .groupBy(home_sales_df.date_built_str.alias("year_built")) \
          .agg(round(avg("price"), 2).alias("avg_price")) \
          .orderBy("year_built")

      avg_price_df.show()
      ```

    - Average price of a home for each year the home was built, with 3 bedrooms, 3 bathrooms, two floors, and greater than or equal to 2,000 square feet

      ```python
      filtered_df = home_sales_df.filter(
          (home_sales_df.bedrooms == 3) &
          (home_sales_df.bathrooms == 3) &
          (home_sales_df.floors == 2) &
          (home_sales_df.sqft_living >= 2000)
      )

      avg_price_df = filtered_df.groupBy(col("date_built").alias("year_built")) \
          .agg(round(avg("price"), 2).alias("avg_price")) \
          .orderBy("year_built")

      avg_price_df.show()
      ```

    - Average price of a home per "view" rating, with an average home price greater than or equal to $350,000

      ```python
      start_time = time.time()

      view_avg_price_df = home_sales_df.groupBy("view") \
          .agg(round(avg("price"), 2).alias("avg_price")) \
          .filter("avg_price >= 350000") \
          .orderBy(col("view").desc())

      view_avg_price_df.show()

      print("--- %s seconds ---" % (time.time() - start_time))
      ```

4. **Cache the temporary table `home_sales`**

    ```python
    spark.sql("CACHE TABLE home_sales")
    spark.catalog.isCached('home_sales')
    ```

5. **Run the previous query using cached data and compare runtime**

    ```python
    start_time = time.time()

    view_avg_price_df = spark.sql("""
    SELECT view, ROUND(AVG(price), 2) AS avg_price
    FROM home_sales
    GROUP BY view
    HAVING AVG(price) >= 350000
    ORDER BY view DESC
    """)

    view_avg_price_df.show()

    print("--- %s seconds ---" % (time.time() - start_time))
    ```

6. **Partition by the `date_built` field and save as parquet**

    ```python
    home_sales_df.write.partitionBy("date_built").parquet("/path/to/partitioned_home_sales")
    ```

7. **Read the parquet formatted data**

    ```python
    partitioned_df = spark.read.parquet("/path/to/partitioned_home_sales")
    partitioned_df.show()
    ```

8. **Create a temporary table for the parquet data**

    ```python
    partitioned_df.createOrReplaceTempView("partitioned_home_sales")
    ```

9. **Run the last query using parquet DataFrame and compare runtime**

    ```python
    start_time = time.time()

    view_avg_price_parquet_df = spark.sql("""
    SELECT view, ROUND(AVG(price), 2) AS avg_price
    FROM partitioned_home_sales
    GROUP BY view
    HAVING AVG(price) >= 350000
    ORDER BY view DESC
    """)

    view_avg_price_parquet_df.show()

    print("--- %s seconds ---" % (time.time() - start_time))
    ```

10. **Uncache the `home_sales` table**

    ```python
    spark.sql("UNCACHE TABLE home_sales")
    is_cached = spark.catalog.isCached('home_sales')
    print(f"Is 'home_sales' table cached? {is_cached}")
    ```

## Conclusion

This `README` provides step-by-step instructions to set up and run a PySpark analysis on the home sales dataset. Follow these steps to reproduce the analysis and compare runtimes with different configurations.
# Home_Sales
