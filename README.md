# Mini Project 1: PySpark Data Processing for Customers and Orders

## Overview
This project demonstrates data reading, cleaning, transformation, analysis, and joining using PySpark. It processes two datasets:
- **customers.csv**: Contains customer information such as ID, name, location, registration date, and activity status.
- **orders.csv**: Contains order details such as ID, customer ID, order date, total amount, and status.

The notebook performs:
- Data ingestion from CSV files.
- Schema inference, type casting (e.g., dates, booleans), and handling missing values.
- Derived columns (e.g., registration year/month).
- Aggregations (e.g., unique counts, group by city/state, pivot tables for active/inactive users).
- Window functions (e.g., rank, dense_rank, row_number) for ranking customers by registration date per state.
- Filtering (e.g., recent customers registered after July 2023).
- Joins between customers and orders datasets.
- Advanced analysis (e.g., orders by month, top spenders, high-frequency low-spend customers).
- Output writing to Parquet format for efficient storage.

This serves as a mini ETL (Extract, Transform, Load) pipeline using PySpark, suitable for learning Spark SQL, DataFrames, and window functions.

## Prerequisites
- **Python 3.x**
- **Apache Spark** (tested with Spark 3.3.2)
- **PySpark** library
- A Spark environment (e.g., local Spark setup, Databricks, or EMR). The notebook is originally from Databricks, using DBFS for file paths—adapt paths for local use.
- Datasets: `customers.csv` and `orders.csv` (place them in a accessible directory or cloud storage).

### Installation
1. Install PySpark via pip:
   ```
   pip install pyspark
   ```
2. If using Databricks, import the notebook directly.
3. For local setup:
   - Download Spark from [Apache Spark](https://spark.apache.org/downloads.html).
   - Set environment variables: `SPARK_HOME` and add to `PATH`.

## Usage
1. **Clone the Repository**:
   ```
   git clone https://github.com/your-username/mini-project-1-pyspark.git
   cd mini-project-1-pyspark
   ```

2. **Prepare Data**:
   - Place `customers.csv` and `orders.csv` in a directory (e.g., `./data/`).
   - Update file paths in the notebook (replace `dbfs:/FileStore/shared_uploads/...` with local paths like `file:///path/to/data/customers.csv`).

3. **Run the Notebook**:
   - Use Jupyter Notebook:
     ```
     jupyter notebook "Mini Project 1 - Data Read & Process.ipynb"
     ```
   - Or convert to a Python script and run with Spark:
     ```
     spark-submit mini_project.py
     ```
   - In Databricks: Upload the notebook and run it in a cluster.

4. **Output**:
   - Processed data is written to Parquet files: `/FileStore/tables/processed_customers` and `/FileStore/tables/final_customer_orders`.
   - Adapt paths as needed (e.g., to local `./output/`).

### Key Code Snippets
- **Reading Data**:
  ```python
  df = spark.read.format("csv").option("header", "true").load("path/to/customers.csv")
  ```
- **Type Casting and Cleaning**:
  ```python
  df = df.withColumn('registration_date', to_date(col("registration_date"), 'yyyy-MM-dd')) \
         .withColumn('is_active', col("is_active").cast('boolean')) \
         .fillna({'city': 'Unknown', 'state': 'Unknown', 'country': 'Unknown'})
  ```
- **Aggregations**:
  ```python
  df.groupBy('city').count().orderBy(col('count').desc()).show()
  ```
- **Window Functions**:
  ```python
  window_spec = Window.partitionBy('state').orderBy(col('registration_date').desc())
  df = df.withColumn('rank', rank().over(window_spec))
  ```
- **Joining Datasets**:
  ```python
  customers_orders_df = df.join(orders_df, 'customer_id', "inner")
  ```

## Dataset Details
- **customers.csv** (sample columns): `customer_id`, `name`, `city`, `state`, `country`, `registration_date`, `is_active`.
- **orders.csv** (sample columns): `order_id`, `customer_id`, `order_date`, `total_amount`, `status`.
- Data is synthetic, focused on Indian locations.

## Results and Insights
- Unique states: 7
- Top cities by customer count: Pune, Hyderabad, etc.
- Pivot: Active/inactive customers per state.
- Recent customers (post-2023-07-01): 9025
- Orders by month: Peaks in March (1539 orders).
- Top spenders: Ranked by total amount spent.

## Contributing
Feel free to fork and submit pull requests. Issues and suggestions are welcome!

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Built with PySpark on Databricks.
- Inspired by common data engineering tasks for customer analytics.
