import pyspark
import matplotlib
import pandas

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd

# Create a new SparkSession to load the CSV file into a DataFrame
spark = SparkSession.builder.appName('Sales Data Analysis').getOrCreate()

sales_data = spark.read.csv('sales_data.csv', header=True, inferSchema=True)

# explore the data
sales_data.show(5)

# Save the DataFrame as a temporary table to run SQL queries on it:
sales_data.createOrReplaceTempView('sales_data')

# for duplicates and missing values
sales_data = sales_data.dropDuplicates()
sales_data = sales_data.na.drop()

# add new columns in the csv file
sales_data = sales_data.withColumn('total_sales', col('quantity') * col('unit_price'))
sales_data = sales_data.withColumn('order_date', to_date('order_date', 'yyyy-MM-dd'))

sales_metrics = sales_data.groupBy('order_date').agg(sum('total_sales').alias('total_sales'))


# visualize the sales data
sales_pandas_df = sales_metrics.toPandas()

# create a line plot of total sales by date
sales_pandas_df.plot(x='order_date', y='total_sales', kind='line')
plt.title('Total Sales by Date')
plt.xlabel('Date')
plt.ylabel('Total Sales')

# show the plot
plt.show()


