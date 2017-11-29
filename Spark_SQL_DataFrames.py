# Databricks notebook source
# Load a dataframe from a CSV file (with header line).  Change the filename to one that matches your S3 bucket.
DF = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferSchema='true').load('/mnt/S3/data/dataSet3Events.csv')

# COMMAND ----------

# Examine the data, and field/column names
eventDF = DF.dropDuplicates(['vin'])
display(eventDF)

# COMMAND ----------

# only register table can apply sql language
eventDF.registerTempTable('table1')


# COMMAND ----------

query1 = sqlContext.sql('SELECT make, model, MIN(price) AS MinPrice, MAX(price) AS MaxPrice, AVG(Price) AS AvgPrice FROM table1 WHERE price != 0 GROUP BY make, model ORDER BY make,model')

# COMMAND ----------

display(query1)

# COMMAND ----------

query2 = sqlContext.sql('SELECT year, MIN(mileage) AS MinMileage, MAX(mileage) AS MaxMileage, AVG(Mileage) AS AvgMileage FROM table1 WHERE mileage != 0 GROUP BY year ORDER BY year DESC')

# COMMAND ----------

display(query2)

# COMMAND ----------

DF.registerTempTable("table2")

query3 = sqlContext.sql("SELECT vin, event, count(event) as sum_event_type FROM table2 GROUP BY vin, event ORDER BY vin,event")
display(query3)

# COMMAND ----------

query1.repartition(1).write.format('com.databricks.spark.csv').options(header='true').save('/mnt/S3/output/dfOut1')

# COMMAND ----------

query2.repartition(1).write.format('com.databricks.spark.csv').options(header='true').save('/mnt/S3/output/dfOut2')

# COMMAND ----------

query3.repartition(1).write.format('com.databricks.spark.csv').options(header='true').save('/mnt/S3/output/dfOut3')

# COMMAND ----------


