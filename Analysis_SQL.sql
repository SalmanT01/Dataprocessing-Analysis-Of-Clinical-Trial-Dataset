-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.types import * 
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC ClinicalTrial_RDD = sc.textFile("/FileStore/tables/clinicaltrial_2023.csv")
-- MAGIC
-- MAGIC # Clean Data from unwanted characters
-- MAGIC ClinicalTrial_RDD1 = ClinicalTrial_RDD.map(lambda line: line.replace('"', "").strip(","))
-- MAGIC ClinicalTrial_RDD3 = ClinicalTrial_RDD1.map(lambda line: line.split('\t'))
-- MAGIC
-- MAGIC List = ClinicalTrial_RDD3.collect()                     
-- MAGIC
-- MAGIC list_Id = []
-- MAGIC list_Type = []
-- MAGIC list_Conditions = []
-- MAGIC list_Sponsors = []
-- MAGIC list_CompletedDates = []
-- MAGIC
-- MAGIC # create separate lists for each column
-- MAGIC for item in List:
-- MAGIC     for index, value in enumerate(item): 
-- MAGIC         if index == 0:
-- MAGIC             list_Id.append(value)
-- MAGIC         elif index == 10:
-- MAGIC             list_Type.append(value)
-- MAGIC         elif index == 4:
-- MAGIC             list_Conditions.append(value)
-- MAGIC         elif index == 6:
-- MAGIC             list_Sponsors.append(value)
-- MAGIC         elif index == 13:
-- MAGIC             list_CompletedDates.append(value)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #### Create Dataframe for Id Column
-- MAGIC Id_Schema = StructType([
-- MAGIC     StructField("Index", IntegerType()),
-- MAGIC     StructField("Id", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC Id_rdd = sc.parallelize(list_Id)
-- MAGIC Id_rdd1 = Id_rdd.zipWithIndex()
-- MAGIC Id_rdd2 = Id_rdd1.map(lambda values:[values[1], values[0]])
-- MAGIC
-- MAGIC Id_DF = spark.createDataFrame(Id_rdd2, Id_Schema)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #### Create Dataframe for Types Column
-- MAGIC Type_Schema = StructType([
-- MAGIC     StructField("Index", IntegerType()),
-- MAGIC     StructField("Type", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC Type_rdd = sc.parallelize(list_Type)
-- MAGIC Type_rdd1 = Type_rdd.zipWithIndex()
-- MAGIC Type_rdd2 = Type_rdd1.map(lambda values:[values[1], values[0]])
-- MAGIC
-- MAGIC Type_DF = spark.createDataFrame(Type_rdd2, Type_Schema)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC
-- MAGIC #### Create Dataframe for Conditions Column
-- MAGIC Conditions_Schema = StructType([
-- MAGIC     StructField("Index", IntegerType()),
-- MAGIC     StructField("Conditions", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC Conditions_rdd1 = sc.parallelize(list_Conditions)
-- MAGIC
-- MAGIC # Optional: indexing based on relation with studies
-- MAGIC Conditions_rdd2 = Conditions_rdd1.zipWithIndex()
-- MAGIC Conditions_rdd3 = Conditions_rdd2.map(lambda values: [values[1], values[0]])
-- MAGIC
-- MAGIC Conditions_DF = spark.createDataFrame(Conditions_rdd3, Conditions_Schema)
-- MAGIC Conditions_DF1 = Conditions_DF.withColumn("Conditions", explode(split(col("Conditions"), '\|')))
-- MAGIC #Conditions_DF1.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #### Create Dataframe for Sponsors Column
-- MAGIC Sponsors_Schema = StructType([
-- MAGIC     StructField("Index", IntegerType()),
-- MAGIC     StructField("Sponsors", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC Sponsors_rdd = sc.parallelize(list_Sponsors)
-- MAGIC Sponsors_rdd1 = Sponsors_rdd.zipWithIndex()
-- MAGIC Sponsors_rdd2 = Sponsors_rdd1.map(lambda values:[values[1], values[0]])
-- MAGIC
-- MAGIC Sponsors_DF = spark.createDataFrame(Sponsors_rdd2, Sponsors_Schema)
-- MAGIC
-- MAGIC Sponsors_DF.createOrReplaceTempView("sqlsponsorDF")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #### Create Dataframe for Completed Dates Column
-- MAGIC
-- MAGIC CompletedDates_Schema = StructType([
-- MAGIC     StructField("Index", IntegerType()),
-- MAGIC     StructField("CompletedDates", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC CompletedDates_rdd = sc.parallelize(list_CompletedDates)
-- MAGIC CompletedDates_rdd1 = CompletedDates_rdd.zipWithIndex()
-- MAGIC CompletedDates_rdd2 = CompletedDates_rdd1.map(lambda values:[values[1], values[0]])
-- MAGIC
-- MAGIC CompletedDates_DF = spark.createDataFrame(CompletedDates_rdd2, CompletedDates_Schema)
-- MAGIC
-- MAGIC CompletedDates_DF.createOrReplaceTempView("sqlcompletedDatesDF")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #### Join All Above created DFs
-- MAGIC ClinicalTrial_DF = Id_DF.join(Type_DF, on='Index', how='inner').join(Conditions_DF1, on='Index', how='inner').join(Sponsors_DF, on='Index', how='inner').join(CompletedDates_DF, on='Index', how='inner')
-- MAGIC
-- MAGIC # Ascending Order of Index
-- MAGIC ClinicalTrial_DF1 = ClinicalTrial_DF.orderBy("Index") 
-- MAGIC
-- MAGIC # Remove 1st Row
-- MAGIC ClinicalTrial_DF2 = ClinicalTrial_DF1.filter(ClinicalTrial_DF1[0]!="0") 
-- MAGIC
-- MAGIC ClinicalTrial_DF2.show(5, truncate=True)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Update Dataframe Schema for Columns 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC Update_ClinicalTrial_DF2 = ClinicalTrial_DF2\
-- MAGIC   .select(
-- MAGIC     col('Index').cast('int'),
-- MAGIC     col('Id').cast('string'),
-- MAGIC     col('Type').cast('string'),
-- MAGIC     col('Conditions').cast('string'),
-- MAGIC     col('Sponsors').cast('string'),
-- MAGIC     col('CompletedDates').cast('date'),
-- MAGIC   )
-- MAGIC    
-- MAGIC Update_ClinicalTrial_DF2.show(5, truncate=True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Convert Dataframe into SQL View 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Update_ClinicalTrial_DF2.createOrReplaceTempView("sqlclinicaltrial1")

-- COMMAND ----------

SELECT * FROM sqlclinicaltrial1 limit 200

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Convert SQL View into Parmanent SQL Table

-- COMMAND ----------

--CREATE TABLE 
CREATE OR REPLACE TABLE default.clinicalTrialTable AS SELECT * FROM sqlclinicaltrial1

-- COMMAND ----------

SHOW TABLES


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DISPLAY "ClinicalTrialTable" TABLE with useful columns

-- COMMAND ----------

SELECT * FROM clinicalTrialTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### For Clinical Trials 2023
-- MAGIC #### Find Number of Distinct Studies in Dataset

-- COMMAND ----------

SELECT COUNT(DISTINCT(Id)) FROM clinicalTrialTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### For Clinical Trials 2023
-- MAGIC #### List all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. In order from most frequent to least frequent. 

-- COMMAND ----------

SELECT Type, COUNT(*) 
FROM clinicalTrialTable
GROUP BY Type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### For Clinical Trial 2023
-- MAGIC #### The top 5 conditions (from Conditions) with their frequencies. 

-- COMMAND ----------

SELECT Conditions, COUNT(*) as count  
FROM clinicalTrialTable
GROUP BY Conditions 
ORDER BY count DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### For Clinical Trial 2023
-- MAGIC #### Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Get Parent Company Column from Pharma.csv Dataset
-- MAGIC RDD_Pharma  = sc.textFile("/FileStore/tables/pharma.csv")
-- MAGIC
-- MAGIC # Clean dataset from unwanted characters
-- MAGIC RDD_Pharma1 = RDD_Pharma.map(lambda line: line.replace('","','"\t"'))
-- MAGIC RDD_Pharma2 = RDD_Pharma1.map(lambda line: line.replace('"', ""))
-- MAGIC RDD_Pharma3 = RDD_Pharma2.map(lambda line: line.split('\t'))
-- MAGIC
-- MAGIC RDD_Pharma4 = RDD_Pharma3.map(lambda values: (values[1]))
-- MAGIC
-- MAGIC #LIST of Pharma Names from Pharma Dataset
-- MAGIC List_Pharma= RDD_Pharma4.distinct().collect()
-- MAGIC
-- MAGIC # Index pharma names
-- MAGIC RDD_Pharma5 = RDD_Pharma4.zipWithIndex()
-- MAGIC RDD_Pharma6 = RDD_Pharma5.map(lambda values:[values[1], values[0]])
-- MAGIC
-- MAGIC NewSchema = StructType ([
-- MAGIC     StructField("Index", IntegerType()),
-- MAGIC     StructField("Parent_Company", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC # Data Frame with Pharma Names
-- MAGIC Pharma_DF = spark.createDataFrame(RDD_Pharma6, NewSchema)
-- MAGIC Pharma_DF.createOrReplaceTempView("sql_pharma")
-- MAGIC

-- COMMAND ----------

SELECT * FROM sql_pharma LIMIT 20

-- COMMAND ----------

SELECT cl.Sponsors, COUNT(*) as count FROM sqlsponsorDF as cl 
WHERE NOT EXISTS (SELECT Parent_Company FROM sql_pharma as ph WHERE cl.Sponsors = ph.Parent_Company) 
GROUP BY cl.Sponsors 
ORDER BY count DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### For Clinical Trial 2023
-- MAGIC #### Plot number of completed studies for each month in 2023.

-- COMMAND ----------

SELECT CONCAT(YEAR(cl.CompletedDates),"-", MONTH(cl.CompletedDates)) AS Formatted_Dates, COUNT(*) as count  
FROM sqlcompletedDatesDF as cl
WHERE YEAR(cl.CompletedDates) LIKE ("2023")
GROUP BY Formatted_Dates
ORDER BY count ASC

