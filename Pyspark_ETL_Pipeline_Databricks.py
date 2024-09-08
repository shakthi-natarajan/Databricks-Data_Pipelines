# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('PysparkonDatabricks').getOrCreate()

# COMMAND ----------

df_properties=spark.read.csv('dbfs:/FileStore/ar_properties.csv', header=True, inferSchema=True)

# COMMAND ----------

df_properties.show(5)

# COMMAND ----------

display(df_properties)

# COMMAND ----------

df_properties.dtypes

# COMMAND ----------

df_properties.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
schema_val=[
    StructField('id', StringType(), True),
    StructField('ad_type', StringType(), True),
    StructField('start_date', DateType(), True),
    StructField('end_date', DateType(), True),
    StructField('created_on', DateType(), True),
    StructField('lat', StringType(), True),
    StructField('lon', StringType(), True),
    StructField('l1', StringType(), True),
    StructField('l2', StringType(), True),
    StructField('l3', StringType(), True),
    StructField('l4', StringType(), True),
    StructField('l5', StringType(), True),
    StructField('l6', StringType(), True),
    StructField('rooms', IntegerType(), True),
    StructField('bedrooms', IntegerType(), True),
    StructField('bathrooms', IntegerType(), True),
    StructField('surface_total', IntegerType(), True),
    StructField('surface_covered', IntegerType(), True),
    StructField('price', IntegerType(), True),
    StructField('currency', StringType(), True),
    StructField('price_period', StringType(), True),
    StructField('title', StringType(), True),
    StructField('description', StringType(), True),
    StructField('property_type', StringType(), True),
    StructField('operation_type', StringType(), True),
]

enforced_schema=StructType(fields=schema_val)
df_properties_st=spark.read.csv('dbfs:/FileStore/ar_properties.csv', header= True, schema=enforced_schema)

# COMMAND ----------

df_properties_st.printSchema()

# COMMAND ----------

display(df_properties_st)

# COMMAND ----------

df_properties_st.count()

# COMMAND ----------

from pyspark.sql.functions import *
final_df=df_properties_st.withColumn('Total_#_Days', datediff('end_date','start_date'))
display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col
final_df_properties=final_df.filter(col('Total_#_Days')<=365)
display(final_df_properties)

# COMMAND ----------

final_df_properties=final_df_properties.na.drop()

# COMMAND ----------

final_df_properties.dtypes

# COMMAND ----------

final_df_properties=final_df_properties.drop('description')

# COMMAND ----------

final_df_group=final_df_properties.groupBy('bedrooms').avg('price')

# COMMAND ----------

final_df_group.head(5)

# COMMAND ----------

final_df_properties.groupBy('property_type').avg('price').show(5)
