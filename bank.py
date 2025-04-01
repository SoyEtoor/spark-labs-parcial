from pyspark.sql import SparkSession
import json
import os
import pandas as pd
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("transaction_data") \
        .getOrCreate()

    print("Reading bank.xlsx ...")
    # Leer el archivo Excel con pandas
    pdf = pd.read_excel("bank.xlsx")
    
    # Convertir a DataFrame de Spark
    df_trans = spark.createDataFrame(pdf)

    # Renombrar columnas para consistencia
    df_trans = df_trans.withColumnRenamed("Date", "date") \
                      .withColumnRenamed("Domain", "domain") \
                      .withColumnRenamed("Location", "location") \
                      .withColumnRenamed("Value", "value") \
                      .withColumnRenamed("Transaction_count", "transaction_count")

    # Mostrar esquema para verificación
    df_trans.printSchema()

    df_trans.createOrReplaceTempView("transactions")

    def save_to_jsonl(df, folder_name):
        path = os.path.join("results", folder_name, "data.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            for row in df.toJSON().collect():
                file.write(row + "\n")

    # Consulta 1: Transacciones por dominio
    query = """SELECT domain, COUNT(*) as total_transactions, 
               SUM(value) as total_value 
               FROM transactions 
               GROUP BY domain 
               ORDER BY total_value DESC"""
    df_domain_stats = spark.sql(query)
    df_domain_stats.show(20)
    save_to_jsonl(df_domain_stats, "domain_statistics")

    # Consulta 2: Actividad por ubicación
    query = """SELECT location, COUNT(*) as transaction_count,
               AVG(value) as avg_value
               FROM transactions
               GROUP BY location
               ORDER BY transaction_count DESC"""
    df_location_stats = spark.sql(query)
    df_location_stats.show(20)
    save_to_jsonl(df_location_stats, "location_statistics")

    # Consulta 3: Transacciones de alto valor
    query = """SELECT date, domain, location, value
               FROM transactions 
               WHERE value > (SELECT AVG(value) * 3 FROM transactions)
               ORDER BY value DESC"""
    df_high_value = spark.sql(query)
    df_high_value.show(20)
    save_to_jsonl(df_high_value, "high_value_transactions")

    # Consulta 4: Tendencia temporal
    query = """SELECT YEAR(date) as year, MONTH(date) as month,
               COUNT(*) as transaction_count,
               SUM(value) as monthly_value
               FROM transactions
               GROUP BY YEAR(date), MONTH(date)
               ORDER BY year, month"""
    df_time_series = spark.sql(query)
    df_time_series.show(20)
    save_to_jsonl(df_time_series, "time_series_analysis")

    spark.stop()
