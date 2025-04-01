from pyspark.sql import SparkSession
import json
import os
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("bank_data") \
        .getOrCreate()

    print("Reading bank.xlsx ...")
    # Leer el archivo Excel con pandas primero
    pdf = pd.read_excel("bank.xlsx")
    
    # Convertir a DataFrame de Spark
    df_bank = spark.createDataFrame(pdf)

    # Renombrar columnas para consistencia
    df_bank = df_bank.withColumnRenamed("CustomerId", "customer_id") \
                     .withColumnRenamed("Surname", "surname") \
                     .withColumnRenamed("CreditScore", "credit_score") \
                     .withColumnRenamed("Geography", "geography") \
                     .withColumnRenamed("Gender", "gender") \
                     .withColumnRenamed("Age", "age") \
                     .withColumnRenamed("Tenure", "tenure") \
                     .withColumnRenamed("Balance", "balance") \
                     .withColumnRenamed("NumOfProducts", "num_of_products") \
                     .withColumnRenamed("HasCrCard", "has_cr_card") \
                     .withColumnRenamed("IsActiveMember", "is_active_member") \
                     .withColumnRenamed("EstimatedSalary", "estimated_salary") \
                     .withColumnRenamed("Exited", "exited")

    df_bank.createOrReplaceTempView("bank")

    def save_to_jsonl(df, folder_name):
        path = os.path.join("results", folder_name, "data.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            for row in df.toJSON().collect():
                file.write(row + "\n")

    # Consultas (mantener las mismas de la versión anterior)
    query = """SELECT customer_id, credit_score, age, geography, gender 
               FROM bank WHERE credit_score > 750 
               ORDER BY credit_score DESC"""
    df_high_credit = spark.sql(query)
    df_high_credit.show(20)
    save_to_jsonl(df_high_credit, "high_credit_customers")

    query = """SELECT customer_id, age, geography, gender, credit_score 
               FROM bank WHERE exited = 1 
               ORDER BY age"""
    df_exited_customers = spark.sql(query)
    df_exited_customers.show(20)
    save_to_jsonl(df_exited_customers, "exited_customers")

    query = """SELECT geography, COUNT(*) as customer_count 
               FROM bank 
               GROUP BY geography ORDER BY customer_count DESC"""
    df_customers_by_country = spark.sql(query)
    df_customers_by_country.show()
    save_to_jsonl(df_customers_by_country, "customers_by_country")

    query = """SELECT customer_id, age, geography, gender, balance 
               FROM bank WHERE age BETWEEN 18 AND 25 AND balance > 100000 
               ORDER BY balance DESC"""
    df_young_high_balance = spark.sql(query)
    df_young_high_balance.show(20)
    save_to_jsonl(df_young_high_balance, "young_high_balance")

    spark.stop()