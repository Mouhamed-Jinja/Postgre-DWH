from pyspark.sql.functions import *

def sparkSession():
    # Create a Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
    .appName("ETL") \
    .config("spark.jars", "/opt/bitnami/spark/drivers/mysql-connector-java-8.0.23.jar") \
    .config("spark.jars", "/opt/bitnami/spark/drivers/postgresql-42.5.3.jar") \
    .getOrCreate()
    return spark

def read_denormalized_fraud(spark_con):
    fraud_denormalized= spark_con.read.format("jdbc") \
        .option("url", "jdbc:postgresql://172.25.0.2:5432/postgres_DWH") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "fraud_denormalized") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()
    fraud_denormalized= fraud_denormalized.withColumn("customer_id", col("cc_num"))
    return fraud_denormalized


def fact_monthly_aggregated(fraud_denormalized):
    
    fraud_denormalized = fraud_denormalized.withColumn(
        "trans_month",
        date_format("trans_date_trans_time", "yyyy-MM")
    )

  
    aggregated_fact = fraud_denormalized.groupBy(
        'customer_id',
        'merchant_id',
        'trans_month'
    ).agg(
        sum('amt').alias('total_amount'),
        count('trans_num').alias('transaction_count') 
    )

    return aggregated_fact

def write_in_Postgres_DWH(df):
    df.write.format("jdbc") \
                .option("url", "jdbc:postgresql://172.25.0.2:5432/postgres_DWH") \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "Fact_Fraud_MG") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .mode("overwrite") \
                .save()
                
if __name__=="__main__":
    spark= sparkSession()
    DFraud= read_denormalized_fraud(spark)
    Fact_MG= fact_monthly_aggregated(DFraud)
    Fact_MG.show(5)
    write_in_Postgres_DWH(Fact_MG)