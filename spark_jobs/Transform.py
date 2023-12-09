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
    return fraud_denormalized


def DIM_Customer(fraud_denormalized):
    dimCust= fraud_denormalized\
        .select(col("cc_num").alias("customer_id")
                ,"first"
                , "last"
                , "gender"
                , col("dob").alias("Date_Of_Birth")
                , "job")
    
    dimCust= dimCust.dropDuplicates(["customer_id"])
    return dimCust


def DIM_Merchant(fraud_denormalized):
    dimMerch= fraud_denormalized\
        .select('merchant_id'
                ,col("merchant").alias("merchant_name")
                ,'category'
                ,'merch_lat'
                , 'merch_long')
    
    
    dimMerch= dimMerch.dropDuplicates(["merchant_id"])
    return dimMerch

    
def DIM_Address(fraud_denormalized):
    dimAdd= fraud_denormalized\
        .select("address_id"
                ,'street'
                ,'zip'
                ,'lat'
                ,'long'
                ,col('city').alias("city_name")
                ,'state'
                ,col('city_pop').alias("city_population"))
    dimAdd= dimAdd.dropDuplicates(["address_id"])  
    return dimAdd  

def DIM_Date(fraud_denormalized):
    order_dates = fraud_denormalized.select("trans_date_trans_time").distinct()
    
    dimdate = order_dates.select(
    col("trans_date_trans_time").alias("DateKey"),
    date_format("trans_date_trans_time", "yyyy-MM-dd").alias("Date"),
    date_format("trans_date_trans_time", "yyyy").alias("Year"),
    date_format("trans_date_trans_time", "MM").alias("Month"),
    date_format("trans_date_trans_time", "dd").alias("Day"),
    quarter("trans_date_trans_time").alias("Quarter"))
    return dimdate
    



def Fact_Transactional(fraud_denormalized):
    fact= fraud_denormalized.select('trans_num',
                                    col('cc_num').alias('customer_id')
                                    ,'merchant_id'
                                    ,'unix_time'
                                    ,to_timestamp("trans_date_trans_time", "yyyy-MM-dd HH:mm:ss").alias("trans_timestamp")
                                    ,'amt'
                                    ,'is_fraud')
    return fact

def write_into_postgres_dwh(dim_customer, dim_merchant, dim_address, dm_date, fact_fraud):
    tables = [dim_customer, dim_merchant, dim_address, dm_date, fact_fraud]
    table_names = ['DIM_Customer', 'DIM_Merchant', 'DIM_address', 'DM_Date', 'Fact_Fraud']

    for table, table_name in zip(tables, table_names):
        table.write.format("jdbc") \
            .option("url", "jdbc:postgresql://172.25.0.2:5432/postgres_DWH") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{table_name}") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .mode("overwrite") \
            .save()

        
if __name__ == "__main__":
    spark= sparkSession()
    df= read_denormalized_fraud(spark)
    fact = Fact_Transactional(df)
    cust= DIM_Customer(df)
    add = DIM_Address(df)
    merch= DIM_Merchant(df)
    dimdate= DIM_Date(df)
    write_into_postgres_dwh(cust, merch, add, dimdate, fact)