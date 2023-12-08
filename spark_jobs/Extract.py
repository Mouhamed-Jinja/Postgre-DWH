from pyspark.sql.functions import *

def sparkSession():
    # Create a Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
    .appName("ETL") \
    .config("spark.jars", "/opt/bitnami/spark/jobs/mysql-connector-java-8.0.23.jar") \
    .getOrCreate()
    return spark

def read_from_csv(spark_conn):
    fraud_csv= spark_conn.read.csv("/opt/bitnami/spark/data/fraud_CSV/fraud.csv", header=True, inferSchema=True)
    return fraud_csv

def read_from_json(spark_conn):
    fraud_json= spark_conn.read.json("/opt/bitnami/spark/data/json_Fromat/fraud_json_format.json")
    return fraud_json

def read_from_parquet(spark_conn):
    fraud_parquet= spark.read.parquet("/opt/bitnami/spark/data/parquet/fraud_parquet_format.parquet")
    return fraud_parquet

def read_from_mysql_and_denormalize(spark_conn):
    ########## Start Reading ############
    
    #Customar transactional table
    customer_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://172.23.0.2:3306/fraud") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "customer") \
    .option("user", "root") \
    .option("password", "rootpass") \
    .load()

    #Merchant transactional table
    merchant_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://172.23.0.2:3306/fraud") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "merchant") \
    .option("user", "root") \
    .option("password", "rootpass") \
    .load()

    #Transaction transactional table
    transaction_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://172.23.0.2:3306/fraud") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "transaction") \
    .option("user", "root") \
    .option("password", "rootpass") \
    .load()

    #Address transactional table
    address_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://172.23.0.2:3306/fraud") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "address") \
    .option("user", "root") \
    .option("password", "rootpass") \
    .load()

    #City transactional table
    city_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://172.23.0.2:3306/fraud") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "city") \
    .option("user", "root") \
    .option("password", "rootpass") \
    .load()
    
    ############### Start Denormaizing #################
    add= address_df.join(city_df, on='city_id', how='inner')
    cust= customer_df.join(add, on="address_id", how="inner")
    trans= transaction_df.join(cust, on="cc_num", how="inner")
    fraud = trans.join(merchant_df, on="merchant_id", how="inner")
    fraud= fraud.withColumn("long", col("longitude"))
    return fraud
    

def transform(csv_df, json_df, parquet_df, mysql_df):
    arranged_cols = [
         'trans_num', 'cc_num', 'merchant_id', 'address_id', 'city_id', 'city', 'street', 'zip',
        'lat', 'long', 'merchant', 'trans_date_trans_time', 'category', 'amt', 'first', 'last', 'gender', 
        'state', 'city_pop', 'job', 'dob', 'unix_time', 'merch_lat', 'merch_long', 'is_fraud'
    ]
     
    #rearrange columns names for Merging
    csv_df = csv_df.select(arranged_cols)
    json_df= json_df.select(arranged_cols)
    parquet_df= parquet_df.select(arranged_cols)
    mysql_df= mysql_df.select(arranged_cols)

    #union all; there are not dublicated transaction so i used unionAll
    df1= csv_df.unionAll(json_df)
    df2= parquet_df.unionAll(mysql_df)
    finalDF= df1.unionAll(df2)

    return finalDF
    
if __name__ == "__main__":
    spark= sparkSession()
    
    csv_data= read_from_csv(spark)
    print("################## read csv ###################")
    
    json_data= read_from_json(spark)
    print("################## read json ##################")
    
    parq_data= read_from_parquet(spark)
    print("################ read parquet #################")
    
    mysql_data= read_from_mysql_and_denormalize(spark)
    print("########### read mysql and joing ##############")
    
    fraud= transform(csv_data, json_data, parq_data, mysql_data)
    print("################# union all ###################")
    print(fraud.columns)


    
    