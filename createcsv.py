from pyspark.sql import SparkSession
import pandas as pd

# Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("Adding Data from CSV to PostgreSQL") \
    .config("spark.driver.extraClassPath", "postgresql-42.7.2.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.7.2.jar") \
    .getOrCreate()

# Wczytaj dane z pliku CSV za pomocą Pandas
pandas_df = pd.read_csv("latlon_RZ.csv", sep=",", dtype=str)

pandas_df = pandas_df.drop('Unnamed: 0', axis=1)

# Konwersja ramki danych Pandas na ra
# mkę danych Sparka
df = spark.createDataFrame(pandas_df)

# Zapis danych do bazy danych PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/my_database") \
    .option("dbtable", "table_name5") \
    .option("user", "my_user") \
    .option("password", "123") \
    .mode("append") \
    .save()

# Zakończenie sesji Spark
spark.stop()
