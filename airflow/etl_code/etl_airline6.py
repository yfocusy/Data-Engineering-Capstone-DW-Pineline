import os
from pyspark.sql.functions import udf, when
from pyspark.sql.session import SparkSession



def create_spark_session():
    spark = SparkSession.builder \
        .appName("Project: Yuli Capstone") \
        .getOrCreate()
    return spark


def spark_read_csv(spark):
    df_sk_airlines = spark.read.format('csv').option('header', 'true').load(
        '.././data/lookups/airline_company_lookup.csv')
    return df_sk_airlines

def clean_data(df_sk_airlines):
    df_sk_airlines = df_sk_airlines.withColumn("airline_name", when(
        (df_sk_airlines["airline_name"].isNull()) | (df_sk_airlines["airline_name"] == "-") | (
                    df_sk_airlines["airline_name"] == "\\N"), "unknown").otherwise(df_sk_airlines["airline_name"]))
    df_sk_airlines = df_sk_airlines.withColumn("iata_designator", when(
        (df_sk_airlines["iata_designator"].isNull()) | (df_sk_airlines["iata_designator"] == "\\N"),
        "unknown").otherwise(df_sk_airlines["iata_designator"]))

    return df_sk_airlines


if __name__ == '__main__':
    mode_file_path = '.././data/lookups/6.airline.json'
    if not os.path.exists(mode_file_path):
        spark_session = create_spark_session()
        df_sk_airlines = spark_read_csv(spark_session)
        df_sk_airlines = clean_data(df_sk_airlines)
        df_sk_airlines.coalesce(1).write.format('json').mode('overwrite').save('.././data/lookups/6.airline')
