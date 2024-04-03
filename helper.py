from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# create dataframe from csv file
def create_spark_dataframe():
    spark = SparkSession.builder \
        .appName("COVID-19 Analysis") \
        .getOrCreate()
    df = spark.read.option("header", "true").csv("covid_data.csv")
    return df


# Find the most affected country
def most_affected_country(df):
    max_death_rate = df.select(col("country").alias("most_affected_country"),"total_deaths","total_cases",(col("total_deaths") / col("total_cases")).alias("death_rate"))
    max_death_rate_value = max_death_rate.agg({"death_rate": "max"}).collect()[0][0]
    most_affected_countries = max_death_rate.filter(max_death_rate["death_rate"] == max_death_rate_value)
    return most_affected_countries


# Find the least affected country
def least_affected_country(df):
    min_death_rate = df.select(col("country").alias("least_affected_country"),"total_deaths","total_cases",(col("total_deaths") / col("total_cases")).alias("death_rate"))
    min_death_rate_value = min_death_rate.agg({"death_rate": "min"}).collect()[0][0]
    least_affected_countries = min_death_rate.filter(min_death_rate["death_rate"] == min_death_rate_value)
    return least_affected_countries


# Find country with highest COVID cases
def country_with_highest_cases(df):
    df = df.select(col("country").alias("country_with_highest_cases"),"total_cases")
    max_cases = df.agg({"total_cases": "max"}).collect()[0][0]
    countries_with_highest_cases = df.filter(df["total_cases"] == max_cases)
    return countries_with_highest_cases


# Find country with minimum COVID cases
def country_with_minimum_cases(df):
    df = df.select(col("country").alias("country_with_minimum_cases"),"total_cases")
    min_cases = df.agg({"total_cases": "min"}).collect()[0][0]
    countries_with_minimum_cases = df.filter(df["total_cases"] == min_cases)
    return countries_with_minimum_cases


# Find total cases
def total_cases(df):
    total_cases = df.groupBy().sum("total_cases").collect()[0][0]
    return total_cases


# Find country that handled COVID most efficiently
def most_efficient_country(df):
    df = df.withColumn("recovery_to_cases_ratio", df["total_recovered"] / df["total_cases"])
    df = df.select(col("country").alias("most_efficient_country"),"total_recovered","total_cases","recovery_to_cases_ratio")
    max_efficiency = df.agg({"recovery_to_cases_ratio": "max"}).collect()[0][0]
    most_efficient_countries = df.filter(df["recovery_to_cases_ratio"] == max_efficiency)
    return most_efficient_countries


# Find country that handled COVID least efficiently
def least_efficient_country(df):
    df = df.withColumn("recovery_to_cases_ratio", df["total_recovered"] / df["total_cases"])
    df = df.select(col("country").alias("least_efficient_country"),"total_recovered","total_cases","recovery_to_cases_ratio")
    min_efficiency = df.agg({"recovery_to_cases_ratio": "min"}).collect()[0][0]
    least_efficient_countries = df.filter(df["recovery_to_cases_ratio"] == min_efficiency)
    return least_efficient_countries


# Find country with least critical cases
def country_with_least_critical_cases(df):
    df = df.withColumn("critical_cases", col("critical").cast("int"))
    df = df.select(col("country").alias("country_with_least_critical_cases"),"critical_cases")
    min_critical_cases = df.agg({"critical_cases": "min"}).collect()[0][0]
    countries_with_least_critical_cases = df.filter(df["critical_cases"] == min_critical_cases)
    return countries_with_least_critical_cases


# Find country with highest critical cases
def country_with_highest_critical_cases(df):
    df = df.withColumn("critical_cases", col("critical").cast("int"))
    df = df.select(col("country").alias("country_with_highest_critical_cases"),"critical_cases")
    max_critical_cases = df.agg({"critical_cases": "max"}).collect()[0][0]
    countries_with_highest_critical_cases = df.filter(df["critical_cases"] == max_critical_cases)
    return countries_with_highest_critical_cases
