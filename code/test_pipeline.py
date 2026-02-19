from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)
from pyspark.sql.functions import col

from data_cleaning_and_preprocessing import *

# create spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("pipeline-test") \
    .getOrCreate()


# sample dataframe for testing

def create_raw_df():

    schema = StructType([
        StructField("Job ID", IntegerType(), True),
        StructField("Agency", StringType(), True),
        StructField("Posting Type", StringType(), True),
        StructField("# Of Positions", IntegerType(), True),
        StructField("Business Title", StringType(), True),
        StructField("Job Category", StringType(), True),
        StructField("Full-Time/Part-Time indicator", StringType(), True),
        StructField("Salary Range From", DoubleType(), True),
        StructField("Salary Range To", DoubleType(), True),
        StructField("Salary Frequency", StringType(), True),
        StructField("Minimum Qual Requirements", StringType(), True),
        StructField("Posting Date", StringType(), True),
    ])

    data = fake_data = [
            (
                87990,
                "DEPARTMENT OF BUSINESS SERV.",
                "Internal",
                1,
                "Senior Account Manager",
                "Finance",
                "F",
                42405.0,
                65485.0,
                "Annual",
                "Bachelor degree and two years experience",
                "2011-06-24"
            ),
            (
                87991,
                "DEPARTMENT OF HEALTH",
                "External",
                3,
                "Data Analyst",
                "IT",
                "F",
                55000.0,
                82000.0,
                "Annual",
                "Bachelor degree in Computer Science",
                "2019-03-15"
            ),
            (
                87992,
                "DEPARTMENT OF EDUCATION",
                "Internal",
                2,
                "HR Coordinator",
                "Human Resources",
                "P",
                25.0,
                40.0,
                "Hourly",
                "3 years HR experience required",
                "2020-07-10"
            ),
            (
                87993,
                "DEPARTMENT OF TRANSPORTATION",
                "External",
                5,
                "Civil Engineer",
                "Engineering",
                "F",
                72000.0,
                98000.0,
                "Annual",
                "Engineering degree and PE license preferred",
                "2018-11-01"
            ),
            (
                87994,
                "DEPARTMENT OF FINANCE",
                "External",
                1,
                "Budget Analyst",
                "Finance",
                "F",
                60000.0,
                90000.0,
                "Annual",
                "Master degree preferred with 5 years experience",
                "2021-01-20"
            )
        ]


    return spark.createDataFrame(data, schema)


# -------------------------------------------------
# Full Production Pipeline
# -------------------------------------------------

def run_full_pipeline(df):

    # data cleaning and preprocessing
    df = clean_data(df)
    df = preprocess_columns(df)

    return df


# -------------------------------------------------
# Integration Test
# -------------------------------------------------

def test_full_pipeline():

    df = create_raw_df()
    df = run_full_pipeline(df)

    # Basic structural checks
    assert "Business_Title" in df.columns
    #assert "mean_salary" in df.columns
    #assert "salary_bucket" in df.columns
    #assert "is_residency_required" in df.columns

    # Salary mean
    #assert df.filter(col("mean_salary").isNull()).count() == 0

    print("Pipeline executed successfully!")


# -------------------------------------------------
# Manual Runner
# -------------------------------------------------

def run_all_tests():
    print("Running Full Pipeline Test...\n")
    test_full_pipeline()
    print("All tests passed successfully!")
