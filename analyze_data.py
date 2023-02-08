import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from itertools import chain

config = configparser.ConfigParser()
config.read('aws_creds.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS_CREDS",
                                             "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS_CREDS",
                                                 "AWS_SECRET_ACCESS_KEY")


# State abbreviations and names dictionary

STATES = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
          "CA": "California", "CO": "Colorado", "CT": "Connecticut",
          "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
          "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
          "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
          "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan",
          "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
          "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
          "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
          "NY": "New York", "NC": "North Carolina", "ND": "North Dakota",
          "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
          "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
          "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
          "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
          "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"}


def create_spark_session():
    """
        This function creates a new SparkSession object, which is the
        entrypoint into all functionality in Spark. This code will
        create a new SparkSession if it doesn't already exist.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def clean_immigration_data(spark, input_data, output_data):
    """
        This function reads the input immigration data from S3
        as a CSV file, checks for missing and duplicate values in
        columns, and cleans the data, before writing the
        results back into S3.
    """
    immi_data = input_data + "i94_feb16_sub.csv"

    # read dataset
    immi_df = spark.read.csv(immi_data, header=True)

    # drop unneeded columns from immi df

    cols = ("_c0", "arrdate", "depdate", "count", "dtadfile", "visapost",
            "occup", "entdepa", "entdepd", "entdepu", "matflag", "dtaddto",
            "insnum", "airline", "admnum", "fltno", "biryear", "i94visa",
            "i94bir", "i94yr", "i94mon", "i94res", "i94cit", "i94addr")

    immi_df = immi_df.drop(*cols)

    # check for missing values and replace with empty string for string
    # columns

    immi_df_2 = immi_df.na.fill("")

    # check for duplicate values on primary key field (cicid)

    num_dup = immi_df_2.groupBy("cicid").count().where("count > 1")
    assert num_dup.count() == 0

    # change types of other columns from float to integer

    cols = ["cicid", "i94mode"]

    immi_df_3 = (
        immi_df_2.select(
            *(c for c in immi_df_2.columns if c not in cols),
            *(col(c).cast("integer").alias(c) for c in cols)
        )
    )

    # reorder columns to keep original order

    immi_df_3 = immi_df_3.select("cicid", "i94port", "i94mode",
                                 "visatype")


    # filter by i94mode variable to keep only air arrivals
    immi_df_4 = immi_df_3.filter(immi_df_3.i94mode == 1)

    # drop i94mode variable
    immi_df_4 = immi_df_4.drop(col("i94mode"))

    # check for duplicates across all columns

    assert immi_df_4.count() == immi_df_4.dropDuplicates(immi_df_4.columns).\
        count()

    # write immigration table to parquet files
    immi_df_4.write.mode("overwrite").parquet(output_data +
                                              "immigration.parquet")


def clean_temperature_data(spark, input_data, output_data):
    """
        This function reads the input temperature data from S3,
        checks for missing and duplicate values in columns, and cleans
        the data, before writing the
        results back into S3.
    """
    temp_data = input_data + "Global_Land_Temperatures_By_City.csv"
    temp_df = spark.read.csv(temp_data, header=True)

    # Keep only US temperature data
    temp_df_2 = temp_df.filter(temp_df.Country == "United States")

    # Drop unneeded columns from immi df

    cols = ("Latitude", "Longitude", "AverageTemperatureUncertainty")
    temp_df_3 = temp_df_2.drop(*cols)

    # round average temperature column 2 decimal places

    col_name = "AverageTemperature"
    temp_df_4 = temp_df_3.withColumn(col_name, round(col(col_name), 2))

    # keep only February historical temperatures using a regex

    expression = r'\d{4}-02-\d{2}'
    temp_df_5 = temp_df_4.filter(temp_df_4['dt'].rlike(expression))

    # check for missing values- none found!

    feb_count = temp_df_5.select([count(when(col(c).contains('NULL') |
                                             (col(c) == '') |
                                             isnan(c), c
                                             )).alias(c)
                                 for c in temp_df_5.columns])

    # check for duplicates across all columns
    # this resulted in 57292 - 57274 = 18 duplicate rows removed
    temp_df_6 = temp_df_5.dropDuplicates()
    assert temp_df_6.count() == temp_df_6.dropDuplicates(temp_df_6.columns)\
        .count()

    # Aggregate the data by City and find min, avg, max temperatures
    temp_df_7 = temp_df_6.groupBy("City") \
        .agg(round(min("AverageTemperature"), 2).alias("min_temp"),
             round(avg("AverageTemperature"), 2).alias("avg_temp"),
             round(max("AverageTemperature"), 2).alias("max_temp"))

    temp_df_7 = temp_df_7.withColumnRenamed("City", "city")

    # write temperature table to parquet files
    temp_df_7.write.mode("overwrite").parquet(output_data +
                                              "temperature.parquet")


def clean_immi_airport_code_data(spark, input_data, output_data):
    """
        This function reads the input airport code data from S3
        as a text file, checks for missing and duplicate values in
        columns, and cleans the data, before writing the
        results back into S3.
    """

    codes_data = input_data + "us_codes.txt"
    codes_df = spark.read.text(codes_data)

    # remove unneeded rows from data frame
    codes_df_1 = codes_df.filter(~col('value').contains('No PORT Code'))
    codes_df_1 = codes_df_1.filter(~col('value').contains('Collapsed'))

    split_code = split(codes_df_1['value'], '=')
    codes_df_2 = codes_df_1.withColumn('code', split_code.getItem(0)) \
        .withColumn('citystate', split_code.getItem(1))

    codes_df_3 = codes_df_2.drop(col("value"))

    split_city = split(codes_df_3['citystate'], ',')
    codes_df_4 = codes_df_3.withColumn('city', split_city.getItem(0)).\
        withColumn('state', split_city.getItem(1))
    codes_df_5 = codes_df_4.drop(col("citystate"))

    # Remove spaces and quotation marks
    codes_df_6 = codes_df_5.withColumn("code", translate(col("code"), "' ",
                                       ""))
    codes_df_6 = codes_df_6.withColumn("city", translate(col("city"), "'",
                                       ""))
    codes_df_6 = codes_df_6.withColumn("state", translate(col("state"), "' ",
                                       ""))

    # Remove tabs and convert city name to title case
    codes_df_7 = codes_df_6.withColumn('code', regexp_replace(col("code"),
                                       "\\\t", ""))
    codes_df_7 = codes_df_7.withColumn('city', initcap(lower(regexp_replace(
                                        col("city"), "\\t", ""))))

    lookup_map = create_map(*[lit(x) for x in chain(*STATES.items())])

    codes_df_8 = codes_df_7.withColumn("statename", lookup_map[col("state")])

    # Remove state abbreviation column from dataframe

    codes_df_9 = codes_df_8.drop(col("state"))

    # write airport codes table to parquet files
    codes_df_9.write.mode("overwrite").parquet(output_data + "codes.parquet")


# def sas_to_csv(input_data):
#     fh = input_data + "i94_feb16_sub.sas7bdat"
#     output_path = input_data + "i94_feb16_sub.csv"
#     df = pd.read_sas(fh, 'sas7bdat', encoding="ISO-8859-1")
#     df.to_csv(output_path)

def main():
    """
        This creates a new SparkSession and runs the functions defined
        above to process song and log JSON data files in S3.
    """
    spark = create_spark_session()
    input_data = "s3a://capstonebucketudacity/source_data/"
    output_data = "s3a://capstonebucketudacity/formatted_data/"

    # Convert the sas7bdat file into csv format
    clean_immigration_data(spark, input_data, output_data)
    clean_temperature_data(spark, input_data, output_data)
    clean_immi_airport_code_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
