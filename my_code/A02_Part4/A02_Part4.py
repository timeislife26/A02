# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark
import pyspark.sql.functions

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, loop_size):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("start_time", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("stop_time", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("trip_duration", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("start_station_id", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("start_station_name", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("start_station_latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("start_station_longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("stop_station_id", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("stop_station_name", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("stop_station_latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("stop_station_longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("bike_id", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("user_type", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("birth_year", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("gender", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("trip_id", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read' to create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # --------------------------------------------------------
    # START OF STUDENT CODE
    # --------------------------------------------------------
    df = inputDF.select("bike_id", "start_time", "stop_time", "start_station_name", "stop_station_name")
    my_window = pyspark.sql.Window.partitionBy(df["bike_id"]).orderBy(pyspark.sql.functions.col("start_time")) # Groups the data by bike_id
    for i in range(1,loop_size): #Create the Lag
        df = df.withColumn("start_time"+"_"+str(i), pyspark.sql.functions.lag(df["start_time"], (i * -1), None).over(my_window))
        df = df.withColumn("stop_time"+"_"+str(i), pyspark.sql.functions.lag(df["stop_time"], (i * -1), None).over(my_window))
        df = df.withColumn("start_station_name"+"_"+str(i), pyspark.sql.functions.lag(df["start_station_name"], (i * -1), None).over(my_window))
        df = df.withColumn("stop_station_name"+"_"+str(i), pyspark.sql.functions.lag(df["stop_station_name"], (i * -1), None).over(my_window))
    df = df.filter(pyspark.sql.functions.col("start_station_name") == pyspark.sql.functions.col("stop_station_name_" + str(loop_size-1))) # Fitler out loops that don't end where it starts
    df = df.withColumnRenamed("start_time", "start_time_"+"0")
    df = df.withColumnRenamed("stop_time", "stop_time_"+"0")
    df = df.withColumnRenamed("start_station_name", "start_station_name_"+"0")
    df = df.withColumnRenamed("stop_station_name", "stop_station_name_"+"0")
    for i in range(1,loop_size):
        df = df.filter(pyspark.sql.functions.col("start_station_name_" + str(i)) == pyspark.sql.functions.col("stop_station_name_" + str(i - 1))) # checs that sarting station is the same as last station
        for j in range(i, loop_size):
            df = df.filter(pyspark.sql.functions.col("start_station_name_" + str(i-1)) != pyspark.sql.functions.col("start_station_name_" + str(j)))

    solutionDF = df.orderBy("bike_id")










    # --------------------------------------------------------
    # END OF STUDENT CODE
    # --------------------------------------------------------

    # Operation A1: 'collect' to get all results
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    loop_size = 5

    # 2. We select the Spark execution mode: Local (0), Google Colab (1) or Databricks (2)
    local_0_GoogleColab_1_databricks_2 = 2

    if (local_0_GoogleColab_1_databricks_2 == 1):
        import google.colab
        google.colab.drive.mount("/content/drive")

    # 3. We select the dataset we want to work with
    my_dataset_dir = "FileStore/tables/my_dataset_1/"

    # 4. We set the path to my_dataset
    my_local_path = "../../"
    my_google_colab_path = "/content/drive/MyDrive/"
    my_databricks_path = "/"

    if (local_0_GoogleColab_1_databricks_2 == 0):
        my_dataset_dir = my_local_path + my_dataset_dir
    elif (local_0_GoogleColab_1_databricks_2 == 1):
        my_dataset_dir = my_google_colab_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, loop_size)

