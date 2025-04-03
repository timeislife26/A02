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

# ------------------------------------------------
# IMPORTS
# ------------------------------------------------
import pyspark
import pyspark.sql.functions


# ------------------------------------------
# FUNCTION my_spark_part_compute_graph
# ------------------------------------------
def my_spark_part_compute_graph(spark, my_dataset_dir):
    # 1. We create the output variable
    res = {}

    # 2. We define the Schema of our DF.
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

    # 3. Operation C1: 'read' to create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # --------------------------------------------------------
    # START OF STUDENT CODE
    # --------------------------------------------------------









    # --------------------------------------------------------
    # END OF STUDENT CODE
    # --------------------------------------------------------

    # Operation A1: 'collect' to get all results
    resVAL = solutionDF.collect()
    for item in resVAL:
        # 8.1. We parse the Row content
        start_station_name = item[0]
        neighbours = item[1]
        num_neighbours = item[2]

        # 8.2. We add the Row info to the dictionary we are returning
        res[start_station_name] = (num_neighbours, neighbours)

    # We return res
    return res


# ------------------------------------------
# FUNCTION my_python_part_compute_page_rank
# ------------------------------------------
def my_python_part_compute_page_rank(edges_per_node,
                                     reset_probability,
                                     max_iterations
                                    ):
    pass


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            reset_probability,
            max_iterations
           ):

    # 1. We compute the graph info using Spark
    graph_info = my_spark_part_compute_graph(spark, my_dataset_dir)

    # 2. We compute the shortest paths to each node
    my_python_part_compute_page_rank(graph_info,
                                     reset_probability,
                                     max_iterations
                                    )


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
    # 1. We get the input values
    reset_probability = 0.15
    max_iterations = 10

    # 2. We select the Spark execution mode: Local (0), Google Colab (1) or Databricks (2)
    local_0_GoogleColab_1_databricks_2 = 0

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

    # 4. We call to my_main
    my_main(spark,
            my_dataset_dir,
            reset_probability,
            max_iterations
           )
