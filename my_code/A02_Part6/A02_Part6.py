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
def my_main(spark,
            my_dataset_dir,
            source_node
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("source", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("target", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("weight", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read' to create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", " ") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # --------------------------------------------------------
    # START OF STUDENT CODE
    # --------------------------------------------------------

    df = inputDF.withColumn("cost", pyspark.sql.functions.lit(-1))
    df = df.withColumn("path", pyspark.sql.functions.lit(""))
    df = df.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == source_node, 0).otherwise(-1))
    df = df.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == source_node, str(source_node)).otherwise(""))
    solutionDF = df.select("source").groupBy("source").agg({"source": "count"})
    solutionDF = solutionDF.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == source_node, 0).otherwise(-1))
    solutionDF = solutionDF.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == source_node, str(source_node)).otherwise(""))
    solutionDF = solutionDF.drop("count(source)").orderBy("source")
    num_nodes = df.groupBy("source").agg({"source": "count"}).count()
    for i in range(1, num_nodes):
        tempdf = df.filter((pyspark.sql.functions.col("target") == i+1) & (pyspark.sql.functions.col("cost") >= 0))
        minWeight = tempdf.agg({"weight": "min"}).collect()[0][0]
        iterRes = tempdf.filter(pyspark.sql.functions.col("weight") == minWeight)
        val = iterRes.collect()
        previousSource = val[0][0]
        weight = int(val[0][2])
        previousNode = solutionDF.filter(pyspark.sql.functions.col("source") == previousSource).collect()
        print("PreviousNode:")
        print(previousNode)
        prevCost = previousNode[0][1]
        print("Prev Cost: " , prevCost)
        print("weight: ", weight)
        prevPath = previousNode[0][2]
        solutionDF = solutionDF.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == i+1, pyspark.sql.functions.lit(prevCost + weight)).otherwise(pyspark.sql.functions.col("cost")))
        solutionDF = solutionDF.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == i+1, prevPath + "-" + str(i+1)).otherwise(pyspark.sql.functions.col("path")))
        df = df.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("target") == i+1, pyspark.sql.functions.lit(-1)).otherwise(pyspark.sql.functions.col("cost")))
        df.show()
        #iterRes.show()





    '''
    for i in range(1, num_nodes):
        minVal1 = targetDf.where((pyspark.sql.functions.col("source_team") == "Red") & (pyspark.sql.functions.col("target_team") != pyspark.sql.functions.col("source_team"))).agg({"weight": "min"})
        minVal = minVal1.collect()
        row = targetDf.where((pyspark.sql.functions.col("weight") == minVal[0][0]) & (pyspark.sql.functions.col("source_team") == "Red"))
        row.show()
        targetDf = targetDf.withColumn("source_team", pyspark.sql.functions.when((pyspark.sql.functions.col("source") == row["target"]) | (pyspark.sql.functions.col("source_team") == "Red"), "Red").otherwise("Blue"))
        print(minVal)
    targetDf.show()'''


    '''
    minVal1 = joinedDf.where(joinedDf["source"] == 5).agg({"weight": "min"})
    minVal = minVal1.collect()
    print(minVal[0][0])'''







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
    source_node = 1

    # 2. We select the Spark execution mode: Local (0), Google Colab (1) or Databricks (2)
    local_0_GoogleColab_1_databricks_2 = 2

    if (local_0_GoogleColab_1_databricks_2 == 1):
        import google.colab
        google.colab.drive.mount("/content/drive")

    # 3. We select the dataset we want to work with
    my_dataset_dir = "FileStore/tables/my_dataset_2/"

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
    my_main(spark,
            my_dataset_dir,
            source_node
           )

