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

    df = inputDF.withColumn("cost", pyspark.sql.functions.lit(0))
    #df = df.withColumn("cost", pyspark.sql.functions.when(df["source"] == source_node,0).otherwise(-1))
    teamsDF = df.groupBy("source").agg({"source": "count"})
    teamsDF = teamsDF.withColumn("team", pyspark.sql.functions.when(df["source"] == source_node,"Red").otherwise("Blue")).select("source", "team").orderBy("source")
    num_nodes = teamsDF.count()
    teamsDF = teamsDF.withColumnRenamed("source", "temp_source")
    joinedDf = df.join(teamsDF, df["source"] == teamsDF["temp_source"], "full_outer")
    joinedDf = joinedDf.withColumnRenamed("team", "source_team")
    joinedDf = joinedDf.drop("temp_source")
    solutionDF = inputDF.groupBy("source").agg({"source": "count"})
    solutionDF = solutionDF.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == source_node, 0).otherwise(-1))
    solutionDF = solutionDF.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == source_node, str(source_node)).otherwise(""))
    solutionDF = solutionDF.drop("count(source)")


    targetDf = joinedDf.join(teamsDF, teamsDF["temp_source"] == joinedDf["target"], "full_outer")#.select(joinedDf["source"], joinedDf["target"], joinedDf["source_team"], teamsDF["team"])
    targetDf = targetDf.select("source", "target", "weight", "cost", "source_team", "team")
    targetDf = targetDf.withColumnRenamed("team", "target_team").orderBy("source")
    targetDf = targetDf.withColumn("node_a", pyspark.sql.functions.least("source", "target")) \
                        .withColumn("node_b", pyspark.sql.functions.greatest("source", "target"))
    targetDf = targetDf.dropDuplicates(["node_a", "node_b"])
    targetDf = targetDf.drop("node_a", "node_b")
    targetDf = targetDf.withColumn("reached_target", pyspark.sql.functions.lit(False))
    targetDf = targetDf.withColumn("path", pyspark.sql.functions.lit(str(source_node)))
    for i in range(1, num_nodes):
        tempDF = (targetDf.where((pyspark.sql.functions.col("source_team") == "Red") & (pyspark.sql.functions.col("target_team") == "Blue")))
        tempDF = tempDF.withColumnRenamed("source", "temp_source")
        tempDF = tempDF.withColumnRenamed("target", "temp_target")
        tempDF = tempDF.withColumnRenamed("target_team", "temp_target_team")
        tempDF = tempDF.withColumnRenamed("path", "temp_path")

        tempDF = tempDF.orderBy("weight")
        tempDF = tempDF.withColumn("cost", pyspark.sql.functions.col("cost") + pyspark.sql.functions.col("weight"))
        tempDF = tempDF.limit(1)
        tempDF = tempDF.withColumnRenamed("cost", "temp_cost")
        tempDF = tempDF.select("temp_source", "temp_target", "temp_cost","temp_target_team", "temp_path")
        tempDF = tempDF.withColumn("temp_target_team", pyspark.sql.functions.lit("Red"))
        temp_path = pyspark.sql.functions.concat(pyspark.sql.functions.col("temp_path").cast("string"), pyspark.sql.functions.lit("-"), pyspark.sql.functions.col("target").cast("string"))
        update_path = pyspark.sql.functions.concat(pyspark.sql.functions.col("temp_path").cast("string"), pyspark.sql.functions.lit("-"), pyspark.sql.functions.col("source").cast("string"))



        targetDf = targetDf.join(tempDF, (targetDf["source"] == tempDF["temp_source"]) & (targetDf["target"] == tempDF["temp_target"]), "left")
        targetDf = targetDf.withColumn("target_team", pyspark.sql.functions.when(pyspark.sql.functions.col("temp_target_team").isNotNull(), pyspark.sql.functions.col("temp_target_team")).otherwise(pyspark.sql.functions.col("target_team")))
        targetDf = targetDf.withColumn("reached_target", pyspark.sql.functions.when(pyspark.sql.functions.col("temp_target_team").isNotNull(), True).otherwise(pyspark.sql.functions.col("reached_target")))


        targetDf = targetDf.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("temp_target_team").isNotNull(), pyspark.sql.functions.concat(
            temp_path
        )).otherwise(pyspark.sql.functions.col("path")))


        targetDf = targetDf.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("temp_cost").isNotNull(), pyspark.sql.functions.col("temp_cost")).otherwise(pyspark.sql.functions.col("cost")))
        targetDf = targetDf.select("source", "target", "weight", "cost", "source_team", "target_team", "reached_target", "path")
        targetDf = targetDf.join(tempDF, targetDf["source"] == tempDF["temp_target"], "left")

        targetDf = targetDf.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("source") == pyspark.sql.functions.col("temp_target"), pyspark.sql.functions.concat(
            update_path
        )).otherwise(pyspark.sql.functions.col("path")))

        targetDf = targetDf.withColumn("source_team", pyspark.sql.functions.when(pyspark.sql.functions.col("temp_cost").isNotNull(), "Red").otherwise(pyspark.sql.functions.col("source_team")))
        targetDf = targetDf.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("temp_cost").isNotNull(), pyspark.sql.functions.col("temp_cost")).otherwise(pyspark.sql.functions.col("cost")))
        targetDf = targetDf.select("source", "target", "weight", "cost", "source_team", "target_team", "reached_target", "path")

    targetDf = targetDf.filter(targetDf["reached_target"] == True)
    solutionDF = solutionDF.withColumnRenamed("source", "id")
    solutionDF = solutionDF.withColumnRenamed("cost", "c")
    solutionDF = solutionDF.withColumnRenamed("path", "p")
    solutionDF = solutionDF.join(targetDf, solutionDF["id"] == targetDf["target"], "left")
    solutionDF = solutionDF.orderBy("id")
    solutionDF = solutionDF.withColumn("cost", pyspark.sql.functions.when(pyspark.sql.functions.col("c") >= 0, pyspark.sql.functions.col("c")).otherwise(pyspark.sql.functions.col("cost")))
    solutionDF = solutionDF.withColumn("path", pyspark.sql.functions.when(pyspark.sql.functions.col("p") != "", pyspark.sql.functions.col("p")).otherwise(pyspark.sql.functions.col("path")))
    solutionDF = solutionDF.select("id", "cost", "path")










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

