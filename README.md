# SparkWordCount
Example word counter using spark 2.3 and JDK 1.8

Command line for run example:

spark-submit \
  --class "com.example.spark.WordCount" \
  --master local[*] \
 /tmp/spark/SparkWordCounter.jar 
