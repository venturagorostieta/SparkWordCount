package com.example.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * Word counter using Spark 2.3 and JDK 1.8
 * Load text in HDFS
 * Write output result to HDFS path
 * @author VENTURA
 *
 */
public class WordCount {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> textFile = sc
				.textFile("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/examples/spark/alice29.txt");
		JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/examples/spark/wordcountResult");

	}

}
