package com.ApacheSparkPOC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class Main {
	
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.OFF);

		// System.out.print("dfdsfsdf");
		
		// SparkSession spark = SparkSession
		// 		  .builder()
		// 		  .appName("Java Spark SQL basic example")
		// 		  .config("spark.some.config.option", "some-value")
		// 		  .getOrCreate();

		SparkSession spark = SparkSession
						.builder()
						.appName("Java Spark SQL basic example")
						.config("spark.master", "local")
						.getOrCreate();

		
		FindLambda fl = new FindLambda(spark);
		fl.findLambdaValue();

		spark.stop();
		//  wordCount("input.txt");
	}
	
	public static void wordCount(String fileName) {		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("ApacheSparkPOC").setMaster("local");
		//Setspark serialization method
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
        JavaRDD<String> wordsFromFile = inputFile.flatMap(
			content -> Arrays.asList(content.split(" ")).iterator()
			);

        JavaPairRDD countData = wordsFromFile.mapToPair(
			t -> new Tuple2(t, 1)
			).reduceByKey((x, y) -> (int) x + (int) y);

        countData.saveAsTextFile("CountData");        
        sparkContext.close();
	}
}
