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
		// TODO Auto-generated method stub
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(3.5);
		inputData.add(4.56);
		inputData.add(4444.33);
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		wordCount("input.txt");
	}
	
	public static void wordCount(String fileName) {
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("ApacheSparkPOC").setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        countData.saveAsTextFile("CountData");
        
        sparkContext.close();
	}

}
