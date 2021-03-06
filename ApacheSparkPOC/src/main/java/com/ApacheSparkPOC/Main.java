package com.ApacheSparkPOC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {
	
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.OFF);

		// String dataUrl = "C:/Users/Tharo/Documents/ResearchProject/Data/Input/sample_isotonic_regression_libsvm_data.txt";
		String dataUrl = "C:/Users/Tharo/Documents/ResearchProject/Data/TwitterInput/";
		String dataUrl1 = "C:/Users/Tharo/Documents/ResearchProject/Data/TwitterInput1/";
		long heapThresholdvalue = 512 * 1024 * 8;

		// SparkSession spark = SparkSession
		// 		  .builder()
		// 		  .appName("Java Spark SQL basic example")
		// 		  .config("spark.some.config.option", "some-value")
		// 		  .getOrCreate();

		//https://spark.apache.org/docs/latest/configuration.html
		// 1b (bytes)
		// 1k or 1kb (kibibytes = 1024 bytes)
		// 1m or 1mb (mebibytes = 1024 kibibytes)
		// 1g or 1gb (gibibytes = 1024 mebibytes)
		// 1t or 1tb (tebibytes = 1024 gibibytes)
		// 1p or 1pb (pebibytes = 1024 tebibytes)
		SparkSession spark = SparkSession
						.builder()
						.appName("Java Spark SQL basic example")
						.config("spark.master", "local")
						.config("spark.eventLog.enabled", true)
						.config("spark.eventLog.dir", "file:///C:/Spark/Log")
						.config("spark.driver.memory", "512m")
						.config("spark.memory.fraction", 0.6)
						.config("spark.executor.memory", "512m")
						.config("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
						.getOrCreate();
		
		// FindLambda fl = new FindLambda(spark);
		// fl.Run();
				
		long startTime = System.nanoTime();
		
		ProcessManager pm = new ProcessManager(spark);		
		pm.ProcessEAnalyser(dataUrl, "json", heapThresholdvalue);	
		// pm.Process(dataUrl, "json", heapThresholdvalue);	
		
		// String csvDataUrl = "C:/Spark/spark-3.0.0-bin-hadoop2.7/data/mllib/sample_kmeans_data.txt";
		// ClusterManager cm = new ClusterManager(spark);
		// int numberofCluster = cm.findClusters(MLibAlgorithm.KMeans, csvDataUrl, "csv");		
		// System.out.println("Number of cluster(s)" + numberofCluster);

		long stopTime = System.nanoTime();
		System.out.println("Total elasped time: " + TimeUnit.SECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS) + " Sec");
		
		// EarthquakeAnalyser eq = new EarthquakeAnalyser(spark);		
		// eq.AnalyseTweetsWithBulkData(dataUrl, 3.45);
		
		spark.stop();
		//  wordCount("input.txt");
	}
	
	// public static void wordCount(String fileName) {		
	// 	SparkConf sparkConf = new SparkConf();
	// 	sparkConf.setAppName("ApacheSparkPOC").setMaster("local");
	// 	//Setspark serialization method
	// 	sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
	// 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    //     JavaRDD<String> inputFile = sparkContext.textFile(fileName);
    //     JavaRDD<String> wordsFromFile = inputFile.flatMap(
	// 		content -> Arrays.asList(content.split(" ")).iterator()
	// 		);

    //     JavaPairRDD countData = wordsFromFile.mapToPair(
	// 		t -> new Tuple2(t, 1)
	// 		).reduceByKey((x, y) -> (int) x + (int) y);

    //     countData.saveAsTextFile("CountData");        
    //     sparkContext.close();
	// }
}
