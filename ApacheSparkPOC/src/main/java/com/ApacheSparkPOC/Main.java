package com.ApacheSparkPOC;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(3.5);
		inputData.add(4.56);
		inputData.add(4444.33);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("ApacheSparkPOC").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD javaRDD = sc.parallelize(inputData);
		
		
		
		sc.close();
	}

}
