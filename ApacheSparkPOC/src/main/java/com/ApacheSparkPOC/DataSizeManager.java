package com.ApacheSparkPOC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSizeManager {

	//This method will return the size in bites
	public long CalculateDataSize(SparkSession spark, String datasetUrl){
		
		Dataset<Row> dataset = spark.read().format(format).load(datasetUrl);
		
		
		for(int i= 0, i< dataset.column(); i++) {
			
		}
		
	}
}
