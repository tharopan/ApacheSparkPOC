package com.ApacheSparkPOC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class DataSizeManager {
	//This method will return the size in bites
		public long CalculateDataSize(SparkSession spark, String datasetUrl){
			
			//size in bits
			long size= 0;
			
			String format = "libsvm";
			
			Dataset<Row> dataset = spark.read().format(format).load(datasetUrl);
			
			
			long totalRows = dataset.count();
			
			StructField[] fields = dataset.schema().fields();
			
			String dataType = null;
			
			for(StructField field: fields) {
				dataType = field.dataType().typeName();
				
				switch(dataType) {
					case "int":
					    size = size +  32 * totalRows;
					    break;
					case "long":
					    size = size + 64 * totalRows;
						break;
					case "String":
						size = size + 2147483647 * totalRows;
						break;
					default:					  
				    // code block
				}
			}
			
			//for(int i= 0, i< dataset.count(); i++) {
				
			//}		
			
			return size;
			
		}
}
