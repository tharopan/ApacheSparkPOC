package com.ApacheSparkPOC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class DataSizeManager {

	private SparkSession spark;

	public DataSizeManager(SparkSession sparkSession){
		this.spark = sparkSession;
	}

	//This method will return the size in bites
	public long calculateDataSize(String datasetUrl, String format, double lambda){
		//size in bits
		long size= 0;		
		lambda = lambda == 0 ? 1 : lambda;

		Dataset<Row> dataset = spark.read().format(format).load(datasetUrl);
		
		long totalRows = dataset.count();		
		StructField[] fields = dataset.schema().fields();		
		String dataType = null;
		
		for(StructField field: fields) {
			dataType = field.dataType().typeName();
			
			switch(dataType) {
				case "int":
					size = size + (long)(lambda * 32 * totalRows);
					break;
				case "long":
					size = size + (long) (lambda * 64 * totalRows);
					break;
				case "string":
					size = size + (long) (lambda * 2147483647 * totalRows);
					break;
				case "float":
					size = size + (long) (lambda * 32 * totalRows);
					break;
				case "double":
					size = size + (long) (lambda * 64 * totalRows);
					break;
				case "boolean":
					size = size + 1 * totalRows;
					break;
				case "short":
					size = size + (long )(lambda * 16 * totalRows);
					break;
				case "char":
					size = size + (long) (lambda * 16 * totalRows);
					break;
				case "byte":
					size = size +  (long) (lambda * 8 * totalRows);
					break;
				default:			  
				// code block
			}
		}
		
		return size;			
	}
}
