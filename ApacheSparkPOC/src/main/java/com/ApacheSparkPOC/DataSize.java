package com.ApacheSparkPOC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class DataSize {

    private SparkSession spark;
    private DataSizeManager dsm;

	public DataSize(SparkSession sparkSession)
	{
        this.spark = sparkSession;
        dsm = new DataSizeManager(spark);
    }
    
    public long calculateDataSize(String datasetUrl, String format){
        double lambda = 1;
        return dsm.calculateDataSize(datasetUrl, format, lambda);
    }
}
