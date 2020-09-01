package com.ApacheSparkPOC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class FindLambda {

    SparkSession spark;

    public FindLambda(SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    public void findLambdaValue(){ 
        String dataUrl = "data/iris.csv";
        int tempLambda = 1;
        String format = "csv"; // "csv"

        DataSizeManager dsm  = new DataSizeManager(spark);        
        long maximumDataSize = dsm.calculateDataSize(dataUrl, format, tempLambda);

        String result = "Maximum data : " + maximumDataSize;
        System.out.println("Maximum data : " + maximumDataSize);

        // System.out.print("my Method");
    }
}
