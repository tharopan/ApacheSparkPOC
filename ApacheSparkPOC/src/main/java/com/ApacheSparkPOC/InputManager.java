package com.ApacheSparkPOC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InputManager {

    public Dataset<Row> readData(SparkSession spark, int take){

        Dataset<Row> dataset = null;
        dataset = spark.read().format("libsvm").load("").limit(take);

        return dataset;
    }

    public void writeData(Dataset<Row> dataSet ){
        
    }
}