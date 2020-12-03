package com.ApacheSparkPOC;


import org.apache.spark.mllib.util.MLUtils;
import java.lang.reflect.Array;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import static org.apache.spark.sql.functions.col;
import java.util.*;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.util.SizeEstimator;
import shapeless.ops.function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
// import org.apache.spark.sql.functions.Array_Contains;


public class CSVDataAnalyser {
    private SparkSession spark;

	public CSVDataAnalyser(SparkSession sparkSession)
	{
		this.spark = sparkSession;
	}
    
    public void AnalyseData(String dataurl, double fraction)
    {
        System.out.println("Execute Data:");
        Dataset<Row> data = spark
                            .read()
                            .option("header", "true")
                            .format("csv")
                            .load("C:/Users/Tharo/Documents/ResearchProject/Data/CSVInputBulk/*.csv");

        
                                    
        System.out.println(data.count());

        // Dataset<Row> parallelizedData = data.select("text");

        // System.out.println(parallelizedData.count());
    }

    public void AnalyseWithBulkData(String dataUrl, double fraction)
    {
        System.out.println("Execute Bulk Data:");

        int numberOfsplits = (int)Math.round(fraction) + 1;
        int numberOfConcurrentTask = spark.sparkContext().maxNumConcurrentTasks();
    
        Dataset<Row> data = spark
                            .read()
                            .option("header", "true")
                            .format("csv")
                            .load("C:/Users/Tharo/Documents/ResearchProject/Data/CSVInputBulk/*.csv")
                            .limit(10000);
                                    
        System.out.println(data.count());

        //Persist data into memory
        data.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("Estimated size of data: " + SizeEstimator.estimate(data));

        Dataset<Row> parallelizedData = data.select("text");

        System.out.println(parallelizedData.count());
    }
}
