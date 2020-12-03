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

public class EarthquakeAnalyser
{
    private SparkSession spark;

	public EarthquakeAnalyser(SparkSession sparkSession)
	{
		this.spark = sparkSession;
	}
    
    public void AnalyseTweets(String dataurl)
    {
        System.out.println("Execute Data:");
        Dataset<Row> data = spark
                            .read()
                            .json(dataurl);

        //Persist data into memory
        //data.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> inputPositiveData = spark
                            .read()
                            .textFile("C:/Users/Tharo/Documents/ResearchProject/Data/Input/positive.txt")
                            .toDF("text");

        Dataset<Row> parallelizedData = data.select("text");

        Dataset<Row> filteredDataSet = parallelizedData.join(
            inputPositiveData,
            parallelizedData.col("text").contains(inputPositiveData.col("text")));

        //JavaRDD<List<String>> inpufile = filteredDataSet.javaRDD().map(row -> row.getList(0));

        System.out.println(filteredDataSet.count());
        
        // JavaRDD<String> wordsFromFile = inpufile.flatMap(
        //     content -> Arrays.asList().iterator()
        // );
        
            //     JavaPairRDD countData = wordsFromFile.mapToPair(
            // 		t -> new Tuple2(t, 1)
            // 		).reduceByKey((x, y) -> (int) x + (int) y);
            // JavaRDD<String> wordsFromFile = filteredDataSet.flatMap(
            //     		content -> Arrays.asList(content.split(" ")).iterator()
            //     		);
            
            //         JavaPairRDD countData = wordsFromFile.mapToPair(
            //     		t -> new Tuple2(t, 1)
            //     		).reduceByKey((x, y) -> (int) x + (int) y);
        
    
            
                //     JavaPairRDD countData = wordsFromFile.mapToPair(
                // 		t -> new Tuple2(t, 1)
                // 		).reduceByKey((x, y) -> (int) x + (int) y);
            

        System.out.println(filteredDataSet.count());
    }

    public void AnalyseTweetsWithBulkData(String dataUrl, double fraction)
    {
        System.out.println("Execute Bulk Data:");
        int numberOfsplits = (int)Math.round(fraction) + 1;

        int numberOfConcurrentTask = spark.sparkContext().maxNumConcurrentTasks();
    
        JavaRDD rdd = spark
                            .read()
                            .json(dataUrl).toJavaRDD();
        
        Dataset<Row> data = spark.createDataFrame(rdd, Twitter.class);
        //Persist data into memory
        //data.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("Estimated size of data: " + SizeEstimator.estimate(data));

        Dataset<Row> inputPositiveData = spark
                            .read()
                            .textFile("C:/Users/Tharo/Documents/ResearchProject/Data/Input/positive.txt")
                            .toDF("text");

        
        Dataset<Row> parallelizedData = data.select("text");

        System.out.println(parallelizedData.count());
        
        Dataset<Row> filteredDataSet = parallelizedData.join(
            inputPositiveData,
            parallelizedData.col("text").contains(inputPositiveData.col("text")));

        System.out.println(filteredDataSet.count());
            // JavaRDD<String> wordsFromFile = filteredDataSet.flatMap(
            //     		content -> Arrays.asList(content.split(" ")).iterator()
            //     		);
            
            //         JavaPairRDD countData = wordsFromFile.mapToPair(
            //     		t -> new Tuple2(t, 1)
            //     		).reduceByKey((x, y) -> (int) x + (int) y);
        
    
            
                //     JavaPairRDD countData = wordsFromFile.mapToPair(
                // 		t -> new Tuple2(t, 1)
                // 		).reduceByKey((x, y) -> (int) x + (int) y);
            

        

        // Dataset<Row> filteredDataSet1 = data.flatMap(la, encoder)
        // // Prepare training and test data.
        // Dataset<Row>[] splits = data.randomSplit(new double[] {0.9, 0.1}, 12345);
        // Dataset<Row> training = splits[0];
        // Dataset<Row> test = splits[1];

        // LinearRegression lr = new LinearRegression();

        // // We use a ParamGridBuilder to construct a grid of parameters to search over.
        // // TrainValidationSplit will try all combinations of values and determine best model using
        // // the evaluator.
        // ParamMap[] paramGrid = new ParamGridBuilder()
        //                         .addGrid(lr.regParam(), new double[] {0.1, 0.01})
        //                         .addGrid(lr.fitIntercept())
        //                         .addGrid(lr.elasticNetParam(), new double[] {0.0, 0.5, 1.0})
        //                         .build();
        
        // // In this case the estimator is simply the linear regression.
        // // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
        // TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
        //                         .setEstimator(lr)
        //                         .setEvaluator(new RegressionEvaluator())
        //                         .setEstimatorParamMaps(paramGrid)
        //                         .setTrainRatio(0.8);  // 80% for training and the remaining 20% for validation

        // // Run train validation split, and choose the best set of parameters.
        // TrainValidationSplitModel model = trainValidationSplit.fit(training);

        // // Make predictions on test data. model is the model with combination of parameters
        // // that performed best.
        // model.transform(test)
        // .select("features", "label", "prediction")
        // .show();
    }
}