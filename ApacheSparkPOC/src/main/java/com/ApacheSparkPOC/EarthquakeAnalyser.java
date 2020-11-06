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
        Dataset<Row> data = spark
                            .read()
                            .json(dataurl);

        //Persist data into memory
        //data.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> inputPositiveData = spark
                .read()
                .textFile("C:/Users/Tharo/Documents/ResearchProject/Data/Input/positive.txt")
                .toDF();

        Dataset<Row> filteredData =  data.filter(col("text").isin(inputPositiveData)).select("text");

        filteredData.show();

        // // Split data into training (60%) and test (40%).
        // val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L);
        // val training = splits(0).cache();
        // val test = splits(1);

        // // Run training algorithm to build the model
        // val numIterations = 100
        // val model = SVMWithSGD.train(training, numIterations)

        // // Clear the default threshold.
        // model.clearThreshold()

        // // Compute raw scores on the test set. 
        // val scoreAndLabels = test.map { point =>
        // val score = model.predict(point.features)
        // (score, point.label)
        // }
        
        // // Get evaluation metrics.
        // val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        // val auROC = metrics.areaUnderROC()
        
        // println("Area under ROC = " + auROC)
    }

    public void AnalyseTweetsWithBulkData(String dataUrl, double fraction)
    {
        int numberOfsplits = (int)Math.round(fraction) + 1;

        int numberOfConcurrentTask = spark.sparkContext().maxNumConcurrentTasks();
    
        // Dataset<Row> data = spark.  
        //                     read().
        //                     format("libsvm").
        //                     load(dataUrl);//sample_earthquate_tweets.txt

        Dataset<Row> data = spark
                            .read()
                            .json(dataUrl);
        
        //Persist data into memory
        //data.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println(SizeEstimator.estimate(data));

        Dataset<Row> inputPositiveData = spark
                            .read()
                            .textFile("C:/Users/Tharo/Documents/ResearchProject/Data/Input/positive.txt")
                            .toDF("text");

        // ArrayList<String> positiveStrings = new ArrayList<String>();

        // inputPositiveData.foreach((ForeachFunction<Row>) row ->  positiveStrings.add(row.toString()));

        // System.out.println(positiveStrings.toArray().length);

        // Dataset<Row> filteredData =  data.filter(
        //     inputPositiveData.foreach({
        //         (ForeachFunction<Row>)row -> return col("text").contains(row.toString());
        //     })).select("text");

        Dataset<Row> parallelizedData = data.select("text");

        // Dataset<Row> filteredDataSet = data.select("text").where(
        //         functions.array_contains(col("text"), positiveStrings.contains(col("text")))
        //     );
        // filteredDataSet.show();

        Dataset<Row> filteredDataSet = parallelizedData.join(
            inputPositiveData,
            parallelizedData.col("text").contains(inputPositiveData.col("text")));

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