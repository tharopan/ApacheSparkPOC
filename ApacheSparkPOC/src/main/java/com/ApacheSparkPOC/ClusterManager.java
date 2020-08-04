package com.ApacheSparkPOC;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ClusterManager {
	
	public int findClusters(SparkSession spark, MLibAlgorithm algorithm , String datasetUrl) {		
		switch(algorithm) {
		  case KMeans:
		    return kMeans(spark, datasetUrl);
		    //break;
		  case GMMs:
		    return GMMs(spark, datasetUrl);
		    //break;
		  default:
			  return 0;
		    // code block
		}
	}
	
	public int kMeans(SparkSession spark, String datasetUrl) {
		String algorithm = "libsvm";
		datasetUrl = "data/mllib/sample_kmeans_data.txt";
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("ApacheSparkPOC").setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		Dataset<Row> dataset = spark.read().format(algorithm).load(datasetUrl);
		
		// Trains a k-means model.
		/*KMeans kmeans = new KMeans().setK(2).setSeed(1L);
		KMeansModel model = kmeans.fit(dataset);

		// Make predictions
		Dataset<Row> predictions = model.transform(dataset);

		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator evaluator = new ClusteringEvaluator();

		//double silhouette = evaluator.evaluate(predictions);
		//System.out.println("Silhouette with squared euclidean distance = " + silhouette);

		// Shows the result.
		Vector[] centers = model.clusterCenters();
		
		return centers.count();*/
		return 1;
	}

	public int GMMs(SparkSession spark, String datasetUrl) {
		int counter = 0;
		
		String format = "libsvm";
		datasetUrl = "data/mllib/sample_kmeans_data.txt";
		// Loads data
		Dataset<Row> dataset = spark.read().format(format).load(datasetUrl);

		// Trains a GaussianMixture model
	//	GaussianMixture gmm = new GaussianMixture().setK(2);
		
	//	GaussianMixtureModel model = gmm.fit(dataset);

		/*// Output the parameters of the mixture model
		for (int i = 0; i < model.getK(); i++) {
		  System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
		          i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
		}*/
		
		//return model.getK().length();
		
		return counter;
	}
	
	public int LDDA(SparkSession spark, String datasetUrl) {
		return 1;
	}
	
}
