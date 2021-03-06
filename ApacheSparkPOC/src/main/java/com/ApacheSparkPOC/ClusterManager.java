	package com.ApacheSparkPOC;

import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ClusterManager {

	SparkSession spark;

	public ClusterManager(SparkSession sprk){
		spark = sprk;
	}

	public int findClusters(MLibAlgorithm algorithm , String datasetUrl, String format) {
		int numberOfCluster = 0;	
		switch(algorithm) {
		  	case KMeans:
		  		numberOfCluster = kMeans(datasetUrl, format);
		    	break;
		 	case GMMs:
		  		numberOfCluster = GMMs(datasetUrl, format);
				break;
			case BisectingKMeans:
				numberOfCluster = bisectingKMeans(datasetUrl, format);
				break;		
		 	 default:
		    	// code block
		}

		return numberOfCluster;
	}
	
	public int kMeans(String datasetUrl, String format) {
		format = "libsvm";
		
		Dataset<Row> dataset = spark.read().format(format).load(datasetUrl);
		
		// Trains a k-means model.
		KMeans kmeans = new KMeans().setK(2).setSeed(1L);
		KMeansModel model = kmeans.fit(dataset);
		
		// Make predictions
		Dataset<Row> predictions = model.transform(dataset);

		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator evaluator = new ClusteringEvaluator();

		Vector[] centers = model.clusterCenters();
		
		if(centers != null)
			return centers.length;
		else
			return 0;
	}

	public int bisectingKMeans(String dataUrl, String format){
		format = "libsvm";
		// Loads data.
		Dataset<Row> dataset = spark.read().format(format).load(dataUrl);

		// Trains a bisecting k-means model.
		BisectingKMeans bkm = new BisectingKMeans().setK(2).setSeed(1);
		BisectingKMeansModel model = bkm.fit(dataset);

		// Make predictions
		Dataset<Row> predictions = model.transform(dataset);

		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator evaluator = new ClusteringEvaluator();

		double silhouette = evaluator.evaluate(predictions);
		System.out.println("Silhouette with squared euclidean distance = " + silhouette);

		// Shows the result.
		System.out.println("Cluster Centers: ");
		Vector[] centers = model.clusterCenters();

		return centers.length;
	}

	public int GMMs(String datasetUrl, String format) {
		int counter = 0;
		
		format = "libsvm";
		datasetUrl = "data/mllib/sample_kmeans_data.txt";
		// Loads data
		Dataset<Row> dataset = spark.read().format(format).load(datasetUrl);

		// Trains a GaussianMixture model
		GaussianMixture gmm = new GaussianMixture().setK(2);
		
		GaussianMixtureModel model = gmm.fit(dataset);
		
		return model.getK();
	}
	
	public int LDDA(SparkSession spark, String datasetUrl) {
		return 1;
	}
}
