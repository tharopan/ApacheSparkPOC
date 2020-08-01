package com.ApacheSparkPOC;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ClusterManager {
	
	public int findClusters(String algorithm, String datasetUrl) {
		
		algorithm = "libsvm";
		datasetUrl = "data/mllib/sample_kmeans_data.txt";
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("ApacheSparkPOC").setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		/*Dataset<Row> dataset = spark.read().format(algorithm).load(datasetUrl);
		
		// Trains a k-means model.
		KMeans kmeans = new KMeans().setK(2).setSeed(1L);
		KMeansModel model = kmeans.fit(dataset);

		// Make predictions
		Dataset<Row> predictions = model.transform(dataset);

		// Evaluate clustering by computing Silhouette score
		ClusteringEvaluator evaluator = new ClusteringEvaluator();

		double silhouette = evaluator.evaluate(predictions);
		System.out.println("Silhouette with squared euclidean distance = " + silhouette);

		// Shows the result.
		Vector[] centers = model.clusterCenters();
		
		return centers.count();*/
		return 1;
	}

}
