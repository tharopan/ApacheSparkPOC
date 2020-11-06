package com.ApacheSparkPOC;

import org.apache.spark.sql.SparkSession;

public class ProcessManager {

    SparkSession spark;
    DataSize dSize;
    EarthquakeAnalyser eAnalyser;

    public ProcessManager(SparkSession spark) {
        this.spark = spark;
        dSize = new DataSize(spark);
        eAnalyser = new EarthquakeAnalyser(spark);
    }

    public void Process(String dataUrl, String format, long heapThresholdvalue){
        long dataSize = dSize.calculateDataSize(
                            dataUrl, 
                            format
                            );
        
        if(dataSize > heapThresholdvalue){
            eAnalyser.AnalyseTweetsWithBulkData(dataUrl, heapThresholdvalue/dataSize);
        } else {
            eAnalyser.AnalyseTweetsWithBulkData(dataUrl, heapThresholdvalue/dataSize);
            // eAnalyser.AnalyseTweets(dataUrl);
        }
    }
}