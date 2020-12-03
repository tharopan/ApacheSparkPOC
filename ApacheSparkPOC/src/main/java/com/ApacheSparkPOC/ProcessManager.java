package com.ApacheSparkPOC;

import org.apache.spark.sql.SparkSession;

public class ProcessManager {

    SparkSession spark;
    DataSize dSize;
    EarthquakeAnalyser eAnalyser;
    CSVDataAnalyser csvDataAnalyser;

    public ProcessManager(SparkSession spark) {
        this.spark = spark;
        dSize = new DataSize(spark);
        eAnalyser = new EarthquakeAnalyser(spark);
        csvDataAnalyser = new CSVDataAnalyser(spark);
    }

    public void ProcessEAnalyser(String dataUrl, String format, long heapThresholdvalue){
        eAnalyser.AnalyseTweetsWithBulkData(dataUrl, 19999);
        // long dataSize = dSize.calculateDataSize(
        //                     dataUrl, 
        //                     format
        //                     );
        
        // if(dataSize > heapThresholdvalue){
        //     eAnalyser.AnalyseTweetsWithBulkData(dataUrl, heapThresholdvalue/dataSize);
        // } else {
        //     //eAnalyser.AnalyseTweetsWithBulkData(dataUrl, heapThresholdvalue/dataSize);
        //      eAnalyser.AnalyseTweets(dataUrl);
        // }
    }

    public void Process(String dataUrl, String format, long heapThresholdvalue){
        csvDataAnalyser.AnalyseData(dataUrl, 19999);
        // long dataSize = dSize.calculateDataSize(
        //                     dataUrl, 
        //                     format
        //                     );
        
        // if(dataSize > heapThresholdvalue){
        //     eAnalyser.AnalyseTweetsWithBulkData(dataUrl, heapThresholdvalue/dataSize);
        // } else {
        //     //eAnalyser.AnalyseTweetsWithBulkData(dataUrl, heapThresholdvalue/dataSize);
        //      eAnalyser.AnalyseTweets(dataUrl);
        // }
    }
}