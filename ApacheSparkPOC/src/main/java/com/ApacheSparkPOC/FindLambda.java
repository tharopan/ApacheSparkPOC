package com.ApacheSparkPOC;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class FindLambda {

    SparkSession spark;

    DataSizeManager dsm;   

    public FindLambda(SparkSession sparkSession) 
    {
        this.spark = sparkSession;
        dsm = new DataSizeManager(spark);
    }

    public void Run()
	{
		File folder = new File("C:/Users/Tharo/Documents/ResearchProject/ApacheSparkPOC/ApacheSparkPOC/Data/CalculateLambda");
		File[] listOfFiles = folder.listFiles();
		String outputFile = "C:/Users/Tharo/Documents/ResearchProject/ApacheSparkPOC/ApacheSparkPOC/Data/Output/CalculateLambda.txt";
		StringBuilder sb = new StringBuilder();

		try (
			BufferedReader br = Files.newBufferedReader(
				Paths.get(
					outputFile
				));
			BufferedWriter bw = Files.newBufferedWriter(
				Paths.get(
					outputFile
					));
			)
		{
			String line;
			while ((line = br.readLine()) != null) 
			{
                sb.append(line).append("\n");
			}
			
			for (File file : listOfFiles) {
				if (file.isFile()) {
					System.out.println(file.getName());
					long calculatedFileSize = dsm.calculateDataSize(file.getAbsolutePath(), "csv", 1);
					long actulFileSize = file.length();
					System.out.println(
						file.getName() + 
						", Actual file size = " + 
						actulFileSize + 
						", Calculated file size = " + 
						calculatedFileSize);
	
					String strLine = "ActualFileSize= " + actulFileSize + "\t\t\t\t\t::" + "CalculatedFileSize= " + calculatedFileSize;
					sb.append(strLine).append("\n");
				}
			}

			bw.write(sb.toString());
		}
		catch (IOException e) 
		{
            System.err.format("IOException: %s%n", e);
		}		
	}

    // public void findLambdaValue(String dataUrl){ 
    //     // String dataUrl = "data/iris.csv";
    //     int tempLambda = 1;
    //     String format = "csv"; // "csv"

    //     DataSizeManager dsm  = new DataSizeManager(spark);        
    //     long maximumDataSize = dsm.calculateDataSize(dataUrl, format, tempLambda);

    //     String result = "Maximum data : " + maximumDataSize;
    //     System.out.println("Maximum data : " + maximumDataSize);

    //     // System.out.print("my Method");
    // }
}
