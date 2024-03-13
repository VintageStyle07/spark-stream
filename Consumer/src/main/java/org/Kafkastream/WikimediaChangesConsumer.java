package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.util.LongAccumulator;

import java.util.concurrent.TimeoutException;

import Source.KafkaSource; // Import KafkaSource
import Transformation.DataTransformation; // Import DataTransformation
import sink.ConsoleSink; // Import ConsoleSink

public class KafkaSparkConsumer {
    public static void main(String[] args) throws TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaStreamingExample")
                .config("spark.master", "local")
                .getOrCreate();

        // Define accumulator for message count
        LongAccumulator messageAccumulator = spark.sparkContext().longAccumulator("Message Accumulator");

        KafkaSource kafkaSource = new KafkaSource();
        DataTransformation dataTransformation = new DataTransformation();
        ConsoleSink consoleSink = new ConsoleSink();

        Dataset<Row> kafkaData = kafkaSource.readData(spark, "employee_data");
        Dataset<Row> transformedData = dataTransformation.transform(kafkaData);

        // Processing logic
        StreamingQuery query = transformedData.writeStream()
                .outputMode("append")
                .foreachBatch((batch, batchId) -> {
                    long count = batch.count();
                    messageAccumulator.add(count); // Increment accumulator by the count of records in the batch
                    System.out.println("BatchId: " + batchId + ", Count: " + count);
                    // Additional processing can be done here on the batch data
                })
                .trigger(Trigger.ProcessingTime("1 seconds")) // Adjust trigger as needed
                .start();

        try {
            query.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Access accumulator value after the streaming query has been terminated
            long totalMessages = messageAccumulator.value();
            System.out.println("Total messages processed: " + totalMessages);
        }
    }
}
