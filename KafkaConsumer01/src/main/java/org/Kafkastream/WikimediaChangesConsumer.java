package org.Kafkastream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import sink.ConsoleSink;
import source.AbstractSource;
import source.KafkaSource;
import transformation.AbstractTransformation;
import transformation.DataTransformation;

public class WikimediaChangesConsumer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaStreamingExample")
                .config("spark.master", "local")
                .getOrCreate();

        AbstractSource source = new KafkaSource();
        Dataset<String> rawStream = source.read(spark);

        AbstractTransformation transformation = new DataTransformation(new ConsoleSink());
        Dataset<String> transformedData = transformation.transform(rawStream);

        try {
            transformedData.writeStream()
                    .outputMode("update")
                    .format("console")
                    .start()
                    .awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
