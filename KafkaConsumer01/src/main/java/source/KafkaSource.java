package source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class KafkaSource extends AbstractSource {
    @Override
    public Dataset<String> read(SparkSession spark) {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "wikimedia")
                .load()
                .selectExpr("CAST(value AS STRING) AS json_value")
                .as(Encoders.STRING());
    }
}
