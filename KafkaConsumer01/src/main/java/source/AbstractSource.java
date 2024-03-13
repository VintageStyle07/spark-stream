package source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractSource {
    public abstract Dataset<String> read(SparkSession spark);
}
