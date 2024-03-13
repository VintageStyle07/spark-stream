package transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public abstract class AbstractTransformation {

    public abstract StreamingQuery transform(Dataset<String> data, String outputPath) throws StreamingQueryException;

    public abstract Dataset<String> transform(Dataset<String> data);
}
