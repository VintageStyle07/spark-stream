package sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public abstract class AbstractConsoleSink {
    public abstract StreamingQuery writeToConsole(Dataset<Row> data) throws StreamingQueryException;

}
