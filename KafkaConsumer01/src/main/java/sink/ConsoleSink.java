package sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.concurrent.TimeoutException;

public class ConsoleSink extends AbstractConsoleSink {
    @Override
    public StreamingQuery writeToConsole(Dataset<Row> data)  {
        try {
            return data.writeStream()
                    .outputMode("update")
                    .format("console")
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
