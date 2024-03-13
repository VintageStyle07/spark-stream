package transformation;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import sink.AbstractConsoleSink;

import java.util.Collections;

public class DataTransformation extends AbstractTransformation {
    private final AbstractConsoleSink consoleSink;

    public DataTransformation(AbstractConsoleSink consoleSink) {
        this.consoleSink = consoleSink;
    }

    @Override
    public StreamingQuery transform(Dataset<String> data, String outputPath) throws StreamingQueryException {
        Dataset<Row> transformedData = data.mapPartitions((MapPartitionsFunction<String, Row>) iterator -> {
            long batchCount = 0;
            while (iterator.hasNext()) {
                iterator.next();
                batchCount++;
            }
            System.out.println("Batch number: " + batchCount);
            return Collections.singletonList(RowFactory.create(batchCount)).iterator();
        }, Encoders.bean(Row.class));

        return consoleSink.writeToConsole(transformedData);
    }

    @Override
    public Dataset<String> transform(Dataset<String> data) {
        // You can implement a different transformation here if needed.
        return data;
    }
}
