package com.da.digital.writer;

import com.da.digital.conf.DFSConfig;
import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import com.da.digital.metadata.Constant;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;


@Component
public class WriterUtil {

    private static final Logger logger = LoggerFactory.getLogger(WriterUtil.class);

    @Autowired
    private DFSConfig dfsConfig;

    @Value("${enableErrorRoute}")
    private String enableErrorRoute;


    public Dataset<Row> getSuccessDS(Dataset<Row> writerDS) {

        return writerDS.filter(not((col(getColName(writerDS)).contains(Constant.BDP_PARSER_ERROR_SWITCH))));
    }

    public Dataset<Row> getErrorsDS(Dataset<Row> writerDS) {

        return writerDS.filter((col(getColName(writerDS)).contains(Constant.BDP_PARSER_ERROR_SWITCH)));

    }

    public String getColName(Dataset<Row> dataset) {
        if (dataset.isStreaming()) {
            return "value";
        } else {
            return dataset.columns()[0];

        }

    }

    public void executeCSVWriter(Dataset<Row> writerDS) {

        String writerFormat = "csv";

        if (enableErrorRoute.equalsIgnoreCase("true")) {
            getSuccessDS(writerDS)
                    .write().format(writerFormat).save(dfsConfig.getSuccessOutputFile());
            getErrorsDS(writerDS)
                    .write().format(writerFormat).save(dfsConfig.getErrorOutputFile());
        } else {
            writerDS.write().format(writerFormat).save(dfsConfig.getOutputFile());
        }

    }

    public void executeJSONWriter(Dataset<Row> writerDS) {

        if (enableErrorRoute.equalsIgnoreCase("true")) {
            getSuccessDS(writerDS)
                    .write().json(dfsConfig.getSuccessOutputFile());
            getErrorsDS(writerDS)
                    .write().json(dfsConfig.getErrorOutputFile());

        } else {
            writerDS.write().json(dfsConfig.getOutputFile());
        }
    }

    public void executeParquetWriter(Dataset<Row> writerDS) throws DataAngosException {

        String checkpointLocation = "checkpointLocation";

        if (writerDS.isStreaming()) {
            switch (enableErrorRoute.toLowerCase()) {
                case "true":
                    StreamingQuery success = getSuccessDS(writerDS)
                            .writeStream()
                            .option(checkpointLocation, dfsConfig.getSuccessCheckpointLocation())
                            .option("path", dfsConfig.getSuccessCheckpointLocation())
                            .start();

                    StreamingQuery error = getErrorsDS(writerDS)
                            .writeStream()
                            .option(checkpointLocation, dfsConfig.getSuccessCheckpointLocation())
                            .option("path", dfsConfig.getSuccessCheckpointLocation())
                            .start();
                    try {
                        success.awaitTermination();
                        error.awaitTermination();
                    } catch (StreamingQueryException ex) {
                        logger.error(ex.getMessage());
                        throw new DataAngosException(DataAngosErrorCode.KAFKA_STREAMING_ERROR);
                    }
                    break;

                case "false":
                    writerDS.writeStream()
                            .option(checkpointLocation, dfsConfig.getSingleCheckpointLocation())
                            .option("path", dfsConfig.getOutputFile())
                            .start();
                    break;

                default:
                    throw new DataAngosException(DataAngosErrorCode.UNKNOWN_ERROR_ROUTE);

            }

        } else {
            switch (enableErrorRoute.toLowerCase()) {
                case "true":
                    getSuccessDS(writerDS)
                            .write().parquet(dfsConfig.getSuccessOutputFile());
                    getErrorsDS(writerDS)
                            .write().parquet(dfsConfig.getErrorOutputFile());
                    break;
                case "false":
                    writerDS.write().parquet(dfsConfig.getOutputFile());
                    break;

                default:
                    throw new DataAngosException(DataAngosErrorCode.UNKNOWN_ERROR_ROUTE);
            }
        }


    }

    public void executeTextWriter(Dataset<Row> writerDS) {

        if (enableErrorRoute.equalsIgnoreCase("true")) {
            getSuccessDS(writerDS)
                    .toJavaRDD().map(x -> x.mkString(",")).saveAsTextFile(dfsConfig.getSuccessOutputFile());
            getErrorsDS(writerDS)
                    .toJavaRDD().map(x -> x.mkString(",")).saveAsTextFile(dfsConfig.getErrorOutputFile());
        } else {
            writerDS.rdd().toJavaRDD().map(x -> x.mkString(",")).saveAsTextFile(dfsConfig.getOutputFile());
        }
    }


}
