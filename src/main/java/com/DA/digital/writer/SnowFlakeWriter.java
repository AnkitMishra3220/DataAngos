package com.da.digital.writer;


import com.da.digital.conf.ContextSerialization;
import com.da.digital.conf.SnowColumnsList;
import com.da.digital.conf.SnowFlakeConfig;
import com.da.digital.exception.OctopusErrorCode;
import com.da.digital.exception.OctopusException;
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

import java.io.Serializable;
import java.util.List;

@Component(value = "snowflakewriter")
public class SnowFlakeWriter implements Writer<Dataset<Row>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SnowFlakeWriter.class);

    @Autowired
    SnowFlakeConfig snowFlakeConfig;

    @Autowired
    ContextSerialization contextSerialization;
    @Autowired
    WriterUtil writerUtil;
    @Autowired
    private SnowColumnsList snowColumnsList;
    @Value("${enableErrorRoute}")
    private String enableErrorRoute;

    @Override
    public void write(Dataset<Row> output) throws OctopusException {

        String checkpointLocation = "checkpointLocation";
        List<String> colList = snowColumnsList.getColList();
        SnowFlakeJDBCSinkWriter snowFlakeJDBCSinkWriter = new SnowFlakeJDBCSinkWriter(contextSerialization.getSnowFlakeConfigObj(), colList);

        switch (enableErrorRoute.toLowerCase()) {
            case "true":
                StreamingQuery success = output.
                        filter(col(Constant.BDP_PARSER_ERROR_SWITCH)
                                .isNull())
                        .drop(col(Constant.BDP_PARSER_ERROR_SWITCH)).drop(col("ts")).drop(col("payload")).drop(col(Constant.ERROR))
                        .writeStream().foreach(snowFlakeJDBCSinkWriter)
                        .outputMode("append")
                        .option(checkpointLocation, snowFlakeConfig.getSuccessCheckpointLocation())
                        .start();

                StreamingQuery error = output.filter(col(Constant.BDP_PARSER_ERROR_SWITCH).isNotNull())
                        .select(col(Constant.ERROR), col("ts"), col("payload"))
                        .writeStream().
                                option(checkpointLocation, snowFlakeConfig.getErrorCheckPointLocation()).
                                option("path", snowFlakeConfig.getErrorRecordsLocation())
                        .start();

                try {
                    success.awaitTermination();
                    error.awaitTermination();
                } catch (StreamingQueryException ex) {
                    logger.error(ex.getMessage());
                    throw new OctopusException(OctopusErrorCode.SNOWFLAKE_STREAMING_ERROR);
                }
                break;
            case "false":
                try {
                    output.
                            filter(col(Constant.BDP_PARSER_ERROR_SWITCH)
                                    .isNull())
                            .drop(col(Constant.BDP_PARSER_ERROR_SWITCH)).drop(col("ts")).drop(col("payload")).drop(col(Constant.ERROR))
                            .writeStream().foreach(snowFlakeJDBCSinkWriter)
                            .outputMode("append")
                            .option(checkpointLocation, snowFlakeConfig.getSuccessCheckpointLocation())
                            .start().awaitTermination();
                } catch (StreamingQueryException e) {
                    logger.error(e.getMessage());
                    throw new OctopusException(OctopusErrorCode.SNOWFLAKE_STREAMING_ERROR);
                }

        }
    }

}
