package com.da.digital.writer;

import com.da.digital.conf.KafkaConfig;
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

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

@Component(value = "kafkawriter")
public class KafkaWriter implements Writer<Dataset<Row>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaWriter.class);

    @Value("${writerType}")
    private String writerType;

    @Value("${enableErrorRoute}")
    private String enableErrorRoute;

    @Autowired
    private KafkaConfig kafkaConfig;


    @Override
    public void write(Dataset<Row> output) throws OctopusException {

        String topic = "topic";
        String checkpointLocation = "checkpointLocation";
        String bootstrapServers = "kafka.bootstrap.servers";

        if (output.isStreaming()) {

            if (enableErrorRoute.equalsIgnoreCase("true")) {

                StreamingQuery error = output.filter((col("value").contains(Constant.BDP_PARSER_ERROR_SWITCH)))
                        .writeStream()
                        .outputMode(kafkaConfig.getOutputMode())
                        .format(writerType.toLowerCase())
                        .option(topic, kafkaConfig.getErrorOutTopic())
                        .option(checkpointLocation, kafkaConfig.getErrorCheckpointLocation())
                        .option(bootstrapServers, kafkaConfig.getBootstrapServers())
                        .start();

                StreamingQuery success = output
                        .filter(not((col("value").contains(Constant.BDP_PARSER_ERROR_SWITCH))))
                        .writeStream().outputMode(kafkaConfig.getOutputMode())
                        .format(writerType.toLowerCase())
                        .option(topic, kafkaConfig.getSuccessOutTopic())
                        .option(checkpointLocation, kafkaConfig.getSuccessCheckpointLocation())
                        .option(bootstrapServers, kafkaConfig.getBootstrapServers())
                        .start();

                try{
                    error.awaitTermination();
                    success.awaitTermination();
                }catch (StreamingQueryException ex){
                    logger.error(ex.getMessage());
                    throw new OctopusException(OctopusErrorCode.KAFKA_STREAMING_ERROR);
                }


            } else {
                try {
                    output.writeStream().outputMode(kafkaConfig.getOutputMode()).format(writerType.toLowerCase())
                            .option(topic, kafkaConfig.getSingleOutTopic())
                            .option(checkpointLocation, kafkaConfig.getSingleCheckpointLocation())
                            .option(bootstrapServers, kafkaConfig.getBootstrapServers())
                            .start().awaitTermination();
                }catch (StreamingQueryException ex){
                    logger.error(ex.getMessage());
                    throw new OctopusException(OctopusErrorCode.KAFKA_STREAMING_ERROR);

                }
            }

        } else {

            output.write().format(writerType.toLowerCase())
                    .option(topic, kafkaConfig.getSingleOutTopic())
                    .option(bootstrapServers, kafkaConfig.getBootstrapServers())
                    .save();

        }

    }
}
