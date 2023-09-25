package com.da.digital.reader;

import com.da.digital.conf.KafkaConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component(value = "kafka")
public class KafkaReader implements Reader<Dataset<Row>>, Serializable {

    @Autowired
    private SparkSession spark;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Override
    public Dataset<Row> read() {

      return spark.readStream().format("kafka").option("kafka.bootstrap.servers",kafkaConfig.getBootstrapServers())
              .option("subscribe",kafkaConfig.getInTopic())
              .option("startingOffsets", kafkaConfig.getStartingOffsets().toLowerCase())
              .load();
    }
}
