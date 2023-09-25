package com.da.digital;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.conf.KafkaConfig;
import com.da.digital.reader.KafkaReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {KafkaReader.class, KafkaConfig.class, JobContext.class, AppConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class KafkaReaderTest {

    @Autowired
    KafkaReader kafkaReader;

    @Autowired
    KafkaConfig kafkaConfig;

    @Autowired
    SparkSession sparkSession;

    @Before
    public void setup() throws InterruptedException {

        kafkaConfig.setInTopic("test_json_input");
        kafkaConfig.setBootstrapServers("localhost:9092");
        kafkaConfig.setStartingOffsets("earliest");

        kafkaReader.read()
                .selectExpr("topic","partition","CAST(key AS STRING) as key","CAST(value AS STRING) as value")
                .writeStream()
                .format("memory")
                .queryName("Output")
                .outputMode(OutputMode.Append())
                .start()
                .processAllAvailable();

        Thread.sleep(1500);
    }

    @Test
    public void testKafkaReader()
    {

        List<Row> outList; outList = sparkSession.sql("select * from Output").collectAsList();

        System.out.println("OutlIst======>"+outList);

    }
}
