package com.da.digital.reader;


import com.da.digital.KafkaTestConfig;
import com.da.digital.conf.AppConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.conf.KafkaConfig;
import com.da.digital.exception.OctopusErrorCode;
import com.da.digital.exception.OctopusException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {KafkaReader.class, KafkaConfig.class, JobContext.class, AppConfig.class, KafkaTestConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class KafkaReaderSuite {

    private static final Logger logger = LoggerFactory.getLogger(KafkaReaderSuite.class);

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private KafkaReader kafkaReader;

    @Autowired
    SparkSession spark;

    @ClassRule
    public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true,
            "inTopic");

    private String inData = "";

    @Before
    public void setUp() throws OctopusException, InterruptedException {

        final Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

        final ProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(senderProperties);

        KafkaTemplate template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(kafkaConfig.getInTopic());

        //send a sample input record and wait for acknowledgement
        Integer key = 1234;
        try {
            inData = new String(Files.readAllBytes(Paths.get("src/test/resources/test-data/kafka.txt")));
            template.send(kafkaConfig.getInTopic(), key,inData).get();
        } catch (IOException e){
            logger.error(e.getMessage());
            throw new OctopusException(OctopusErrorCode.IO_FILE_ERROR);
        }
        catch (ExecutionException e) {
            logger.error(e.getMessage());
            throw new OctopusException(OctopusErrorCode.EXECUTION_EXCEPTION);
        }

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
    public void testKafkaRecord() {

        List<Row> outList; outList = spark.sql("select * from Output").collectAsList();
        Row outData = outList.get(0) ;
        Assert.assertEquals(inData,outData.getString(3));
        Assert.assertEquals(kafkaConfig.getInTopic(),outData.get(0));
    }
}
