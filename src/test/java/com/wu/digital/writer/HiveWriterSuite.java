package com.da.digital.writer;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.HiveConfig;
import com.da.digital.conf.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {HiveWriter.class, JobContext.class, AppConfig.class,HiveConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class HiveWriterSuite {

    @Autowired
    private SparkSession sparkSession;

    private Dataset<Row> inputDS;

    @Autowired
    private HiveConfig hiveConfig;

    @Autowired
    private HiveWriter hiveWriter;

    @Before
    public void setup() {
        inputDS = sparkSession.read().format("csv").load("src/test/resources/test-data/test.csv");

    }

    @Test
    public void testHiveWriter(){
        hiveConfig.setPartitionColumn("");
        hiveConfig.setDatabaseName("test");
        hiveConfig.setOutTableName("test_table");
        hiveConfig.setSaveMode("overwrite");
        hiveWriter.write(inputDS);
        Dataset<Row> outDS = sparkSession.sql("SELECT * FROM test.test_table");
        Assert.assertEquals(inputDS.count(),outDS.count());
        Assert.assertEquals(inputDS.collectAsList().get(0).getString(0),outDS.collectAsList().get(0).getString(0));

    }


}
