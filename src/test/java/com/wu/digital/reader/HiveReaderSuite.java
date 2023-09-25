package com.da.digital.reader;

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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {HiveReader.class, HiveConfig.class, JobContext.class, AppConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class HiveReaderSuite {


    @Autowired
    HiveReader hiveReader;

    @Autowired
    HiveConfig hiveConfig;

    @Autowired
    SparkSession sparkSession;

    private Dataset<Row> inputDS;
    private List<String> listOfRecords;
    private List<Row> listOfRow;


    @Before
    public void setup() throws IOException {

        sparkSession.sql("DROP TABLE IF EXISTS sales");
        sparkSession.sql("CREATE EXTERNAL TABLE sales(Country String,Age int,Salary String," +
                "Purchased String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                "STORED AS TEXTFILE location 'src/test/resources/test-data/hive'");
        hiveConfig.setDatabaseName("default");
        hiveConfig.setSourceReaderSQL("SELECT * FROM sales");
        inputDS = hiveReader.read();
        Path path = Paths.get("src/test/resources/test-data/hive/sales");
        listOfRecords = Files.readAllLines(path);
        listOfRow = inputDS.toJavaRDD().collect();

    }

    @Test
    public void testNumberOfRows(){

        long lineCount = listOfRecords.size();
        Assert.assertEquals(lineCount, inputDS.count());
    }

    @Test
    public void testContent() {

        int i = 0;
        for (String record:listOfRecords) {
            Assert.assertEquals(record,listOfRow.get(i).mkString(","));
            i++;

        }
    }




}
