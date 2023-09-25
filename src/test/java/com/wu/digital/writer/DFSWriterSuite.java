package com.da.digital.writer;


import com.da.digital.TestUtil;
import com.da.digital.conf.AppConfig;
import com.da.digital.conf.DFSConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.exception.OctopusException;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DFSWriter.class, JobContext.class, AppConfig.class, TestUtil.class,
        DFSConfig.class, WriterUtil.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class DFSWriterSuite {

    @Autowired
    private DFSWriter dfsWriter;

    @Autowired
    private DFSConfig dfsConfig;

    @Value("${enableErrorRoute:}")
    private String enableErrorRoute;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private TestUtil testUtil;

    private Dataset<Row> inputDS;

    private String outFile = "src/test/resources/test-data/dfs-out/";

    private long lineCount;

    private String falseStr = "false";
    private String part = "part-*";
    private String parquet = "parquet";


    @Before
    public void setup() throws OctopusException {
        inputDS = sparkSession.read().format("csv").load("src/test/resources/test-data/test.csv");
        lineCount = inputDS.count();
        testUtil.deleteRecursively(new File(outFile));

    }


    @Test
    public void testCSVWriter() throws OctopusException {

        outFile = outFile + "csv";
        dfsConfig.setWriteFormat("csv");
        dfsConfig.setOutputFile(outFile);
        enableErrorRoute = falseStr;
        dfsWriter.write(inputDS);
        outFile = outFile + "/" + part;

        RDD<String> outFileRDD = sparkSession.sparkContext().textFile(outFile, 1);
        long outFileLength = outFileRDD.count();

        Assert.assertEquals(lineCount, outFileLength);

        List<String> outFileList = outFileRDD.toJavaRDD().collect();

        int i = 0;
        for (String str : outFileList) {
            Assert.assertEquals(inputDS.toJavaRDD().collect().get(i).mkString(","), str);
            i++;
        }
    }

    @Test
    public void testJSONWriter() throws OctopusException {

        outFile = outFile + "json";
        dfsConfig.setWriteFormat("json");
        dfsConfig.setOutputFile(outFile);
        enableErrorRoute = falseStr;
        dfsWriter.write(inputDS);
        outFile = outFile + "/" + part;

        Dataset<Row> outDS = sparkSession.read().json(outFile);

        Assert.assertEquals(lineCount, outDS.count());

        int i = 0;
        for (Row row : outDS.toJavaRDD().collect()) {
            Assert.assertEquals(inputDS.toJavaRDD().collect().get(i), row);
            i++;

        }
    }

    @Test
    public void testParquetWriter() throws OctopusException {

        testUtil.deleteRecursively(new File(outFile));
        outFile = outFile + parquet;
        dfsConfig.setWriteFormat(parquet);
        dfsConfig.setOutputFile(outFile);
        enableErrorRoute = falseStr;
        dfsWriter.write(inputDS);
        outFile = outFile + "/" + part;

        Dataset<Row> outDS = sparkSession.read().parquet(outFile);

        Assert.assertEquals(lineCount, outDS.count());

        int i = 0;
        for (Row row : outDS.toJavaRDD().collect()) {
            Assert.assertEquals(inputDS.toJavaRDD().collect().get(i), row);
            i++;

        }
    }

    @Test
    public void testTextWriter() throws OctopusException {

        outFile = outFile + "text";
        dfsConfig.setWriteFormat("text");
        dfsConfig.setOutputFile(outFile);
        enableErrorRoute = falseStr;
        dfsWriter.write(inputDS);
        outFile = outFile + "/" + part;

        RDD<String> outFileRDD = sparkSession.sparkContext().textFile(outFile, 1);
        long outFileLength = outFileRDD.count();

        Assert.assertEquals(lineCount, outFileLength);

        List<String> outFileList = outFileRDD.toJavaRDD().collect();

        int i = 0;
        for (String str : outFileList) {
            Assert.assertEquals(inputDS.toJavaRDD().collect().get(i).mkString(","), str);
            i++;
        }
    }

}
