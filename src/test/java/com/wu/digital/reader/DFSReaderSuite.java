package com.da.digital.reader;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.DFSConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.metadata.Metadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DFSReader.class, DFSConfig.class, Metadata.class, JobContext.class, AppConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class DFSReaderSuite {

    @Autowired
    private DFSReader dfsReader;

    private Dataset<Row> inputDS;
    private List<String> listOfRecords;
    private List<Row> listOfRow;

    @Value("${readerType:}")
    private String readerType ;

    @Autowired
    private DFSConfig dfsConfig;

    private String inputFile = "src/test/resources/test-data/test.txt";


    @Before
    public void setup() throws IOException {
        readerType = "dfs";
        dfsConfig.setInputFile(inputFile);
        dfsConfig.setWriteFormat("text");
        dfsConfig.setFieldDelim("/");
        inputDS = dfsReader.read();
        Path path = Paths.get(inputFile);
        listOfRecords = Files.readAllLines(path);
        listOfRow = inputDS.toJavaRDD().collect();
    }


    @Test
    public void testNumberOfRows() {

        long lineCount = listOfRecords.size();

        Assert.assertEquals(lineCount, inputDS.count());
    }

    @Test
    public void testContent() {



        int i = 0;
        for (String record:listOfRecords) {
            Assert.assertEquals(record,listOfRow.get(i).mkString("/"));
            i++;

        }
    }


}
