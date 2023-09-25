package com.da.digital.reader;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.FileConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.exception.DataAngosException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
@ContextConfiguration(classes = {FileReader.class, FileConfig.class, JobContext.class, AppConfig.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class FileReaderSuite {

    @Autowired
    FileReader fileReader;

    @Autowired
    FileConfig fileConfig;

    private Dataset<Row> inputDS;
    private List<String> listOfRecords;
    private List<Row> listOfRow;

    @Before
    public void setup() throws DataAngosException, IOException {

        fileConfig.setInputFile("src/test/resources/test-data/test.csv");
        fileConfig.setHeaderRequired("true");
        fileConfig.setFileType("csv");
        inputDS = fileReader.read();
        Path path = Paths.get(fileConfig.getInputFile());
        listOfRecords = Files.readAllLines(path);
        listOfRecords.remove(0);
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
            Assert.assertEquals(record,listOfRow.get(i).mkString(","));
            i++;

        }
    }


}
