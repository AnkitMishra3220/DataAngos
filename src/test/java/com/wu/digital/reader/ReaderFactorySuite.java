package com.da.digital.reader;


import com.da.digital.conf.AppConfig;
import com.da.digital.conf.DFSConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.exception.DataAngosException;
import com.da.digital.metadata.Metadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {ReaderFactory.class, DFSConfig.class, Metadata.class, JobContext.class,AppConfig.class,DFSReader.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class ReaderFactorySuite {

    @Autowired
    private Map<String, Reader<Dataset<Row>>> mapOfReader;

    @Autowired
    private DFSReader dfsReader;

    @Value("${readerType:}")
    private String readerType;

    @Autowired
    private ReaderFactory readerFactory;

    @Test
    public void testReaderFactory() throws DataAngosException {

        Assert.assertEquals("com.da.digital.reader.DFSReader",readerFactory.getReaderObject().getClass().getName());

    }

}
