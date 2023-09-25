package com.da.digital.writer;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.DFSConfig;
import com.da.digital.conf.JobContext;
import com.da.digital.exception.DataAngosException;
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
@ContextConfiguration(classes = {WriterFactory.class, DFSConfig.class,WriterUtil.class, JobContext.class, AppConfig.class,DFSWriter.class})
@ActiveProfiles("test")
@SpringBootTest
@EnableAutoConfiguration
public class WriterFactorySuite {

    @Value("${writerType:}")
    private StringBuilder writerType;

    @Autowired
    private Map<String, Writer<Dataset<Row>>> mapOfWriter;

    @Autowired
    private DFSConfig dfsConfig;

    @Autowired
    private WriterFactory writerFactory;

    @Test
    public void testWriterFactory() throws DataAngosException {
        Assert.assertEquals("com.da.digital.writer.DFSWriter",writerFactory.getWriterObject().getClass().getName());
    }
}
