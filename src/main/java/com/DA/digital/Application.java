package com.da.digital;

import com.da.digital.exception.OctopusException;
import com.da.digital.processor.ProcessorFactory;
import com.da.digital.reader.ReaderFactory;
import com.da.digital.writer.WriterFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration
public class Application implements CommandLineRunner {

    @Autowired
    private ReaderFactory readerFactory;

    @Autowired
    private ProcessorFactory<Dataset<Row>, Dataset<Row>> processorFactory;

    @Autowired
    private WriterFactory writerFactory;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws OctopusException {

        writerFactory.getWriterObject().write(processorFactory.process(readerFactory.getReaderObject().read()));

    }
}