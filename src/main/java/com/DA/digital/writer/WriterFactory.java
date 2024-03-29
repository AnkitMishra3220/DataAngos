package com.da.digital.writer;

import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Map;

@Component
public class WriterFactory implements Serializable {

    @Value("${writerType}")
    private StringBuilder writerType;

    @Autowired
    private Map<String, Writer<Dataset<Row>>> mapOfWriter;

    public Writer<Dataset<Row>> getWriterObject() throws DataAngosException {

        writerType.append("writer");

        try {
            return mapOfWriter.get(writerType.toString().toLowerCase());
        } catch (NullPointerException ex) {
            throw new DataAngosException(DataAngosErrorCode.UNKNOWN_WRITER_TYPE);
        }


    }


}
