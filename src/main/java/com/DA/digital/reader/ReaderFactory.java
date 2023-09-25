package com.da.digital.reader;

import com.da.digital.conf.AppConfig;
import com.da.digital.exception.OctopusErrorCode;
import com.da.digital.exception.OctopusException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import java.util.Map;

@Component
public class ReaderFactory implements Serializable {

    @Value("${readerType}")
    private String readerType;

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private Map<String, Reader<Dataset<Row>>> mapOfReader;

    public Reader<Dataset<Row>> getReaderObject() throws OctopusException {

        try {
            return mapOfReader.get(readerType.toLowerCase());

        }catch (NullPointerException e){
            throw new OctopusException(OctopusErrorCode.UNKNOWN_READER_TYPE);
        }

    }
}
