package com.da.digital.writer;

import com.da.digital.conf.DFSConfig;
import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.Serializable;

@Component(value = "dfswriter")
public class DFSWriter implements Writer<Dataset<Row>>, Serializable {

    @Autowired
    private DFSConfig dfsConfig;

    @Autowired
    private WriterUtil writerUtil;

    @Override
    public void write(Dataset<Row> output) throws DataAngosException {

        switch (dfsConfig.getWriteFormat().toLowerCase()) {
            case "csv":
               writerUtil.executeCSVWriter(output);
                break;
            case "json":
                writerUtil.executeJSONWriter(output);
                break;
            case "parquet":
                writerUtil.executeParquetWriter(output);
                break;
            case "text":
              writerUtil.executeTextWriter(output);
                break;
            default:
                throw new DataAngosException(DataAngosErrorCode.UNKNOWN_WRITER_FILE_FORMAT);
        }


    }

}
