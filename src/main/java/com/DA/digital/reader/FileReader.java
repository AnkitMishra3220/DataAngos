package com.da.digital.reader;

import com.da.digital.conf.FileConfig;
import com.da.digital.exception.OctopusErrorCode;
import com.da.digital.exception.OctopusException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component(value = "file")
public class FileReader implements Reader<Dataset<Row>> {

    @Autowired
    private FileConfig fileConfig;

    @Autowired
    private SparkSession spark;

    @Override
    public Dataset<Row> read() throws OctopusException {

        switch (fileConfig.getFileType()) {
            case "csv":
                return spark.read().format(fileConfig.getFileType()).option("header", fileConfig.getHeaderRequired())
                                   .load(fileConfig.getInputFile());
            case "json":
                return spark.read().format(fileConfig.getFileType()).option("multiline", "true").load(fileConfig.getInputFile());
            case "parquet":
                return spark.read().format(fileConfig.getFileType()).load(fileConfig.getInputFile());
            case "avro":
                return spark.read().format(fileConfig.getFileType()).load(fileConfig.getInputFile());
            default:
                throw new OctopusException(OctopusErrorCode.UNKNOWN_FILE_TYPE);

        }


    }
}
