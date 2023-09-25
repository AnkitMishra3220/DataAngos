package com.da.digital.reader;

import com.da.digital.conf.HiveConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.Serializable;

@Component(value = "hive")
public class HiveReader implements Reader<Dataset<Row>>, Serializable {

    @Autowired
    private SparkSession spark;

    @Autowired
    private HiveConfig hiveConfig;


    @Override
    public Dataset<Row> read() {

        return spark.sql(hiveConfig.getSourceReaderSQL());
    }
}
