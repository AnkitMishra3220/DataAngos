package com.da.digital.reader;

import com.da.digital.conf.KuduConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Component(value = "kudu")
public class KuduReader implements Reader<Dataset<Row>>, Serializable {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private KuduConfig kuduConfig;

    @Override
    public Dataset<Row> read() {

        String kuduTableName = "impala::" + kuduConfig.getDatabaseName() + "." + kuduConfig.getInTableName();

        Map<String,String> kuduWriterMap = new HashMap<>();
        kuduWriterMap.put("kudu.master",kuduConfig.getMasterAddress());
        kuduWriterMap.put("kudu.table",kuduTableName);
        kuduWriterMap.put("kudu.socketReadTimeoutMs","720000");

        return sparkSession.read().options(kuduWriterMap).format("org.apache.kudu.spark.kudu").load();
    }
}