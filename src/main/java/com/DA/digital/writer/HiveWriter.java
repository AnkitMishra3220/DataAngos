package com.da.digital.writer;

import com.da.digital.conf.HiveConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.Serializable;

@Component(value = "hivewriter")
public class HiveWriter implements Writer<Dataset<Row>>, Serializable {

    @Autowired
    HiveConfig hiveConfig;

    @Autowired
    SparkSession spark;


    @Override
    public void write(Dataset<Row> output) {

        spark.sql("CREATE DATABASE IF NOT EXISTS "+ hiveConfig.getDatabaseName());

        String hiveTableName = hiveConfig.getDatabaseName() + "." + hiveConfig.getOutTableName();

        if (hiveConfig.getPartitionColumn().equals("")) {
            spark.sqlContext().setConf("hive.exec.dynamic.partition", "true");
            spark.sqlContext().setConf("hive.exec.dynamic.partition.mode", "nonstrict");
        }

        switch (hiveConfig.getSaveMode().toLowerCase()) {

            case "overwrite":
                if (!hiveConfig.getPartitionColumn().equals("")) {
                    output.write().partitionBy(hiveConfig.getPartitionColumn()).mode(SaveMode.Overwrite).saveAsTable(hiveTableName);
                } else {
                    output.write().mode(SaveMode.Overwrite).saveAsTable(hiveTableName);
                }
                break;
            case "append":
                if (!hiveConfig.getPartitionColumn().equals("")) {
                    output.write().partitionBy(hiveConfig.getPartitionColumn()).mode(SaveMode.Append).saveAsTable(hiveTableName);
                } else {
                    output.write().mode(SaveMode.Append).saveAsTable(hiveTableName);
                }
                break;
            default:
                output.write().saveAsTable(hiveTableName);

        }
    }
}
