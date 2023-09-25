package com.da.digital.writer;

import com.da.digital.conf.AppConfig;
import com.da.digital.conf.KuduConfig;
import com.da.digital.exception.OctopusException;
import org.apache.kudu.spark.kudu.KuduWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.Serializable;



@Component(value = "kuduwriter")
public class KuduWriter implements Writer<Dataset<Row>>, Serializable {

    @Autowired
    private KuduConfig kuduConfig;

    @Autowired
    private AppConfig appConfig;

    @Override
    public void write(Dataset<Row> output) throws OctopusException {

        KuduWriteOptions kuduWriteOption = new KuduWriteOptions(false, true);
        String kuduTableName = "impala::" + kuduConfig.getDatabaseName() + "." + kuduConfig.getOutTableName();

        if (!KuduUtil.existKuduTable(kuduConfig,kuduTableName)) {
            KuduUtil.createKuduTable(kuduConfig, kuduTableName,output.schema());
        }

        kuduConfig.kuduContext().upsertRows(output.toDF(), kuduTableName, kuduWriteOption);

    }
}
