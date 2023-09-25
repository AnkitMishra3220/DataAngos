package com.da.digital.writer;

import com.da.digital.conf.OutputJDBCConfig;
import com.da.digital.exception.OctopusException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;

@Component(value = "jdbcwriter")
public class JDBCWritter implements Writer<Dataset<Row>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(JDBCWritter.class);

    @Autowired
    private OutputJDBCConfig outputJDBCConfig;


    @Override
    public void write(Dataset<Row> output) throws OctopusException {

        Configuration conf = new Configuration();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, outputJDBCConfig.getOutJceksPath());

        String password = "";

        try {
            password = String.valueOf(conf.getPassword(outputJDBCConfig.getOutJceksAlias()));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        output.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", outputJDBCConfig.getOutDriverName())
                .option("url", outputJDBCConfig.getOutUrl())
                .option("dbtable", outputJDBCConfig.getOutTableName())
                .option("user", outputJDBCConfig.getOutUserName())
                .option("password", password)
                .option("numPartitions", outputJDBCConfig.getOutNumConnection())
                .save();


    }
}
