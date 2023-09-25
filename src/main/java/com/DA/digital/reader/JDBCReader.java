package com.da.digital.reader;

import com.da.digital.conf.InputJDBCConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.IOException;
import java.io.Serializable;

@Component(value = "jdbc")
public class JDBCReader implements Reader<Dataset<Row>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(JDBCReader.class);

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private InputJDBCConfig inputJdbcConfig;

    @Override
    public Dataset<Row> read() {


        Configuration conf = new Configuration();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, inputJdbcConfig.getInJceksPath());

        String password = "";

        try {
            password = String.valueOf(conf.getPassword(inputJdbcConfig.getInJceksAlias()));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }


        return sparkSession.read().format(inputJdbcConfig.getJdbcFormat())
                .option("url", inputJdbcConfig.getInUrl())
                .option("driver", inputJdbcConfig.getInDriverName())
                .option("user", inputJdbcConfig.getInUserName())
                .option("password", password)
                .option("dbtable", inputJdbcConfig.getInTableName())
                .option("numPartitions", inputJdbcConfig.getInNumConnection()).load();

    }
}
