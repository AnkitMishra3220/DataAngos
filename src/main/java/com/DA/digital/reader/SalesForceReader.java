package com.da.digital.reader;

import com.da.digital.conf.SalesForceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;

@Component(value = "salesforce")
public class SalesForceReader implements Reader<Dataset<Row>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SalesForceReader.class);

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private SalesForceConfig salesForceConfig;

    @Override
    public Dataset<Row> read() {

        Configuration conf = new Configuration();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, salesForceConfig.getJceksPath());

        String password = "";

        try {
            password = String.valueOf(conf.getPassword(salesForceConfig.getJceksAlias()));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

       return sparkSession.read().format(salesForceConfig.getFormat()).
                option("login", salesForceConfig.getLogin()).
                option("username", salesForceConfig.getUsername()).
                option("password", password).
                option("soql", salesForceConfig.getSoql()).
                option("version", salesForceConfig.getVersion()).
                load();
    }
}
