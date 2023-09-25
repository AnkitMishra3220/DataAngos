package com.da.digital.writer;

import com.da.digital.conf.S3Config;
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

@Component(value = "s3writer")
public class S3Writer implements Writer<Dataset<Row>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(S3Writer.class);

    @Autowired
    private S3Config s3Config;

    @Autowired
    SparkSession spark;

    @Override
    public void write(Dataset<Row> output) {

        Configuration conf = new Configuration();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, s3Config.getOutSecretKeyPath());

        String secretKeyValue = "";

        try {
            secretKeyValue = String.valueOf(conf.getPassword(s3Config.getOutJceksAlias()));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        output.sparkSession().sparkContext().hadoopConfiguration().set(s3Config.getOutFileSystemProperty(), s3Config.getOutFileSystemValue());
        output.sparkSession().sparkContext().hadoopConfiguration().set(s3Config.getOutAccesskeyProperty(), s3Config.getOutAccesskeyValue());
        output.sparkSession().sparkContext().hadoopConfiguration().set(s3Config.getOutSecretKeyProperty(), secretKeyValue);
        if (s3Config.getWriteFormat().equals("parquet")) {
            output.repartition(Integer.parseInt(s3Config.getRepartition())).
                    write().mode(s3Config.getSaveMode()).parquet(s3Config.getOutputFile());
        } else {
            output.write().format(s3Config.getWriteFormat()).option(s3Config.getDelimterKey(), s3Config.getOutDelim()).save(s3Config.getOutputFile());
        }
    }

}
