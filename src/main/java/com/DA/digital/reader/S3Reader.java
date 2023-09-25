package com.da.digital.reader;

import com.da.digital.conf.S3Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component(value = "s3")
public class S3Reader implements Reader<Dataset<Row>>, Serializable {


    @Autowired
    private SparkSession spark;

    @Autowired
    private S3Config s3Config;


    @Override
    public Dataset<Row> read() {

        Dataset<Row> input = spark.read().format(s3Config.getReaderFormat())
                .option("mergeSchema", "false")
                .load(s3Config.getInputFile());

        input.sparkSession().sparkContext().hadoopConfiguration()
                .set(s3Config.getInAccesskeyProperty(), s3Config.getInSecretKeyValue());
        input.sparkSession().sparkContext().hadoopConfiguration()
                .set(s3Config.getInSecretKeyProperty(), s3Config.getInSecretKeyValue());
        input.sparkSession().sparkContext().hadoopConfiguration()
                .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        input.sparkSession().sparkContext().hadoopConfiguration()
                .set("fs.s3a.experimental.fadvise", "random");

        input.sparkSession().sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3-us-east-1.amazonaws.com");

        return input;
    }
}
