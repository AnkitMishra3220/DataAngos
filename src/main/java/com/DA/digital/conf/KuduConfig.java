package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@Slf4j
@ConfigurationProperties(prefix = "kuduConfig")
@Configuration
public class KuduConfig implements Serializable {

    @Autowired
    private SparkSession sparkSession;

    @Value("${writerType}")
    private String writerType;

    private String databaseName;
    private String inTableName;
    private String outTableName;
    private String masterAddress;
    private ArrayList<String> primaryKey;
    private String partitionNum;
    private ArrayList<String> partitionColumn;
    private String numReplicas;
    private String ignoreNull;
    private String checkpointLocation;

    @Bean
    public KuduContext kuduContext(){

        if(writerType.equalsIgnoreCase("kudu")){
            return new KuduContext(masterAddress,sparkSession.sparkContext());
        }else
        {
            return null;
        }
    }
}
