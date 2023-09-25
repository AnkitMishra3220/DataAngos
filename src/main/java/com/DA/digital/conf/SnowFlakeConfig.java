package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@Slf4j
@ConfigurationProperties(prefix = "snowflakeConfig")
@Configuration
public class SnowFlakeConfig implements Serializable {

    private String connectionURL;
    private String userName;
    private String password;
    private String database;
    private String warehouse;
    private String role;
    private String schema;
    private String jdbcDriver;
    private String inTableName;
    private String outTableName;
    private ArrayList<String> primaryKey;
    private ArrayList<String> partitionColumn;
    private String ignoreNull;
    private String successCheckpointLocation;
    private String errorRecordsLocation;
    private String errorCheckPointLocation;

}
