package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Slf4j
@ConfigurationProperties(prefix = "inJDBCConfig")
@Configuration
public class InputJDBCConfig {

    private String inDriverName;
    private String inUrl;
    private String inTableName;
    private String inUserName;
    private String inJceksPath;
    private String inJceksAlias;
    private String inNumConnection;
    private String sourceReaderSQL;
    private String jdbcFormat;

}
