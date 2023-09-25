package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "hiveConfig")
@Configuration
public class HiveConfig implements Serializable {
    private String sourceReaderSQL;
    private String databaseName;
    private String outTableName;
    private String partitionColumn;
    private String saveMode;
}
