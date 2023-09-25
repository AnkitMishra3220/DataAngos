package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "outJDBCConfig")
@Configuration
public class OutputJDBCConfig implements Serializable {

    private String outUrl;
    private String outTableName;
    private String outUserName;
    private String outJceksPath;
    private String outJceksAlias;
    private String outNumConnection;
    private String outDriverName;
    private String outSaveMode;
}
