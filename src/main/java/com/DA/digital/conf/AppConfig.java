package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "appConfig")
@Configuration
public class AppConfig implements Serializable {
    private String appName;
    private String executionMode;
    private String keytabUser;
    private String keytabLocation;
    private String jdbcDriver;
    private String connectionURL;
    private String metadataURL;
    private String dataSource;
}
