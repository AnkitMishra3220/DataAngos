package com.da.digital.conf;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "salesForceConfig")
@Configuration
public class SalesForceConfig implements Serializable {

    private String login;
    private String format;
    private String username;
    private String soql;
    private String version;
    private String jceksPath;
    private String jceksAlias;

}
