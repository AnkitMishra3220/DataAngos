package com.da.digital.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;
import java.util.List;

@Data
@ConfigurationProperties(prefix = "parsingConfig")
@Configuration
public class ParsingConfig implements Serializable {
    private String metadataFile;
    private List<String> primaryKeys;
    private Boolean enableDataContainer;
}
