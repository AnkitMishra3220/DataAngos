package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "fileConfig")
@Configuration
public class FileConfig implements Serializable {
    private String inputFile;
    private String outputFile;
    private String fileType;
    private String headerRequired;
}
