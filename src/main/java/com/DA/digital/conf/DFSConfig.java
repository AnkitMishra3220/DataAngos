package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "dfsConfig")
@Configuration
public class DFSConfig implements Serializable {
    private String inputFile;
    private String outputFile;
    private String successOutputFile;
    private String errorOutputFile;
    private String writeFormat;
    private String fieldDelim;
    private String successCheckpointLocation;
    private String errorCheckpointLocation;
    private String singleCheckpointLocation;
}
