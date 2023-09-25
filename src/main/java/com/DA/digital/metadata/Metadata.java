package com.da.digital.metadata;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;
import java.util.LinkedHashMap;

@Data
@Slf4j
@ConfigurationProperties(prefix = "metadata")
@Configuration
public class Metadata implements Serializable {
    private LinkedHashMap<String, ColumnAttributes> columnIndexMap;
}
