package com.da.digital.conf;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

@Data
@ConfigurationProperties(prefix = "kafkaConfig")
@Configuration
public class KafkaConfig implements Serializable {

    private String bootstrapServers;
    private String inTopic;
    private String successOutTopic;
    private String errorOutTopic;
    private String singleOutTopic;
    private String startingOffsets;
    private String successCheckpointLocation;
    private String errorCheckpointLocation;
    private String singleCheckpointLocation;
    private String outputMode;
}
