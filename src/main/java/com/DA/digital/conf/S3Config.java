package com.da.digital.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

@Data
@Slf4j
@ConfigurationProperties(prefix = "s3Config")
@Configuration
public class S3Config implements Serializable {

    private String outAccesskeyProperty;
    private String outAccesskeyValue;
    private String outSecretKeyProperty;
    private String outSecretKeyPath;
    private String outFileSystemProperty;
    private String outFileSystemValue;
    private String outEncryptionAlgorithmProperty;
    private String outEncryptionAlgorithmValue;
    private String outEncryptionKeyProperty;
    private String outEncryptionKeyValue;
    private String writeFormat;
    private String outDelim;
    private String outputFile;
    private String delimterKey = "delimter";
    private String outJceksAlias;
    private String repartition;
    private String saveMode;
    private String inAccesskeyProperty;
    private String inAccesskeyValue;
    private String inSecretKeyProperty;
    private String inSecretKeyValue;
    private String inputFile;
    private String readerFormat;
}
