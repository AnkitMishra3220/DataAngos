package com.da.digital.conf;

import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class JobContext {

    @Autowired
    AppConfig appConfig;

    @Autowired
    SnowFlakeConfig snowFlakeConfig;

    @Bean
    public SparkSession sparkSession() {

        SparkSession spark = SparkSession.builder().appName(appConfig.getAppName())
                                                   .master(appConfig.getExecutionMode())
                                                   .enableHiveSupport().getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        return spark;
    }

    public JdbcTemplate jdbcTemplate() {

        SnowflakeBasicDataSource snowflakeBasicDataSource =  new SnowflakeBasicDataSource();
        String URL = "jdbc:snowflake://" + snowFlakeConfig.getConnectionURL();
        snowflakeBasicDataSource.setUrl(URL);
        snowflakeBasicDataSource.setUser(snowFlakeConfig.getUserName());
        snowflakeBasicDataSource.setPassword(snowFlakeConfig.getPassword());
        snowflakeBasicDataSource.setDatabaseName(snowFlakeConfig.getDatabase());
        snowflakeBasicDataSource.setWarehouse(snowFlakeConfig.getWarehouse());
        snowflakeBasicDataSource.setRole(snowFlakeConfig.getRole());
        snowflakeBasicDataSource.setSchema(snowFlakeConfig.getSchema());

        return new JdbcTemplate(snowflakeBasicDataSource);


    }
}
