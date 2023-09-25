package com.da.digital.conf;

import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@Configuration
public class SnowFlakeDataSourceConfig {

    @Autowired
    SnowFlakeConfig snowFlakeConfig;

    public SnowflakeBasicDataSource getSnowDataSource() throws IOException, SQLException {

        SnowflakeBasicDataSource snowflakeBasicDataSource =  new SnowflakeBasicDataSource();
        String snowFlakeJDBCUrl = snowFlakeConfig.getConnectionURL();
        snowflakeBasicDataSource.setUrl(snowFlakeJDBCUrl);
        snowflakeBasicDataSource.setUser(snowFlakeConfig.getUserName());
        snowflakeBasicDataSource.setPassword(snowFlakeConfig.getPassword());
        snowflakeBasicDataSource.setDatabaseName(snowFlakeConfig.getDatabase());
        snowflakeBasicDataSource.setWarehouse(snowFlakeConfig.getWarehouse());
        snowflakeBasicDataSource.setRole(snowFlakeConfig.getRole());
        snowflakeBasicDataSource.setSchema(snowFlakeConfig.getSchema());
        Connection con = snowflakeBasicDataSource.getConnection();

        return snowflakeBasicDataSource;
    }

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE,proxyMode = ScopedProxyMode.TARGET_CLASS)
    public JdbcTemplate getJDBCTemplate() throws IOException, SQLException {
        return new JdbcTemplate(getSnowDataSource());
    }



}
