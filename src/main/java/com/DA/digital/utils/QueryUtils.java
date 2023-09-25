package com.da.digital.utils;

import com.da.digital.conf.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import com.da.digital.metadata.Constant;

@Component
public class QueryUtils implements Serializable {


    @Autowired
    SnowFlakeConfig snowFlakeConfig;

    @Scope("prototype")
    @Bean
    public SnowColumnsList getColumnList(JdbcTemplate jdbcTemplate, SnowFlakeConfig snowFlakeConfig) {
        String extractColumnsQuery = new StringBuilder("SELECT COLUMN_NAME from information_schema.columns WHERE table_schema = ")
                .append(Constant.OPEN_SINGLE_QUOTE).append(snowFlakeConfig.getSchema().toUpperCase()).append(Constant.CLOSE_SINGLE_QUOTE)
                .append(" AND TABLE_NAME=").append(Constant.OPEN_SINGLE_QUOTE).append(snowFlakeConfig.getOutTableName().toUpperCase())
                .append(Constant.CLOSE_SINGLE_QUOTE).append(" ORDER BY ORDINAL_POSITION ASC ;").toString();

        SnowColumnsList snowColumnsList = new SnowColumnsList();

        snowColumnsList.setColList(jdbcTemplate.queryForList(extractColumnsQuery, String.class));

        return snowColumnsList;
    }

}
