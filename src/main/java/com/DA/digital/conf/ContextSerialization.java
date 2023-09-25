package com.da.digital.conf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ContextSerialization {


    @Autowired
    SnowFlakeConfig snowFlakeConfig;


    public SnowFlakeConfigObj getSnowFlakeConfigObj()
    {
        SnowFlakeConfigObj snowFlakeConfigObj =  new SnowFlakeConfigObj();
        snowFlakeConfigObj.setConnectionURL(snowFlakeConfig.getConnectionURL());
        snowFlakeConfigObj.setDatabase(snowFlakeConfig.getDatabase());
        snowFlakeConfigObj.setOutTableName(snowFlakeConfig.getOutTableName());
        snowFlakeConfigObj.setInTableName(snowFlakeConfig.getInTableName());
        snowFlakeConfigObj.setJdbcDriver(snowFlakeConfig.getJdbcDriver());
        snowFlakeConfigObj.setPassword(snowFlakeConfig.getPassword());
        snowFlakeConfigObj.setPrimaryKey(snowFlakeConfig.getPrimaryKey());
        snowFlakeConfigObj.setRole(snowFlakeConfig.getRole());
        snowFlakeConfigObj.setSchema(snowFlakeConfig.getSchema());
        snowFlakeConfigObj.setUserName(snowFlakeConfig.getUserName());
        snowFlakeConfigObj.setWarehouse(snowFlakeConfig.getWarehouse());
        snowFlakeConfigObj.setIgnoreNull(snowFlakeConfig.getIgnoreNull());
        snowFlakeConfigObj.setSuccessCheckpointLocation(snowFlakeConfig.getSuccessCheckpointLocation());
        snowFlakeConfigObj.setErrorCheckPointLocation(snowFlakeConfig.getErrorCheckPointLocation());
        snowFlakeConfigObj.setErrorRecordsLocation(snowFlakeConfig.getErrorRecordsLocation());


        return snowFlakeConfigObj;
    }
}
