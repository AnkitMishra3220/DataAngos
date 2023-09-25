package com.da.digital.conf;

import java.io.Serializable;
import java.util.ArrayList;

public class SnowFlakeConfigObj implements Serializable {

    private String connectionURL;
    private String userName;
    private String password;
    private String database;
    private String warehouse;
    private String role;
    private String schema;
    private String jdbcDriver;
    private String inTableName;
    private String outTableName;
    private ArrayList<String> primaryKey;
    private ArrayList<String> partitionColumn;
    private String ignoreNull;


    private String successCheckpointLocation;
    private String errorRecordsLocation;
    private String errorCheckPointLocation;

    public String getSuccessCheckpointLocation() {
        return successCheckpointLocation;
    }

    public void setSuccessCheckpointLocation(String successCheckpointLocation) {
        this.successCheckpointLocation = successCheckpointLocation;
    }

    public String getErrorRecordsLocation() {
        return errorRecordsLocation;
    }

    public void setErrorRecordsLocation(String errorRecordsLocation) {
        this.errorRecordsLocation = errorRecordsLocation;
    }

    public String getErrorCheckPointLocation() {
        return errorCheckPointLocation;
    }

    public void setErrorCheckPointLocation(String errorCheckPointLocation) {
        this.errorCheckPointLocation = errorCheckPointLocation;
    }

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getInTableName() {
        return inTableName;
    }

    public void setInTableName(String inTableName) {
        this.inTableName = inTableName;
    }

    public String getOutTableName() {
        return outTableName;
    }

    public void setOutTableName(String outTableName) {
        this.outTableName = outTableName;
    }

    public ArrayList<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(ArrayList<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public ArrayList<String> getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(ArrayList<String> partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public String getIgnoreNull() {
        return ignoreNull;
    }

    public void setIgnoreNull(String ignoreNull) {
        this.ignoreNull = ignoreNull;
    }



}
