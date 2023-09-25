package com.da.digital.conf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KuduConfigObj implements Serializable {

    private String databaseName;
    private String inTableName;

    private String outTableName;
    private String masterAddress;
    private ArrayList<String> primaryKey;
    private String partitionNum;
    private String numReplicas;
    private String ignoreNull;
    private String checkpointLocation;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
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

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = (ArrayList<String>) primaryKey;
    }

    public String getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(String partitionNum) {
        this.partitionNum = partitionNum;
    }

    public String getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(String numReplicas) {
        this.numReplicas = numReplicas;
    }

    public String getIgnoreNull() {
        return ignoreNull;
    }

    public void setIgnoreNull(String ignoreNull) {
        this.ignoreNull = ignoreNull;
    }

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public void setCheckpointLocation(String checkpointLocation) {
        this.checkpointLocation = checkpointLocation;
    }

}
