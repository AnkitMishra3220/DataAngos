package com.da.digital.metadata;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataContainer implements Serializable {

    private String id;
    private String event;
    private String ingestionTimestamp;
    private LinkedHashMap<String, String> data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getIngestionTimestamp() {
        return ingestionTimestamp;
    }

    public void setIngestionTimestamp(String ingestionTimestamp) {
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String,String> data) {
        this.data = (LinkedHashMap<String, String>) data;
    }

    @Override
    public String toString() {
        return "{" +
                "id:'" + id + '\'' +
                ", event:'" + event + '\'' +
                ", ingestion_timestamp:'" + ingestionTimestamp + '\'' +
                ", data:" + data +
                '}';
    }
}
