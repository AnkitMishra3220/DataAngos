package com.da.digital.udf;

import com.da.digital.parser.ParserUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.json.simple.JSONObject;
import java.io.Serializable;
import java.util.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicJSONParser implements UDF, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DynamicJSONParser.class);

    private SparkSession sparkSession;

    private List<String> primaryKeys;

    private Map<String, String> metadataMap;

    private ParserUtil parserUtil;

    private Boolean enableDataContainer;

    public DynamicJSONParser(SparkSession sparkSession, List<String> primaryKeys, Map<String, String> metadataMap,
                             ParserUtil parserUtil,boolean enableDataContainer) {
        this.sparkSession = sparkSession;
        this.primaryKeys = primaryKeys;
        this.metadataMap = metadataMap;
        this.parserUtil = parserUtil;
        this.enableDataContainer = enableDataContainer;
    }

    @Override
    public void callUDF() {

        sparkSession.udf().register("DynamicJSONParser", (String jsonString) -> {

            Map<String, String> outputMap = null;

            try {
                if (!jsonString.isEmpty()) {

                    JSONParser parser = new JSONParser();
                    Object jobj = parser.parse(jsonString);

                    JSONObject jsonObject = (JSONObject) jobj;
                    Map<String, String> vJson = parserUtil.validateJson(jsonObject,
                            primaryKeys);

                    if (vJson == null) {

                        /*  Json Flatten  */
                        Map<String, String> flattenedJsonMap = parserUtil.flattenJson(jsonObject);

                        /* Column-Value Mapping   */
                        outputMap =  parserUtil.executeMapping(flattenedJsonMap,metadataMap);
                    } else {

                        outputMap = vJson;

                    }

                } else {
                    outputMap = parserUtil.createError("payload is empty", jsonString);
                }

            } catch (ParseException e) {
                logger.error(e.getMessage());
                outputMap = parserUtil.createError("ParseException", jsonString);
            } catch (NullPointerException e) {
                logger.error(e.getMessage());
                outputMap = parserUtil.createError("NullPointerException", jsonString);
            }

            Map<String, String> finalMap = parserUtil.replaceNullValuesOfMap(outputMap, "");
            return parserUtil.createContainer(finalMap,enableDataContainer);


        }, DataTypes.StringType);

    }

}
