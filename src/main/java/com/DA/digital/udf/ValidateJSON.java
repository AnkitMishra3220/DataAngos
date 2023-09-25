package com.da.digital.udf;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.da.digital.metadata.Constant;
import com.da.digital.parser.ParserUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ValidateJSON implements UDF, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ValidateJSON.class);

    private SparkSession sparkSession;

    private List<String> primaryKeys;

    private ParserUtil parserUtil;

    private Boolean enableDataContainer;

    public ValidateJSON(SparkSession sparkSession, List<String> primaryKeys,
                             ParserUtil parserUtil, boolean enableDataContainer) {
        this.sparkSession = sparkSession;
        this.primaryKeys = primaryKeys;
        this.parserUtil = parserUtil;
        this.enableDataContainer = enableDataContainer;
    }

    @Override
    public void callUDF() {

        sparkSession.udf().register("validateJSON", (String jsonString) -> {

            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            Map<String, String> outputMap = null;
            Object jobj=null ;
            if (!jsonString.isEmpty()) {

                JSONParser parser = new JSONParser();

                try {
                    jobj = parser.parse(jsonString);

                JSONObject jsonObject = (JSONObject) jobj;
                Map<String, String> vJson = parserUtil.validateJson(jsonObject,
                        primaryKeys);

                if (vJson == null) {
                    return jsonString;
                } else {
                    return parserUtil.createContainer(vJson, enableDataContainer);

                }
            }
                catch (ParseException e) {
                    logger.error(e.getMessage());
                 return parserUtil.createContainer(parserUtil.createError("ParseException", jsonString), enableDataContainer);

                } catch (NullPointerException e) {
                    logger.error(e.getMessage());
                    return parserUtil.createContainer(parserUtil.createError("NullPointerException", jsonString),enableDataContainer );
                }
            }
            else
            {
                return parserUtil.createContainer(parserUtil.createError("JSON is Empty", jsonString),enableDataContainer);
            }
            }, DataTypes.StringType);
    }

}