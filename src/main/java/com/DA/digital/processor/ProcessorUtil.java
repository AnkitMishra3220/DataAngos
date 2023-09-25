package com.da.digital.processor;

import com.da.digital.conf.ParsingConfig;
import com.da.digital.conf.SnowColumnsList;
import com.da.digital.conf.SnowFlakeConfig;
import com.da.digital.metadata.Constant;
import com.da.digital.parser.ParserUtil;
import com.da.digital.udf.DynamicJSONParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

@Component
public class ProcessorUtil {

    @Value("${enableLoadDate}")
    private String enableLoadDate;

    @Value("${sourceRecordsContainsExpn}")
    private String sourceRecordsContainsExpn;

    @Autowired
   private SnowFlakeConfig snowFlakeConfig;

    @Autowired
    private SnowColumnsList snowColumnsList;

    @Autowired
    private SparkSession sparkSession;

    public Dataset<Row> enableLoadDate(Dataset<Row> input) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date();

        if (enableLoadDate.equalsIgnoreCase("true")) {
            return input.withColumn("load_date", lit(dateFormat.format(date)));
        } else {
            return input;
        }
    }


    public Dataset<Row> extractJSONData(Dataset<Row> inputDS) {

        inputDS.selectExpr("CAST (value as STRING) AS value").createOrReplaceTempView(snowFlakeConfig.getOutTableName());
        List<String> snowFlakeColList = snowColumnsList.getColList();

        StringBuilder flattenDFBuilder = new StringBuilder("SELECT ");

        snowFlakeColList.add(Constant.BDP_PARSER_ERROR_SWITCH);
        snowFlakeColList.add("ts");
        snowFlakeColList.add("payload");
        snowFlakeColList.add(Constant.ERROR);

        int numberOfCols = snowFlakeColList.size();

        int counter = 1;
        for (String col : snowFlakeColList) {
            flattenDFBuilder.append("get_json_object")
                    .append(Constant.OPEN_BRACKET)
                    .append(" value ").append(Constant.COMMA).append(Constant.OPEN_SINGLE_QUOTE)
                    .append("$.").append(col.toLowerCase()).append(Constant.CLOSE_SINGLE_QUOTE).append(Constant.CLOSE_BRACKET)
                    .append(" AS ").append(col);

            if (numberOfCols > counter) {
                flattenDFBuilder.append(Constant.COMMA);
                counter++;
            }
        }
        flattenDFBuilder.append(" FROM ").append(snowFlakeConfig.getOutTableName());

        return sparkSession.sql(flattenDFBuilder.toString());
    }


    public Dataset<Row> getFlattenedJSON(Dataset<Row> inputDS, ParsingConfig parsingConfig, ParserUtil parserUtil) {
        List<String> primaryKeys = parsingConfig.getPrimaryKeys();
        String castValueAsString = "CAST(value AS STRING) as value";
        String udfColName = "parsedJSON";
        String udfName = "dynamicJSONParser";

        Map<String, String> metadataMap = parserUtil.mappingJson(parsingConfig.getMetadataFile());

        DynamicJSONParser dynamicJSONParser = new DynamicJSONParser(sparkSession, primaryKeys,
                metadataMap, parserUtil, parsingConfig.getEnableDataContainer());

        dynamicJSONParser.callUDF();
        String colName = "value";

        return inputDS.selectExpr("CAST(key AS STRING) as key", castValueAsString)
                .filter((col(colName).contains(sourceRecordsContainsExpn)))
                .withColumn(udfColName, callUDF(udfName, col(colName)))
                .drop(colName).withColumnRenamed(udfColName, colName)
                .filter("value is not null").selectExpr(castValueAsString);

    }

}
