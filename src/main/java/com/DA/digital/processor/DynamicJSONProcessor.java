package com.da.digital.processor;

import com.da.digital.conf.ParsingConfig;
import com.da.digital.exception.DataAngosException;
import com.da.digital.parser.ParserUtil;
import com.da.digital.udf.DynamicJSONParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Component(value = "dynamicjsonprocessor")
public class DynamicJSONProcessor implements Processor<Dataset<Row>, Dataset<Row>> {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private ParsingConfig parsingConfig;

    @Value("${sourceRecordsContainsExpn}")
    private String sourceRecordsContainsExpn;

    @Override
    public Dataset<Row> process(Dataset<Row> input) throws DataAngosException {

        List<String> primaryKeys = parsingConfig.getPrimaryKeys();

        ParserUtil parserUtil = new ParserUtil();

        String castValueAsString = "CAST(value AS STRING) as value";
        String udfColName = "parsedJSON";
        String udfName = "dynamicJSONParser";

        Map<String, String> metadataMap = parserUtil.mappingJson(parsingConfig.getMetadataFile());

        DynamicJSONParser dynamicJSONParser = new DynamicJSONParser(sparkSession, primaryKeys,
                metadataMap, parserUtil, parsingConfig.getEnableDataContainer());

        dynamicJSONParser.callUDF();

        if (input.isStreaming()) {
            String colName = "value";
            return input.selectExpr("CAST(key AS STRING) as key", castValueAsString)
                    .filter((col(colName).contains(sourceRecordsContainsExpn)))
                    .withColumn(udfColName, callUDF(udfName, col(colName)))
                    .drop(colName).withColumnRenamed(udfColName, colName)
                    .filter("value is not null").selectExpr("key", castValueAsString);
        } else {
            String colName = input.columns()[0];
            return input.filter((col(colName).contains(sourceRecordsContainsExpn)))
                    .withColumn(udfColName, callUDF(udfName, col(colName)))
                    .drop(colName).withColumnRenamed(udfColName, colName)
                    .selectExpr(castValueAsString);
        }
    }
}
