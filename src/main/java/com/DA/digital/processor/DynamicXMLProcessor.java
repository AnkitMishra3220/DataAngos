package com.da.digital.processor;

import com.da.digital.exception.DataAngosException;
import com.da.digital.udf.DynamicXMLParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;


@Component(value = "dynamicxmlprocesssor")
public class DynamicXMLProcessor implements Processor<Dataset<Row>, Dataset<Row>> {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DynamicXMLParser dynamicXMLParser;



    @Override
    public Dataset<Row> process(Dataset<Row> input) throws DataAngosException {

        String castValueAsString = "CAST(value AS STRING) as value";
        String udfColName = "parsedXML";
        String udfName = "dynamicXMLParser";

        dynamicXMLParser.callUDF();

        if(input.isStreaming()){
            String colName = "value";
            return input.selectExpr("CAST(key AS STRING) as key", castValueAsString)
                    .withColumn(udfColName, callUDF(udfName, col(colName)))
                    .drop(colName).withColumnRenamed(udfColName, colName)
                    .selectExpr("key", castValueAsString);

        }else{
            String colName = input.columns()[0];
            return input.withColumn(udfColName, callUDF(udfName, col(colName)))
                    .drop(colName).withColumnRenamed(udfColName, colName)
                    .selectExpr("key", castValueAsString);
        }

    }
}
