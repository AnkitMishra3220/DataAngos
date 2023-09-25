package com.da.digital.processor;

import com.da.digital.conf.*;
import com.da.digital.exception.OctopusException;
import com.da.digital.metadata.Constant;
import com.da.digital.parser.ParserUtil;
import com.da.digital.udf.DynamicJSONParser;
import com.da.digital.udf.ValidateJSON;
import com.da.digital.utils.QueryUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;


@Component(value = "defaultprocessor")
public class DefaultProcessor implements Processor<Dataset<Row>, Dataset<Row>> {

    @Value("${transformationTempTable}")
    private String transformationTempTable;

    @Value("${transformationSQL}")
    private String transformationSQL;

    @Value("${enableJsonToDF}")
    private Boolean enableJsonToDF;

    @Value("${enableJsonParser}")
    private Boolean enableJsonParser;

    @Autowired
    private KuduConfig kuduConfig;

    @Value("${writerType}")
    private String writerType;

    @Value("${sourceRecordsContainsExpn}")
    private String sourceRecordsContainsExpn;

    @Value("${enableErrorRoute}")
    private Boolean enableErrorRoute;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private ProcessorUtil processorUtil;

    @Autowired
    SnowFlakeConfig snowFlakeConfig;

    @Autowired
    private ContextSerialization contextSerialization;

    @Autowired
    private SnowColumnsList snowColumnsList;

    @Autowired
    QueryUtils queryUtils;

    @Autowired
    ParsingConfig parsingConfig;


    @Override
    public Dataset<Row> process(Dataset<Row> inputDS) throws OctopusException {

        if (!inputDS.isStreaming()) {
            JavaRDD<Row> inputRDD = inputDS.toJavaRDD();

            List<StructField> fields = new ArrayList<>(Arrays.asList(inputDS.schema().fields()));

            for (int i = 0; i < fields.size(); i++) {

                StructField field = fields.get(i);

                if (writerType.equalsIgnoreCase("kudu") && kuduConfig.getPrimaryKey().contains(field.name())) {

                    StructField nullableField = new StructField(field.name(), field.dataType(), false, Metadata.empty());

                    fields.remove(field);

                    fields.add(i, nullableField);

                    StructType schema = DataTypes.createStructType(fields);

                    inputDS = sparkSession.createDataFrame(inputRDD, schema);
                }
            }


        } else {

            ParserUtil parserUtil = new ParserUtil();

            if (enableJsonParser && enableJsonToDF) {
                inputDS = processorUtil.getFlattenedJSON(inputDS, parsingConfig, parserUtil);

                inputDS = processorUtil.extractJSONData(inputDS);

            }
            if (!enableJsonParser && enableJsonToDF) {
                List<String> primaryKeys = parsingConfig.getPrimaryKeys();
                ValidateJSON validateJSON = new ValidateJSON(sparkSession, primaryKeys, parserUtil, parsingConfig.getEnableDataContainer());
                String udfColName = "vJSON";
                String udfName = "validateJSON";
                String colName = "value";

                validateJSON.callUDF();

                inputDS = inputDS.selectExpr("CAST(value AS STRING) as value").withColumn(udfColName, callUDF(udfName, col(colName)))
                        .drop(colName).withColumnRenamed(udfColName, colName);

                inputDS = processorUtil.extractJSONData(inputDS);
            }

        }

        if (transformationSQL.equals("")) {
            return inputDS;
        } else {

            inputDS.createOrReplaceTempView(transformationTempTable);
            Dataset<Row> interimResultDF = inputDS;
            if(!enableErrorRoute) {
                interimResultDF = sparkSession.sql(transformationSQL);
            }
            else {
                String regex = "(?i)from";

                String sqlParts[] = transformationSQL.split(regex);

                String selectQuery = sqlParts[0];

                String afterFromStmt = " , bdp_parser_error_switch, ts, payload, error FROM " + transformationTempTable;

                 interimResultDF = sparkSession.sql(selectQuery.concat(afterFromStmt));
            }

            return processorUtil.enableLoadDate(interimResultDF);
        }

    }
}

