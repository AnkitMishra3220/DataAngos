package com.da.digital.processor;

import com.da.digital.conf.ContextSerialization;
import com.da.digital.conf.SnowColumnsList;
import com.da.digital.conf.SnowFlakeConfig;
import com.da.digital.exception.OctopusException;
import com.da.digital.utils.QueryUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import java.util.List;
import com.da.digital.metadata.Constant;


@Component(value = "jsontodfstreamingprocessor")

public class JSONToDFStreamingProcessor implements Processor<Dataset<Row>, Dataset<Row>>, Serializable {

    @Autowired
    QueryUtils queryUtils;

    @Autowired
    SnowFlakeConfig snowFlakeConfig;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private ContextSerialization contextSerialization;

    @Autowired
    private SnowColumnsList snowColumnsList;


    @Override
    public Dataset<Row> process(Dataset<Row> inputDS) throws OctopusException {
        return generateFlattenedDF(inputDS.selectExpr("CAST (value as STRING) AS value"));
    }


    public Dataset<Row> generateFlattenedDF(Dataset<Row> inputDS) throws OctopusException {

        inputDS.createOrReplaceTempView(snowFlakeConfig.getOutTableName());
        List<String> snowFlakeColList = snowColumnsList.getColList();

        StringBuilder flattenDFBuilder = new StringBuilder("SELECT ");
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

        Dataset<Row> outputDF = sparkSession.sql(flattenDFBuilder.toString());

        //register outputDF
        // datamap--> flat json
        // data.map--> sql-->dataframe

        return outputDF;
    }
}
