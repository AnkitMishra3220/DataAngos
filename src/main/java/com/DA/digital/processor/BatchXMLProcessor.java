package com.da.digital.processor;

import com.da.digital.conf.KuduConfig;
import com.da.digital.exception.OctopusException;
import com.da.digital.metadata.Metadata;
import com.da.digital.metadata.MetadataObj;
import com.da.digital.parser.ParserFunction;
import com.da.digital.metadata.ColumnAttributes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.*;

@Component(value = "batchxmlprocessor")
public class BatchXMLProcessor implements Processor<Dataset<Row>, Dataset<Row>> {

    @Autowired
    private Metadata autoWiredMetadata;

    @Autowired
    private SparkSession sparkSession;

    @Value("${transformationTempTable}")
    private String transformationTempTable;

    @Value("${transformationSQL}")
    private String transformationSQL;

    @Value("${writerType}")
    private String writerType;

    @Value("${errorFile}")
    private String errorFile;

    @Autowired
    private ProcessorUtil processorUtil;

    @Autowired
    private KuduConfig kuduConfig;


    @Override
    public Dataset<Row> process(Dataset<Row> inputDS) throws OctopusException {

        List<String> fields = new ArrayList<>();

        for (Map.Entry<String, ColumnAttributes> entry : autoWiredMetadata.getColumnIndexMap().entrySet()) {
            if (entry.getValue().getParser().equalsIgnoreCase("xml")) {
                entry.getValue().getXpaths().forEach((k, v) -> fields.add(k));
                if ((entry.getValue().getAdditionalNSFieldName() != null)) {
                    fields.add(entry.getValue().getAdditionalNSFieldName());
                }
            } else {
                fields.add(entry.getValue().getName());
            }
        }

        List<StructField> structFields = new ArrayList<>();

        Iterator<String> fieldIterator = fields.iterator();

        while (fieldIterator.hasNext()) {

            String colName = fieldIterator.next();

            if (writerType.equalsIgnoreCase("kudu") && kuduConfig.getPrimaryKey().contains(colName)) {

                structFields.add(DataTypes.createStructField(colName, DataTypes.StringType, false));

            } else {
                structFields.add(DataTypes.createStructField(colName, DataTypes.StringType, true));

            }

        }

        StructType schema = DataTypes.createStructType(structFields);

        MetadataObj metadataObj = new MetadataObj();

        metadataObj.setColumnIndexMap(autoWiredMetadata.getColumnIndexMap());

        ParserFunction parserFunction = new ParserFunction(metadataObj);

        JavaRDD<Tuple2<Row, String>> resultRDD = inputDS.toJavaRDD().map(parserFunction::parsingFunction);


        Dataset<Row> interimResultDF = sparkSession.createDataFrame((resultRDD.map(x -> x._1()).filter(Objects::nonNull)
                .rdd()), schema);

        Dataset<Row> resultDF = processorUtil.enableLoadDate(interimResultDF);

        if (transformationSQL.equals("")) {

            return resultDF;

        } else {

            resultDF.createOrReplaceTempView(transformationTempTable);
            return sparkSession.sql(transformationSQL);
        }

    }

}

