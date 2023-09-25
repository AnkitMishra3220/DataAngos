package com.da.digital.reader;

import com.da.digital.metadata.Metadata;
import com.da.digital.conf.DFSConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@Component(value = "dfs")
public class DFSReader implements Reader<Dataset<Row>>, Serializable {


    @Autowired
    private DFSConfig dfsConfig;

    @Autowired
    private Metadata metadata;

    @Autowired
    private SparkSession spark;


    @Override
    public Dataset<Row> read() {

        String fieldDelim = dfsConfig.getFieldDelim();

        JavaRDD<Row> javaRowRDD = spark.sparkContext()
                .textFile(dfsConfig.getInputFile(), 1)
                .toJavaRDD()
                .map(line -> RowFactory.create(line.split(fieldDelim)));

        List<StructField> fields = new ArrayList<>();

        int index = 0;

        while (index < metadata.getColumnIndexMap().size()) {
            StructField field = DataTypes
                    .createStructField(metadata.getColumnIndexMap().get(Integer.toString(index)).getName(),
                            DataTypes.StringType, true);
            fields.add(field);
            index++;
        }

        StructType schema = DataTypes.createStructType(fields);

        return spark.createDataFrame(javaRowRDD, schema);
    }
}
