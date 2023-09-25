package com.da.digital.utils;

import com.da.digital.conf.SnowFlakeConfigObj;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

import com.da.digital.metadata.Constant;

@Component
public class DMLGenerator implements Serializable {


    public static String generateDMLStmt(Row value, SnowFlakeConfigObj snowFlakeConfigObj, List<String> colList) {

        return getMergeStatement(colList, value, snowFlakeConfigObj)
                .append(getUpdateStatement(colList, snowFlakeConfigObj))
                .append(getInsertStatement(colList)).toString();
    }

    public static StringBuilder getMergeStatement(List<String> colList, Row value, SnowFlakeConfigObj snowFlakeConfigObj) {

        int colSize = colList.size();
        int colCounter = 1;
        int primaryKeyCounter = 1;
        int primaryKeySize = snowFlakeConfigObj.getPrimaryKey().size();

        StringBuilder mergeStmtBuilder = new StringBuilder(" MERGE INTO " + snowFlakeConfigObj.getSchema() + "." + snowFlakeConfigObj.getOutTableName() + " A USING (SELECT ");

        for (String col : colList) {

            if (value.getAs(col) == null) {
                mergeStmtBuilder.append(value.getAs(col) + " AS ").append(col + " ");

            } else {
                mergeStmtBuilder.append(Constant.OPEN_SINGLE_QUOTE).append(value.getAs(col).toString())
                        .append(Constant.CLOSE_SINGLE_QUOTE).append(" AS ").append(col + " ");
            }

            if (colSize > colCounter) {

                mergeStmtBuilder.append(", ");
                colCounter++;
            }
        }
        mergeStmtBuilder.append(Constant.CLOSE_BRACKET).append(" B ON ");
        for (String key : snowFlakeConfigObj.getPrimaryKey()) {
            mergeStmtBuilder.append(" A." + key + "=" + " B." + key);
            if (primaryKeySize > primaryKeyCounter)
                mergeStmtBuilder.append(" AND ");
            primaryKeyCounter++;
        }
        return mergeStmtBuilder;
    }


    public static StringBuilder getInsertStatement(List<String> colList) {
        int counter = 1;
        int size = colList.size();

        StringBuilder insertStmtBuilder = new StringBuilder(" WHEN NOT MATCHED THEN INSERT VALUES ( ");

        for (String col : colList) {

            insertStmtBuilder.append(" B." + col);
            if (size > counter) {

                insertStmtBuilder.append(Constant.COMMA);
                counter++;
            }
        }
        insertStmtBuilder.append(Constant.CLOSE_BRACKET).append(";");
        return insertStmtBuilder;
    }


    public static StringBuilder getUpdateStatement(List<String> colList, SnowFlakeConfigObj snowFlakeConfigObj) {
        int numberOfCols = colList.size() - snowFlakeConfigObj.getPrimaryKey().size();

        int colCounter = 1;

        StringBuilder updateStmtBuilder = new StringBuilder(" WHEN MATCHED THEN UPDATE SET ");


        for (String col : colList) {
            if (!snowFlakeConfigObj.getPrimaryKey().contains(col)) {
                updateStmtBuilder.append(" A.")
                        .append(col)
                        .append("=").append(" COALESCE ").append(Constant.OPEN_BRACKET).append(" NULLIF ")
                        .append(Constant.OPEN_BRACKET).append("B.").append(col).append(Constant.COMMA)
                        .append(Constant.OPEN_SINGLE_QUOTE).append(Constant.CLOSE_SINGLE_QUOTE).append(Constant.CLOSE_BRACKET)
                        .append(Constant.COMMA)
                        .append(" A.").append(col).append(Constant.CLOSE_BRACKET);
                if (numberOfCols > colCounter) {
                    updateStmtBuilder.append(Constant.COMMA);
                    colCounter++;
                }
            }
        }


        return updateStmtBuilder;
    }
}
