package com.da.digital.writer;

import com.da.digital.conf.SnowFlakeConfigObj;
import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import com.da.digital.utils.DMLGenerator;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class SnowFlakeJDBCSinkWriter extends ForeachWriter<Row> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SnowFlakeJDBCSinkWriter.class);
    SnowFlakeConfigObj snowFlakeConfigObj;
    List<String> colList = new ArrayList<>();
    Connection connection;
    Statement statement;

    public SnowFlakeJDBCSinkWriter(SnowFlakeConfigObj snowFlakeConfigObj, List<String> colList) {
        this.snowFlakeConfigObj = snowFlakeConfigObj;
        this.colList = colList;
    }

    @Override
    public boolean open(long partitionId, long epochId) {

        try {

            String url = snowFlakeConfigObj.getConnectionURL() + "?role=" + snowFlakeConfigObj.getRole() + "&db=" + snowFlakeConfigObj.getDatabase() + "&warehouse=" + snowFlakeConfigObj.getWarehouse();

            Class.forName(snowFlakeConfigObj.getJdbcDriver()).newInstance();

            connection = DriverManager.getConnection(url, snowFlakeConfigObj.getUserName(), snowFlakeConfigObj.getPassword());
            statement = connection.createStatement();
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            throw new DataAngosException(DataAngosErrorCode.SNOWFLAKE_ILLEGAL_ACCESS_ERROR);
        } catch (InstantiationException e) {
            logger.error(e.getMessage());
            throw new DataAngosException(DataAngosErrorCode.SNOWFLAKE_JDBC_CLASS_INSTANTIATION_ERROR);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DataAngosException(DataAngosErrorCode.SNOWFLAKE_DATABASE_ACCESS_ERROR);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
            throw new DataAngosException(DataAngosErrorCode.SNOWFLAKE_JDBC_DRIVER_ERROR);
        }
        return true;
    }

    @Override
    public void process(Row value) {

        try {

            statement.executeUpdate(DMLGenerator.generateDMLStmt(value, snowFlakeConfigObj, colList));
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DataAngosException(DataAngosErrorCode.SNOWFLAKE_SQL_ERROR);
            }
    }

    @Override
    public void close(Throwable errorOrNull) {

        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DataAngosException(DataAngosErrorCode.SNOWFLAKE_DATABASE_ACCESS_ERROR);

        }
    }
}
