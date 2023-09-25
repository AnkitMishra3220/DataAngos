package com.da.digital.exception;


import java.sql.SQLException;

public enum OctopusErrorCode {

    UNKNOWN_FILE_TYPE(4001, "Unknown File Type in Application YML"),
    UNKNOWN_PROCESSOR_TYPE(4002, "Unknown Processor Type in Application YML"),
    UNKNOWN_WRITER_TYPE(4003,"Unknown Writer Type in Application YML"),
    UNKNOWN_WRITER_FILE_FORMAT(4004,"Unknown Writer file format in Application YML"),
    UNKNOWN_ERROR_ROUTE(4005,"Unknown enableErrorRoute in configuration"),
    KUDU_TABLE_CREATE_ERROR(4006,"Error in creating kudu table"),
    KAFKA_STREAMING_ERROR(4007,"Streaming Query Exception"),
    IO_FILE_ERROR(4008,"IO File Exception"),
    EXECUTION_EXCEPTION(4009,"Execution Exception"),
    UNABLE_TO_DELETE_FILE(4010,"Unable to delete the file"),
    FILE_PARSE_EXCEPTION(4011,"File Parse Error"),
    UNKNOWN_READER_TYPE(4012,"Unknown Reader Type in Application YML"),

    SNOWFLAKE_STREAMING_ERROR(4013,"Streaming Query Exception While Writting Into Snowflake"),
    SNOWFLAKE_SQL_ERROR(4014,"SnowFlake Query Execution Error"),
    SNOWFLAKE_DATABASE_ACCESS_ERROR(4015,"SnowFlake Database Access Error"),
    SNOWFLAKE_JDBC_DRIVER_ERROR(4016,"Snowflake JDBC Driver Class Is Not Provided"),
    SNOWFLAKE_JDBC_CLASS_INSTANTIATION_ERROR(4017,"JDBC Driver Class Object Cannot Be Instantiated"),
    SNOWFLAKE_ILLEGAL_ACCESS_ERROR(4018,"SnowFlake JDBC IllegalAccessException ERROR ");




    private int errorCode;
    private String errorMessage;

    OctopusErrorCode(int errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

}
