package com.da.digital.exception;

import java.io.Serializable;

public class DataAngosException extends RuntimeException implements Serializable {

    public DataAngosException(DataAngosErrorCode DataAngosErrorCode)
    {
        super("{" + "DataAngosErrorCode:" +DataAngosErrorCode.getErrorCode()
                + " DataAngosErrorMessage: " +DataAngosErrorCode.getErrorMessage() + "}");
    }
}
