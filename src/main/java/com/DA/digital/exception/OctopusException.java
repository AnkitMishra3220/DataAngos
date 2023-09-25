package com.da.digital.exception;

import java.io.Serializable;

public class OctopusException extends RuntimeException implements Serializable {

    public OctopusException(OctopusErrorCode octopusErrorCode)
    {
        super("{" + "octopusErrorCode:" +octopusErrorCode.getErrorCode()
                + " octopusErrorMessage: " +octopusErrorCode.getErrorMessage() + "}");
    }
}
