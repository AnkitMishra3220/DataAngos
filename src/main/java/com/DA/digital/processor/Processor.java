package com.da.digital.processor;

import com.da.digital.exception.OctopusException;

import java.io.Serializable;

public interface Processor<I, O> extends Serializable {

    O process(I input) throws OctopusException;

}
