package com.da.digital.writer;

import com.da.digital.exception.OctopusException;

public interface Writer<T> {

    void write(T output) throws OctopusException;
}
