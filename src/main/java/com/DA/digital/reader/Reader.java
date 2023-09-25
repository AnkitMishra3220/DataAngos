package com.da.digital.reader;

import com.da.digital.exception.OctopusException;

public interface Reader<T> {

    T read() throws OctopusException;
}
