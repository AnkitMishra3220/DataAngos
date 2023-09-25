package com.da.digital.writer;

import com.da.digital.exception.DataAngosException;

public interface Writer<T> {

    void write(T output) throws DataAngosException;
}
