package com.da.digital.reader;

import com.da.digital.exception.DataAngosException;

public interface Reader<T> {

    T read() throws DataAngosException;
}
