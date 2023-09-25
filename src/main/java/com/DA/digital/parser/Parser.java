package com.da.digital.parser;

import com.da.digital.exception.DataAngosException;
import java.io.Serializable;

public interface Parser<I, O> extends Serializable {
    
     O parse(I input) throws DataAngosException;

}