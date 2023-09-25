package com.da.digital.processor;

import com.da.digital.exception.DataAngosErrorCode;
import com.da.digital.exception.DataAngosException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
public class ProcessorFactory<I, O> implements Processor<I, O> {

    @Value("${processorType}")
    private String processorType;

//how does this collection gets set and autowired.
    @Autowired
    private Map<String, Processor<I,O>> mapOfProcessor;

    @Override
    public O process(I input) throws DataAngosException {

        processorType = processorType.toLowerCase() + "processor";

        try {
            return mapOfProcessor.get(processorType).process(input);
        } catch (NullPointerException ex) {
            throw new DataAngosException(DataAngosErrorCode.UNKNOWN_PROCESSOR_TYPE);
        }

    }
}
