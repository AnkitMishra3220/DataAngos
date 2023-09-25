package com.da.digital;

import com.da.digital.exception.OctopusErrorCode;
import com.da.digital.exception.OctopusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static java.nio.file.Files.readAllLines;

@Component
public class TestUtil {

    private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);

    public void deleteRecursively(File file) throws OctopusException {

        if(file.isDirectory()){
            List<File> listOfFile = Arrays.asList(file.listFiles());
            listOfFile.forEach(x -> {
                try {
                    deleteRecursively(x);
                } catch (OctopusException e) {
                    logger.error("Unable to delete the file");
                }
            });
        }
        if (file.exists() && !file.delete()){
            logger.error("Unable to delete the file {}",file.getAbsolutePath());
            throw new OctopusException(OctopusErrorCode.UNABLE_TO_DELETE_FILE);

        }
    }

}
