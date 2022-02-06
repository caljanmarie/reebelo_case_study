package com.reebelo.springbatch.config.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

import static com.reebelo.springbatch.constants.BatchConstants.*;

public class CsvStepPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CsvStepPartitioner.class);

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<>();

        Resource[] resources = null;
        int partitionNumber = 0;
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            resources = resolver.getResources(PRODUCTS_FILENAME_INPUT);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        for(Resource resource: resources) {
            int noOfLines = 0;
            try {
                noOfLines = getNoOfLines(resource.getFilename());
            } catch (IOException e) {
                e.printStackTrace();
            }

            int firstLine = 1;
            int lastLine = gridSize;

            while(firstLine < noOfLines) {

                if(lastLine >= noOfLines) {
                    lastLine = noOfLines;
                }

                //logger.info("Partition number : {}, first line is : {}, last  line is : {} ", partitionNumber, firstLine, lastLine);

                ExecutionContext value = new ExecutionContext();

                value.putLong("partition_number", partitionNumber);
                value.putLong("first_line", firstLine);
                value.putLong("last_line", lastLine);
                value.putString("file_name", resource.getFilename());

                result.put("PartitionNumber-" + partitionNumber, value);

                firstLine = firstLine + gridSize;
                lastLine = lastLine + gridSize;
                partitionNumber++;
            }

            logger.info("No of lines {}", noOfLines);
        }

        return result;
    }

    public int getNoOfLines(String fileName) throws IOException {
        ClassPathResource classPathResource = new ClassPathResource("/input/"+fileName);
        LineNumberReader reader = new LineNumberReader(new FileReader(classPathResource.getFile().getAbsolutePath()));
        reader.skip(Integer.MAX_VALUE);
        return reader.getLineNumber();
    }
}
