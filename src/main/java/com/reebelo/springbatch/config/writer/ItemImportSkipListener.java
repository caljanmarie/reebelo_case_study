package com.reebelo.springbatch.config.writer;

import com.reebelo.springbatch.entity.Product;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;

public class ItemImportSkipListener implements SkipListener<Product, Product> {

    private static final Logger logger = LoggerFactory.getLogger(ItemImportSkipListener.class);

    @Override
    public void onSkipInRead(Throwable t) {
        logger.warn("Line skipped on read");
        if(t instanceof FlatFileParseException) {
            logger.warn("Data Input Error: {}", ((FlatFileParseException) t).getInput());
        } else {

        }
    }

    @Override
    public void onSkipInWrite(Product item, Throwable t) {
        logger.warn("Line skipped on write {}", t);
    }

    @Override
    public void onSkipInProcess(Product item, Throwable t) {
        logger.warn("Line skipped on process {}", t);
    }

}

