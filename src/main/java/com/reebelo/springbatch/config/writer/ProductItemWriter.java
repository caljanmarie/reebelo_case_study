package com.reebelo.springbatch.config.writer;

import com.reebelo.springbatch.entity.Product;
import com.reebelo.springbatch.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public class ProductItemWriter implements ItemWriter<Product> {

    private static final Logger logger = LoggerFactory.getLogger(ProductItemWriter.class);

    private final ProductRepository productRepository;

    public ProductItemWriter(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Override
    @Transactional
    public void write(List<? extends Product> productList) {
        try {
            productRepository.saveAll(productList);
        } catch (Exception e) {
            logger.error("error saving products {}", e.getMessage());
        }
    }
}
