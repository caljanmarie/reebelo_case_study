package com.reebelo.springbatch.config;

import com.reebelo.springbatch.config.writer.ItemImportSkipListener;
import com.reebelo.springbatch.entity.Product;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import com.reebelo.springbatch.config.partitioner.CsvStepPartitioner;
import com.reebelo.springbatch.config.writer.ProductItemWriter;
import com.reebelo.springbatch.repository.ProductRepository;

import java.io.IOException;

import static com.reebelo.springbatch.constants.BatchConstants.*;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);
    public static final Long LONG_OVERRIDE = null;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final ProductRepository productRepository;

    public BatchConfig(JobBuilderFactory jobBuilderFactory,
                       StepBuilderFactory stepBuilderFactory,
                       ProductRepository productRepository) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.productRepository = productRepository;
    }

    @Bean
    public Job loadCsvFileJob() throws IOException {
        return this.jobBuilderFactory.get(LOAD_CSV_FILE_JOB)
                .start(loadCsvStepPartitioner())
                .build();
    }

    private Step loadCsvStepPartitioner() throws IOException {

        return stepBuilderFactory.get(LOAD_CSV_STEP_PARTITIONER)
                .partitioner(LOAD_CSV_STEP, csvStepPartitioner())
                .partitionHandler(loadCsvFileStepPartitionHandler(loadCsvStep(), GRID_SIZE))
                .build();
    }

    private CsvStepPartitioner csvStepPartitioner() {
        return new CsvStepPartitioner();
    }

    private PartitionHandler loadCsvFileStepPartitionHandler(final Step step,
                                                             final int gridSize) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler =
                new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(step);
        taskExecutorPartitionHandler.setGridSize(gridSize);
        return taskExecutorPartitionHandler;
    }

    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor =
                new SimpleAsyncTaskExecutor(THREAD_NAME_PREFIX);
        asyncTaskExecutor.setThreadNamePrefix(SLAVE_THREAD);
        asyncTaskExecutor.setConcurrencyLimit(CONCURRENCY_LIMIT);
        return asyncTaskExecutor;
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }


    public ProductItemWriter writer() {
        return new ProductItemWriter(productRepository);
    }

    @Bean
    public Step loadCsvStep() throws IOException {
        return this.stepBuilderFactory.get(LOAD_CSV_STEP)
                .<Product, Product>chunk(CHUNK_SIZE)
                .writer(writer())
                .reader(flatFileItemReader(LONG_OVERRIDE, LONG_OVERRIDE,
                        LONG_OVERRIDE, ""))
                .faultTolerant()
                .skipLimit(SKIP_LIMIT_SIZE)
                .skip(Exception.class)
                .skip(ValidationException.class)
                .listener(new ItemImportSkipListener())
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Product> flatFileItemReader(
            @Value("#{stepExecutionContext[partition_number]}") final Long partitionNumber,
            @Value("#{stepExecutionContext[first_line]}") final Long firstLine,
            @Value("#{stepExecutionContext[last_line]}") final Long lastLine,
            @Value("#{stepExecutionContext[file_name]}") final String filename) {

            logger.info("Partition Number : {}, Reading file from line : {}, to line: {} ", partitionNumber, firstLine, lastLine);

            FlatFileItemReader<Product> reader = new FlatFileItemReader<>();
            reader.setLinesToSkip(Math.toIntExact(firstLine));
            reader.setMaxItemCount(Math.toIntExact(lastLine));
            reader.setResource(new ClassPathResource("/input/"+filename));
            reader.setLineMapper(new DefaultLineMapper<Product>() {
                {
                    setLineTokenizer(new DelimitedLineTokenizer() {
                        {
                            setNames("product_id", "price", "stock");
                        }
                    });

                    setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
                        {
                            setTargetType(Product.class);
                        }
                    });

                }
            });
            return reader;
    }

}

