package com.reebelo.springbatch.constants;

public class BatchConstants {
    public static final String LOAD_CSV_FILE_JOB = "loadCsvFileJob";
    public static final String LOAD_CSV_STEP_PARTITIONER = "loadCsvStepPartitioner";
    public static final String THREAD_NAME_PREFIX = "spring_batch";
    public static final String SLAVE_THREAD = " Slave_Thread_";
    public static final String LOAD_CSV_STEP = "loadCsvStep";
    public static final int CONCURRENCY_LIMIT = 8;
    public static final int CHUNK_SIZE = 100;
    public static final int GRID_SIZE = 100;
    public static final int SKIP_LIMIT_SIZE = 25;
    public static final String PRODUCTS_FILENAME_INPUT = "/input/products*.csv";
}
