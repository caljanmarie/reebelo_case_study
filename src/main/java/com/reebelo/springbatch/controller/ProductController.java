package com.reebelo.springbatch.controller;

import com.reebelo.springbatch.config.writer.ItemImportSkipListener;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.reebelo.springbatch.constants.BatchConstants.LOAD_CSV_FILE_JOB;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController()
@RequestMapping("products")
public class ProductController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    public ProductController(JobLauncher jobLauncher,
                             JobRegistry jobRegistry) {
        this.jobLauncher = jobLauncher;
        this.jobRegistry = jobRegistry;
    }

    @RequestMapping(method = GET, value = "load")
    public ResponseEntity loadProducts() throws Exception{
        JobExecution jobExecution = jobLauncher.run(jobRegistry.getJob(LOAD_CSV_FILE_JOB), new JobParameters());
        return ResponseEntity.ok().body("Successfully Started" + "\n" +"Batch Job Id : "+ jobExecution.getJobId()  +" | Status : "
                +  jobExecution.getStatus().name() + "\n"
                + "Start Time : " + jobExecution.getStartTime()
                + "| End Time : " + jobExecution.getEndTime());
    }
}
